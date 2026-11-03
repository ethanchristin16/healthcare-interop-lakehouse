"""Bronze / silver / gold pipeline using pandas (local) with the same layer
contracts as the PySpark job in ``spark_job.py``.

Bronze  raw payloads + lineage metadata, append-only
Silver  parsed, typed, de-duplicated, direct identifiers removed
Gold    analytics-ready tables (encounter summary with 30-day readmission flag,
        denial summary by CARC reason code)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .parsers import flatten_bundle, parse_adt, parse_835

log = logging.getLogger(__name__)

DEV_SALT = "local-dev-salt-change-me"


def _salt() -> str:
    salt = os.environ.get("HC_LAKEHOUSE_SALT")
    if not salt:
        log.warning("HC_LAKEHOUSE_SALT not set; using a development salt. Never do this outside local runs.")
        return DEV_SALT
    return salt


def patient_key(mrn: str | None, salt: str) -> str | None:
    """Stable pseudonymous key so silver/gold never carry the raw MRN."""
    if not mrn:
        return None
    return hashlib.sha256(f"{salt}:{mrn}".encode()).hexdigest()[:24]


# ---------------------------------------------------------------- bronze
def build_bronze(landing: str | Path) -> pd.DataFrame:
    landing = Path(landing)
    rows = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    for source, pattern in (("hl7v2", "hl7/*.hl7"), ("fhir_r4", "fhir/*.json"), ("x12_835", "x12/*.835")):
        for path in sorted(landing.glob(pattern)):
            payload = path.read_text()
            rows.append({
                "source_system": source,
                "source_file": path.name,
                "payload": payload,
                "payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
                "ingested_at": ingested_at,
            })
    df = pd.DataFrame(rows, columns=["source_system", "source_file", "payload", "payload_sha256", "ingested_at"])
    # idempotent re-runs: identical payloads are only kept once
    return df.drop_duplicates(subset=["payload_sha256"]).reset_index(drop=True)


# ---------------------------------------------------------------- silver
def build_silver(bronze: pd.DataFrame, salt: str | None = None) -> dict[str, pd.DataFrame]:
    salt = salt or _salt()
    adt_rows, rejects = [], []
    patients, conditions, claims, adjustments = [], [], [], []

    for rec in bronze.itertuples(index=False):
        try:
            if rec.source_system == "hl7v2":
                e = parse_adt(rec.payload)
                adt_rows.append({
                    "visit_number": e.visit_number, "event_type": e.event_type, "message_ts": e.message_ts,
                    "patient_key": patient_key(e.mrn, salt), "patient_class": e.patient_class, "unit": e.unit,
                    "admit_ts": e.admit_ts, "discharge_ts": e.discharge_ts,
                })
            elif rec.source_system == "fhir_r4":
                flat = flatten_bundle(json.loads(rec.payload))
                id_map = {p["fhir_patient_id"]: patient_key(p["mrn"], salt) for p in flat["patients"]}
                for p in flat["patients"]:
                    patients.append({"patient_key": id_map[p["fhir_patient_id"]],
                                     "birth_year": int(p["birth_date"][:4]) if p["birth_date"] else None,
                                     "gender": p["gender"]})
                for c in flat["conditions"]:
                    conditions.append({"patient_key": id_map.get(c["fhir_patient_id"]),
                                       "visit_number": c["fhir_encounter_id"], "icd10_code": c["code"],
                                       "icd10_display": c["display"]})
            elif rec.source_system == "x12_835":
                for c in parse_835(rec.payload):
                    claims.append({
                        "claim_id": c.claim_id, "visit_number": c.claim_id.removeprefix("CLM"),
                        "patient_key": patient_key(c.patient_mrn, salt), "status": c.status,
                        "is_denied": c.is_denied, "charge_amount": float(c.charge_amount),
                        "paid_amount": float(c.paid_amount),
                    })
                    for a in c.adjustments:
                        adjustments.append({"claim_id": c.claim_id, "group_code": a.group_code,
                                            "carc_code": a.reason_code, "amount": float(a.amount)})
        except Exception as exc:  # quarantine, never drop silently
            log.error("Rejected %s (%s): %s", rec.source_file, rec.source_system, exc)
            rejects.append({"source_file": rec.source_file, "source_system": rec.source_system, "error": str(exc)})

    adt = pd.DataFrame(adt_rows)
    if not adt.empty:
        # latest event per visit wins; discharge (A03) supplies discharge_ts
        adt = adt.sort_values(["visit_number", "message_ts"])
        encounters = (adt.groupby("visit_number", as_index=False)
                      .agg(patient_key=("patient_key", "last"), patient_class=("patient_class", "last"),
                           unit=("unit", "last"), admit_ts=("admit_ts", "min"), discharge_ts=("discharge_ts", "max"),
                           event_count=("event_type", "count")))
    else:
        encounters = pd.DataFrame(columns=["visit_number", "patient_key", "patient_class", "unit",
                                           "admit_ts", "discharge_ts", "event_count"])

    return {
        "encounters": encounters,
        "patients": pd.DataFrame(patients).drop_duplicates("patient_key") if patients else pd.DataFrame(),
        "conditions": pd.DataFrame(conditions),
        "claims": pd.DataFrame(claims),
        "claim_adjustments": pd.DataFrame(adjustments),
        "rejects": pd.DataFrame(rejects, columns=["source_file", "source_system", "error"]),
    }


# ---------------------------------------------------------------- gold
def build_gold(silver: dict[str, pd.DataFrame], readmit_days: int = 30) -> dict[str, pd.DataFrame]:
    enc = silver["encounters"].copy()
    enc["admit_ts"] = pd.to_datetime(enc["admit_ts"])
    enc["discharge_ts"] = pd.to_datetime(enc["discharge_ts"])
    enc["los_days"] = (enc["discharge_ts"] - enc["admit_ts"]).dt.total_seconds() / 86400
    enc = enc.sort_values(["patient_key", "admit_ts"])
    enc["next_admit_ts"] = enc.groupby("patient_key")["admit_ts"].shift(-1)
    gap = (enc["next_admit_ts"] - enc["discharge_ts"]).dt.days
    enc["readmitted_within_30d"] = gap.between(0, readmit_days).fillna(False).astype(bool)

    primary_dx = (silver["conditions"].drop_duplicates("visit_number")[["visit_number", "icd10_code", "icd10_display"]]
                  if not silver["conditions"].empty else pd.DataFrame(columns=["visit_number", "icd10_code", "icd10_display"]))
    encounter_summary = enc.merge(primary_dx, on="visit_number", how="left").drop(columns=["next_admit_ts"])

    claims, adj = silver["claims"], silver["claim_adjustments"]
    if claims.empty:
        denial_summary = pd.DataFrame(columns=["carc_code", "denied_claims", "denied_amount"])
    else:
        denied = claims[claims["is_denied"]].merge(adj, on="claim_id", how="left")
        denial_summary = (denied.groupby("carc_code", as_index=False)
                          .agg(denied_claims=("claim_id", "nunique"), denied_amount=("amount", "sum"))
                          .sort_values("denied_amount", ascending=False))
    return {"encounter_summary": encounter_summary.reset_index(drop=True),
            "denial_summary": denial_summary.reset_index(drop=True)}


def write_tables(tables: dict[str, pd.DataFrame], out_dir: str | Path, layer: str) -> None:
    target = Path(out_dir) / layer
    target.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(target / f"{name}.csv", index=False)
