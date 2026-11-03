"""Generate fully synthetic HL7 v2, FHIR R4 and X12 835 test data.

Every name, identifier and amount is random. Nothing here comes from a real
patient, provider, payer or employer system.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

FAMILY = ["RIVERA", "CHEN", "PATEL", "OKAFOR", "NGUYEN", "MILLER", "GARCIA", "KOWALSKI", "HASSAN", "BROOKS"]
GIVEN = ["ALEX", "SAM", "JORDAN", "TAYLOR", "MORGAN", "CASEY", "RILEY", "JAMIE", "AVERY", "QUINN"]
UNITS = ["4W", "5E", "ICU", "ED", "MEDSURG"]
ICD10 = [("I50.9", "Heart failure, unspecified"), ("J44.1", "COPD with acute exacerbation"),
         ("E11.9", "Type 2 diabetes without complications"), ("N17.9", "Acute kidney failure, unspecified"),
         ("J18.9", "Pneumonia, unspecified organism")]
# CARC codes commonly seen on denials
CARC = ["45", "50", "197", "16", "29", "96"]

FACILITY = "EXAMPLEHOSP"


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _adt(control_id: str, event: str, mrn: str, fam: str, given: str, dob: str, sex: str,
         pclass: str, visit: str, unit: str, admit: datetime, discharge: datetime | None) -> str:
    msg_ts = discharge if (event == "A03" and discharge) else admit
    return "\r".join([
        f"MSH|^~\\&|EHR|{FACILITY}|LAKEHOUSE|{FACILITY}|{_ts(msg_ts)}||ADT^{event}|{control_id}|P|2.5.1",
        f"EVN|{event}|{_ts(msg_ts)}",
        f"PID|1||{mrn}^^^{FACILITY}^MR||{fam}^{given}||{dob}|{sex}",
        "PV1|1|{pc}|{unit}^101^A^{fac}||||||||||||||||{visit}|||||||||||||||||||||||||{adm}|{dis}".format(
            pc=pclass, unit=unit, fac=FACILITY, visit=visit, adm=_ts(admit), dis=_ts(discharge) if discharge else ""),
    ])


def generate(out_dir: str | Path, n_patients: int = 40, seed: int = 7) -> dict[str, int]:
    rng = random.Random(seed)
    out = Path(out_dir)
    (out / "hl7").mkdir(parents=True, exist_ok=True)
    (out / "fhir").mkdir(parents=True, exist_ok=True)
    (out / "x12").mkdir(parents=True, exist_ok=True)

    base = datetime(2026, 1, 5, 8, 0, 0)
    n_msgs = n_claims = 0
    clp_segments: list[str] = []

    for p in range(n_patients):
        mrn = f"MRN{100000 + p}"
        fam, given = rng.choice(FAMILY), rng.choice(GIVEN)
        dob = f"{rng.randint(1940, 2000)}{rng.randint(1, 12):02d}{rng.randint(1, 28):02d}"
        sex = rng.choice(["F", "M"])
        entries = [{"resource": {
            "resourceType": "Patient", "id": f"pat-{p}",
            "identifier": [{"type": {"coding": [{"code": "MR"}]}, "value": mrn}],
            "name": [{"family": fam.title(), "given": [given.title()]}],
            "birthDate": f"{dob[:4]}-{dob[4:6]}-{dob[6:]}", "gender": "female" if sex == "F" else "male"}}]

        admit = base + timedelta(days=rng.randint(0, 60), hours=rng.randint(0, 23))
        visits = 2 if rng.random() < 0.25 else 1  # some patients come back
        for v in range(visits):
            visit = f"V{p:04d}{v}"
            los = timedelta(days=rng.randint(1, 7), hours=rng.randint(0, 12))
            discharge = admit + los
            pclass = rng.choice(["I", "I", "E"])
            unit = rng.choice(UNITS)
            for event, dis in (("A01", None), ("A03", discharge)):
                n_msgs += 1
                (out / "hl7" / f"{visit}_{event}.hl7").write_text(
                    _adt(f"MSG{n_msgs:06d}", event, mrn, fam, given, dob, sex, pclass, visit, unit, admit, dis))
            code, display = rng.choice(ICD10)
            entries.append({"resource": {
                "resourceType": "Encounter", "id": visit, "status": "finished",
                "class": {"code": "IMP" if pclass == "I" else "EMER"},
                "subject": {"reference": f"Patient/pat-{p}"},
                "period": {"start": admit.isoformat(), "end": discharge.isoformat()}}})
            entries.append({"resource": {
                "resourceType": "Condition", "id": f"cond-{visit}",
                "subject": {"reference": f"Patient/pat-{p}"}, "encounter": {"reference": f"Encounter/{visit}"},
                "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": code, "display": display}]}}})

            charge = rng.randint(800, 12000)
            denied = rng.random() < 0.3
            paid = 0 if denied else round(charge * rng.uniform(0.5, 0.9))
            status = "4" if denied else "1"
            n_claims += 1
            clp_segments.append(f"CLP*CLM{visit}*{status}*{charge}*{paid}*0*MC*PCN{n_claims:06d}*11*1")
            adj = charge - paid
            reason = rng.choice(CARC) if denied else "45"
            clp_segments.append(f"CAS*CO*{reason}*{adj}")
            clp_segments.append(f"NM1*QC*1*{fam}*{given}****MR*{mrn}")
            clp_segments.append(f"SVC*HC:99223*{charge}*{paid}**1")
            # next visit 5-60 days later (some inside the 30-day readmission window)
            admit = discharge + timedelta(days=rng.randint(5, 60))

        (out / "fhir" / f"bundle_{p:04d}.json").write_text(
            json.dumps({"resourceType": "Bundle", "type": "collection", "entry": entries}, indent=2))

    isa = "ISA*00*          *00*          *ZZ*EXAMPLEPAYER   *ZZ*EXAMPLEHOSP    *260301*1200*^*00501*000000001*0*T*:"
    body = ["GS*HP*EXAMPLEPAYER*EXAMPLEHOSP*20260301*1200*1*X*005010X221A1", "ST*835*0001",
            "BPR*I*0*C*NON************20260301", "TRN*1*12345*1999999999", "N1*PR*EXAMPLE PAYER",
            *clp_segments, f"SE*{len(clp_segments) + 6}*0001", "GE*1*1", "IEA*1*000000001"]
    (out / "x12" / "remit_0001.835").write_text(isa + "~" + "~".join(body) + "~")

    return {"hl7_messages": n_msgs, "fhir_bundles": n_patients, "claims": n_claims}
