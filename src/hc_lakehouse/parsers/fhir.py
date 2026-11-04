"""Flatten FHIR R4 Bundles into tabular rows."""

from __future__ import annotations

from typing import Any, Iterable


class FHIRParseError(ValueError):
    pass


def _ref_id(reference: str | None) -> str | None:
    """'Patient/123' -> '123'."""
    if not reference:
        return None
    return reference.split("/")[-1]


def _identifier(resource: dict[str, Any], type_code: str = "MR") -> str | None:
    for ident in resource.get("identifier", []):
        codings = ident.get("type", {}).get("coding", [])
        if any(c.get("code") == type_code for c in codings):
            return ident.get("value")
    return None


def iter_resources(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    if bundle.get("resourceType") != "Bundle":
        raise FHIRParseError("Expected a FHIR Bundle")
    for entry in bundle.get("entry", []):
        resource = entry.get("resource")
        if resource:
            yield resource


def flatten_bundle(bundle: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return {'patients': [...], 'encounters': [...], 'conditions': [...]}."""
    out: dict[str, list[dict[str, Any]]] = {"patients": [], "encounters": [], "conditions": []}
    for res in iter_resources(bundle):
        rtype = res.get("resourceType")
        if rtype == "Patient":
            name = (res.get("name") or [{}])[0]
            out["patients"].append(
                {
                    "fhir_patient_id": res.get("id"),
                    "mrn": _identifier(res, "MR"),
                    "family_name": name.get("family"),
                    "given_name": " ".join(name.get("given", [])),
                    "birth_date": res.get("birthDate"),
                    "gender": res.get("gender"),
                }
            )
        elif rtype == "Encounter":
            period = res.get("period", {})
            out["encounters"].append(
                {
                    "fhir_encounter_id": res.get("id"),
                    "fhir_patient_id": _ref_id(res.get("subject", {}).get("reference")),
                    "status": res.get("status"),
                    "class_code": res.get("class", {}).get("code"),
                    "start_ts": period.get("start"),
                    "end_ts": period.get("end"),
                }
            )
        elif rtype == "Condition":
            coding = (res.get("code", {}).get("coding") or [{}])[0]
            out["conditions"].append(
                {
                    "fhir_condition_id": res.get("id"),
                    "fhir_patient_id": _ref_id(res.get("subject", {}).get("reference")),
                    "fhir_encounter_id": _ref_id(res.get("encounter", {}).get("reference")),
                    "code_system": coding.get("system"),
                    "code": coding.get("code"),
                    "display": coding.get("display"),
                }
            )
    return out
