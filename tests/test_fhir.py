import pytest

from hc_lakehouse.parsers.fhir import FHIRParseError, flatten_bundle

BUNDLE = {
    "resourceType": "Bundle",
    "entry": [
        {"resource": {"resourceType": "Patient", "id": "p1",
                      "identifier": [{"type": {"coding": [{"code": "MR"}]}, "value": "MRN1"}],
                      "name": [{"family": "Chen", "given": ["Alex"]}], "birthDate": "1980-02-03", "gender": "male"}},
        {"resource": {"resourceType": "Encounter", "id": "e1", "status": "finished", "class": {"code": "IMP"},
                      "subject": {"reference": "Patient/p1"},
                      "period": {"start": "2026-01-01T10:00:00", "end": "2026-01-03T10:00:00"}}},
        {"resource": {"resourceType": "Condition", "id": "c1", "subject": {"reference": "Patient/p1"},
                      "encounter": {"reference": "Encounter/e1"},
                      "code": {"coding": [{"system": "icd10", "code": "I50.9", "display": "HF"}]}}},
        {"resource": {"resourceType": "Observation", "id": "ignored"}},
    ],
}


def test_flatten_bundle():
    out = flatten_bundle(BUNDLE)
    assert out["patients"][0]["mrn"] == "MRN1"
    assert out["encounters"][0]["fhir_patient_id"] == "p1"
    assert out["conditions"][0]["code"] == "I50.9"
    assert out["conditions"][0]["fhir_encounter_id"] == "e1"


def test_rejects_non_bundle():
    with pytest.raises(FHIRParseError):
        flatten_bundle({"resourceType": "Patient"})
