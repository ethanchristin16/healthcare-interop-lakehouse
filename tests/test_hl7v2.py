from datetime import datetime

import pytest

from hc_lakehouse.parsers.hl7v2 import HL7ParseError, parse_adt, parse_ts

MSG = "\r".join([
    "MSH|^~\\&|EHR|EXAMPLEHOSP|LAKEHOUSE|EXAMPLEHOSP|20260105083000||ADT^A03|MSG000001|P|2.5.1",
    "EVN|A03|20260105083000",
    "PID|1||MRN100001^^^EXAMPLEHOSP^MR||RIVERA^SAM||19700101|F",
    "PV1|1|I|4W^101^A^EXAMPLEHOSP||||||||||||||||V00010|||||||||||||||||||||||||20260101120000|20260105083000",
])


def test_parse_adt_fields():
    e = parse_adt(MSG)
    assert e.event_type == "A03"
    assert e.mrn == "MRN100001"
    assert e.family_name == "RIVERA" and e.given_name == "SAM"
    assert e.patient_class == "I"
    assert e.visit_number == "V00010"
    assert e.unit == "4W"
    assert e.admit_ts == datetime(2026, 1, 1, 12, 0, 0)
    assert e.discharge_ts == datetime(2026, 1, 5, 8, 30, 0)


def test_accepts_newline_separators():
    assert parse_adt(MSG.replace("\r", "\n")).visit_number == "V00010"


def test_rejects_non_adt():
    with pytest.raises(HL7ParseError):
        parse_adt(MSG.replace("ADT^A03", "ORU^R01"))


def test_rejects_missing_msh():
    with pytest.raises(HL7ParseError):
        parse_adt("PID|1||X")


@pytest.mark.parametrize("raw,expected", [
    ("20260101", datetime(2026, 1, 1)),
    ("202601011230", datetime(2026, 1, 1, 12, 30)),
    ("20260101123045.123-0500", datetime(2026, 1, 1, 12, 30, 45)),
    ("", None),
])
def test_parse_ts(raw, expected):
    assert parse_ts(raw) == expected


def test_parse_ts_invalid():
    with pytest.raises(HL7ParseError):
        parse_ts("2026")
