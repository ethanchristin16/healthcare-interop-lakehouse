"""Minimal HL7 v2.x parser for ADT messages.

Handles the segments a lakehouse ingestion layer usually needs from ADT feeds
(MSH, EVN, PID, PV1). It is deliberately small and dependency-free; production
feeds normally go through an interface engine first.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

SEGMENT_SEPARATORS = ("\r\n", "\n", "\r")


class HL7ParseError(ValueError):
    """Raised when a message cannot be parsed."""


@dataclass(frozen=True)
class ADTEvent:
    message_control_id: str
    event_type: str  # e.g. A01 (admit), A03 (discharge), A08 (update)
    message_ts: datetime | None
    sending_facility: str
    mrn: str
    family_name: str
    given_name: str
    birth_date: str | None
    sex: str
    patient_class: str  # I = inpatient, E = emergency, O = outpatient
    visit_number: str
    unit: str
    admit_ts: datetime | None
    discharge_ts: datetime | None


def _split_segments(message: str) -> list[str]:
    text = message.strip()
    for sep in SEGMENT_SEPARATORS:
        text = text.replace(sep, "\r")
    return [s for s in text.split("\r") if s.strip()]


def parse_ts(value: str) -> datetime | None:
    """Parse HL7 DTM values such as 20260101, 202601011200 or 20260101120000."""
    value = (value or "").strip().split("+")[0].split("-")[0]
    if not value:
        return None
    value = value.split(".")[0]  # drop fractional seconds
    formats = {14: "%Y%m%d%H%M%S", 12: "%Y%m%d%H%M", 8: "%Y%m%d"}
    fmt = formats.get(len(value))
    if fmt is None:
        raise HL7ParseError(f"Unrecognised HL7 timestamp: {value!r}")
    try:
        return datetime.strptime(value, fmt)
    except ValueError as exc:
        raise HL7ParseError(f"Unrecognised HL7 timestamp: {value!r}") from exc


def _field(fields: list[str], idx: int) -> str:
    return fields[idx] if idx < len(fields) else ""


def _component(value: str, idx: int, sep: str = "^") -> str:
    parts = value.split(sep)
    return parts[idx] if idx < len(parts) else ""


def parse_segments(message: str) -> dict[str, list[list[str]]]:
    """Return {segment_id: [fields, ...]} with HL7 field numbering preserved.

    For every segment, ``fields[n]`` is field n (``SEG-n``). MSH is special
    because MSH-1 is the field separator itself, so it is re-inserted.
    """
    segments = _split_segments(message)
    if not segments or not segments[0].startswith("MSH"):
        raise HL7ParseError("Message must start with an MSH segment")
    field_sep = segments[0][3]
    out: dict[str, list[list[str]]] = {}
    for seg in segments:
        fields = seg.split(field_sep)
        if fields[0] == "MSH":
            fields = ["MSH", field_sep] + fields[1:]
        out.setdefault(fields[0], []).append(fields)
    return out


def parse_adt(message: str) -> ADTEvent:
    segs = parse_segments(message)
    for required in ("MSH", "PID", "PV1"):
        if required not in segs:
            raise HL7ParseError(f"Missing required segment {required}")
    msh, pid, pv1 = segs["MSH"][0], segs["PID"][0], segs["PV1"][0]

    message_type = _field(msh, 9)
    if _component(message_type, 0) != "ADT":
        raise HL7ParseError(f"Not an ADT message: {message_type!r}")

    name = _field(pid, 5)
    return ADTEvent(
        message_control_id=_field(msh, 10),
        event_type=_component(message_type, 1),
        message_ts=parse_ts(_field(msh, 7)),
        sending_facility=_field(msh, 4),
        mrn=_component(_field(pid, 3), 0),
        family_name=_component(name, 0),
        given_name=_component(name, 1),
        birth_date=_field(pid, 7) or None,
        sex=_field(pid, 8),
        patient_class=_field(pv1, 2),
        visit_number=_component(_field(pv1, 19), 0),
        unit=_component(_field(pv1, 3), 0),
        admit_ts=parse_ts(_field(pv1, 44)),
        discharge_ts=parse_ts(_field(pv1, 45)),
    )
