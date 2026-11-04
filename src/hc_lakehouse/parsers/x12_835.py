"""Parse X12 835 (health care claim payment/advice) into claim-level rows.

Covers the loops needed for denial analytics: CLP (claim payment info),
CAS (adjustments with CARC reason codes) and NM1*QC (patient). Service-line
detail (SVC) is counted but not exploded, to keep the example compact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

# CLP02 claim status codes (subset)
CLAIM_STATUS = {
    "1": "processed_primary",
    "2": "processed_secondary",
    "3": "processed_tertiary",
    "4": "denied",
    "22": "reversal",
}


class X12ParseError(ValueError):
    pass


@dataclass
class Adjustment:
    group_code: str  # CO, PR, OA, PI, CR
    reason_code: str  # CARC
    amount: Decimal


@dataclass
class ClaimPayment:
    claim_id: str
    status_code: str
    status: str
    charge_amount: Decimal
    paid_amount: Decimal
    patient_responsibility: Decimal
    payer_claim_control_number: str
    patient_mrn: str | None = None
    service_line_count: int = 0
    adjustments: list[Adjustment] = field(default_factory=list)

    @property
    def is_denied(self) -> bool:
        return self.status_code == "4" or (self.paid_amount == 0 and self.charge_amount > 0)


def _dec(value: str) -> Decimal:
    try:
        return Decimal(value or "0")
    except Exception as exc:  # decimal.InvalidOperation
        raise X12ParseError(f"Invalid amount {value!r}") from exc


def parse_835(text: str) -> list[ClaimPayment]:
    text = text.strip()
    if not text.startswith("ISA"):
        raise X12ParseError("Interchange must start with ISA")
    element_sep = text[3]
    segment_term = text[105] if len(text) > 105 else "~"
    segments = [s.strip() for s in text.split(segment_term) if s.strip()]

    if not any(s.startswith(f"ST{element_sep}835") for s in segments):
        raise X12ParseError("No 835 transaction set found")

    claims: list[ClaimPayment] = []
    current: ClaimPayment | None = None
    for seg in segments:
        el = seg.split(element_sep)
        sid = el[0]
        if sid == "CLP":
            current = ClaimPayment(
                claim_id=el[1],
                status_code=el[2],
                status=CLAIM_STATUS.get(el[2], "other"),
                charge_amount=_dec(el[3]),
                paid_amount=_dec(el[4]),
                patient_responsibility=_dec(el[5] if len(el) > 5 else "0"),
                payer_claim_control_number=el[7] if len(el) > 7 else "",
            )
            claims.append(current)
        elif sid == "CAS" and current is not None:
            group = el[1]
            # reason/amount/quantity triplets start at element 2
            for i in range(2, len(el), 3):
                if el[i]:
                    current.adjustments.append(Adjustment(group, el[i], _dec(el[i + 1] if i + 1 < len(el) else "0")))
        elif sid == "NM1" and current is not None and len(el) > 1 and el[1] == "QC":
            current.patient_mrn = el[9] if len(el) > 9 else None
        elif sid == "SVC" and current is not None:
            current.service_line_count += 1
        elif sid == "SE":
            current = None
    return claims
