from decimal import Decimal

import pytest

from hc_lakehouse.parsers.x12_835 import X12ParseError, parse_835

ISA = "ISA*00*          *00*          *ZZ*EXAMPLEPAYER   *ZZ*EXAMPLEHOSP    *260301*1200*^*00501*000000001*0*T*:"
BODY = [
    "GS*HP*A*B*20260301*1200*1*X*005010X221A1", "ST*835*0001",
    "CLP*CLM1*1*1000*800*0*MC*PCN1*11*1", "CAS*CO*45*200", "NM1*QC*1*DOE*SAM****MR*MRN1", "SVC*HC:99223*1000*800**1",
    "CLP*CLM2*4*500*0*0*MC*PCN2*11*1", "CAS*CO*197*450*1*16*50", "NM1*QC*1*ROE*ALEX****MR*MRN2",
    "SE*9*0001", "GE*1*1", "IEA*1*000000001",
]
TEXT = ISA + "~" + "~".join(BODY) + "~"


def test_parse_claims():
    claims = parse_835(TEXT)
    assert [c.claim_id for c in claims] == ["CLM1", "CLM2"]
    assert claims[0].paid_amount == Decimal("800") and not claims[0].is_denied
    assert claims[0].service_line_count == 1
    assert claims[1].is_denied and claims[1].status == "denied"
    assert claims[1].patient_mrn == "MRN2"


def test_multiple_cas_triplets():
    adj = parse_835(TEXT)[1].adjustments
    assert [(a.reason_code, a.amount) for a in adj] == [("197", Decimal("450")), ("16", Decimal("50"))]


def test_rejects_non_x12():
    with pytest.raises(X12ParseError):
        parse_835("hello")
