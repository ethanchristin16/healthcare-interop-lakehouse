import pandas as pd
import pytest

from hc_lakehouse import cli, medallion, quality


def test_end_to_end(landing, tmp_path):
    bronze = medallion.build_bronze(landing)
    assert set(bronze["source_system"]) == {"hl7v2", "fhir_r4", "x12_835"}
    silver = medallion.build_silver(bronze, salt="test-salt")
    assert silver["rejects"].empty
    quality.run_checks(quality.silver_checks(silver, bronze))

    enc = silver["encounters"]
    assert enc["visit_number"].is_unique
    assert enc["discharge_ts"].notna().all()  # A03 merged into the visit

    gold = medallion.build_gold(silver)
    es = gold["encounter_summary"]
    assert (es["los_days"] > 0).all()
    assert es["readmitted_within_30d"].dtype == bool
    assert gold["denial_summary"]["denied_claims"].sum() == silver["claims"]["is_denied"].sum()


def test_no_raw_identifiers_in_outputs(landing):
    silver = medallion.build_silver(medallion.build_bronze(landing), salt="s")
    gold = medallion.build_gold(silver)
    text = pd.concat([*silver.values(), *gold.values()], axis=0).astype(str).to_csv()
    assert "MRN1" not in text  # matches every synthetic MRN (MRN100000+)
    for name in ("RIVERA", "CHEN", "PATEL"):
        assert name not in text


def test_patient_key_is_stable_and_salted():
    assert medallion.patient_key("MRN1", "a") == medallion.patient_key("MRN1", "a")
    assert medallion.patient_key("MRN1", "a") != medallion.patient_key("MRN1", "b")
    assert medallion.patient_key(None, "a") is None


def test_readmission_flag():
    silver = {
        "encounters": pd.DataFrame({
            "visit_number": ["a", "b", "c"], "patient_key": ["p1", "p1", "p2"],
            "patient_class": ["I", "I", "I"], "unit": ["4W"] * 3,
            "admit_ts": ["2026-01-01", "2026-01-20", "2026-01-01"],
            "discharge_ts": ["2026-01-05", "2026-01-22", "2026-01-03"], "event_count": [2, 2, 2]}),
        "conditions": pd.DataFrame(), "claims": pd.DataFrame(), "claim_adjustments": pd.DataFrame(),
    }
    es = medallion.build_gold(silver)["encounter_summary"].set_index("visit_number")
    assert es.loc["a", "readmitted_within_30d"]
    assert not es.loc["b", "readmitted_within_30d"]
    assert not es.loc["c", "readmitted_within_30d"]


def test_bad_payload_is_quarantined(landing, tmp_path):
    (tmp_path / "hl7").mkdir()
    (tmp_path / "hl7" / "bad.hl7").write_text("NOT AN HL7 MESSAGE")
    silver = medallion.build_silver(medallion.build_bronze(tmp_path), salt="s")
    assert len(silver["rejects"]) == 1


def test_quality_gate_blocks_duplicates():
    df = pd.DataFrame({"visit_number": ["a", "a"]})
    with pytest.raises(quality.DataQualityError):
        quality.run_checks([quality.unique(df, "t", "visit_number")])


def test_cli_run(landing, tmp_path, capsys):
    assert cli.main(["run", "--landing", str(landing), "--out", str(tmp_path)]) == 0
    assert (tmp_path / "gold" / "encounter_summary.csv").exists()
    assert (tmp_path / "gold" / "denial_summary.csv").exists()
