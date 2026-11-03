"""Lightweight data-quality checks (Great Expectations-style, no dependency).

Each check returns a CheckResult. ``run_checks`` raises DataQualityError when
any blocking check fails, so orchestration (Airflow / Databricks Workflows)
stops before downstream tables refresh.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    table: str
    check: str
    passed: bool
    detail: str = ""
    blocking: bool = True


def not_null(df: pd.DataFrame, table: str, column: str) -> CheckResult:
    n = int(df[column].isna().sum()) if column in df else len(df)
    return CheckResult(table, f"not_null({column})", n == 0, f"{n} null rows")


def unique(df: pd.DataFrame, table: str, column: str) -> CheckResult:
    n = int(df[column].duplicated().sum()) if column in df else -1
    return CheckResult(table, f"unique({column})", n == 0, f"{n} duplicate rows")


def accepted_values(df: pd.DataFrame, table: str, column: str, values: set) -> CheckResult:
    bad = sorted(set(df[column].dropna().unique()) - values) if column in df else ["<missing column>"]
    return CheckResult(table, f"accepted_values({column})", not bad, f"unexpected: {bad[:5]}")


def no_phi_columns(df: pd.DataFrame, table: str) -> CheckResult:
    banned = {"mrn", "family_name", "given_name", "birth_date", "ssn", "address", "phone"}
    found = sorted(banned & {c.lower() for c in df.columns})
    return CheckResult(table, "no_direct_identifiers", not found, f"found: {found}")


def reconcile(expected: int, actual: int, table: str, label: str) -> CheckResult:
    return CheckResult(table, f"reconcile({label})", expected == actual, f"expected={expected} actual={actual}")


def silver_checks(silver: dict[str, pd.DataFrame], bronze: pd.DataFrame) -> list[CheckResult]:
    enc, claims = silver["encounters"], silver["claims"]
    results = [
        not_null(enc, "silver.encounters", "visit_number"),
        unique(enc, "silver.encounters", "visit_number"),
        not_null(enc, "silver.encounters", "patient_key"),
        accepted_values(enc, "silver.encounters", "patient_class", {"I", "E", "O"}),
        unique(claims, "silver.claims", "claim_id"),
        CheckResult("silver.rejects", "reject_rate<=1%",
                    len(silver["rejects"]) <= 0.01 * max(len(bronze), 1),
                    f"{len(silver['rejects'])} rejected of {len(bronze)}"),
    ]
    results += [no_phi_columns(df, f"silver.{name}") for name, df in silver.items() if name != "rejects"]
    # every claim on the 835 should map to a known encounter (warning only)
    if not claims.empty and not enc.empty:
        orphan = int((~claims["visit_number"].isin(enc["visit_number"])).sum())
        results.append(CheckResult("silver.claims", "claims_match_encounters", orphan == 0,
                                   f"{orphan} claims without encounter", blocking=False))
    return results


def run_checks(results: list[CheckResult]) -> list[CheckResult]:
    failed = [r for r in results if r.blocking and not r.passed]
    if failed:
        msg = "; ".join(f"{r.table}:{r.check} ({r.detail})" for r in failed)
        raise DataQualityError(msg)
    return results
