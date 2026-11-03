"""Command-line entry point.

    python -m hc_lakehouse.cli generate --landing data/landing
    python -m hc_lakehouse.cli run --landing data/landing --out data/lake
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from . import medallion, quality, synthetic


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hc-lakehouse")
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create synthetic HL7/FHIR/X12 files")
    g.add_argument("--landing", default="data/landing")
    g.add_argument("--patients", type=int, default=40)
    g.add_argument("--seed", type=int, default=7)

    r = sub.add_parser("run", help="run bronze -> silver -> gold with quality gates")
    r.add_argument("--landing", default="data/landing")
    r.add_argument("--out", default="data/lake")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "generate":
        print(json.dumps(synthetic.generate(args.landing, args.patients, args.seed)))
        return 0

    bronze = medallion.build_bronze(args.landing)
    silver = medallion.build_silver(bronze)
    try:
        results = quality.run_checks(quality.silver_checks(silver, bronze))
    except quality.DataQualityError as exc:
        logging.error("Quality gate failed, gold not refreshed: %s", exc)
        return 2
    for res in results:
        logging.info("DQ %-6s %s %s %s", "PASS" if res.passed else "WARN", res.table, res.check, res.detail)
    gold = medallion.build_gold(silver)
    medallion.write_tables({"raw_payloads": bronze.drop(columns=["payload"])}, args.out, "bronze")
    medallion.write_tables(silver, args.out, "silver")
    medallion.write_tables(gold, args.out, "gold")
    summary = {
        "bronze_records": len(bronze),
        **{f"silver_{k}": len(v) for k, v in silver.items()},
        **{f"gold_{k}": len(v) for k, v in gold.items()},
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
