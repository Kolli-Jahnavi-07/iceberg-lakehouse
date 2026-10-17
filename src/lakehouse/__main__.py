"""Command line.

python -m lakehouse demo [--days 90] [--vehicles 400]   # full walkthrough, see README
python -m lakehouse query "SELECT ..."                   # DuckDB over the current Iceberg tables
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import timedelta
from pathlib import Path

import duckdb
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from . import generator
from .maintenance import compact, expire_snapshots
from .pipeline import Lakehouse
from .tables import BRONZE, DAILY, GOLD, LEASES, SILVER, catalog, create_all

PARTITION_EVOLUTION_DAY = 45


def query(lh: Lakehouse, sql: str):
    con = duckdb.connect()
    for name in (BRONZE, SILVER, DAILY, LEASES, GOLD):
        con.register(name.replace(".", "_"), lh.table(name).scan().to_arrow())
    return con.execute(sql)


def demo(workdir: Path, days: int, vehicles: int) -> int:
    if workdir.exists():
        shutil.rmtree(workdir)
    cat = catalog(workdir)
    create_all(cat)
    lh = Lakehouse(cat)
    fleet = generator.fleet(vehicles)
    leases = generator.lease_rows(fleet)
    arrivals = generator.telemetry(fleet, days)
    print(
        f"{vehicles} leased EVs, {days} days, {sum(map(len, arrivals.values())):,} events "
        f"(late and duplicated deliveries included)\n"
    )

    t0 = time.perf_counter()
    for d in sorted(arrivals):
        day = generator.START + timedelta(days=d)
        if d == PARTITION_EVOLUTION_DAY:
            lh.evolve_bronze_to_daily_partitions()
            print(f"day {d:>3}  partition evolution: bronze month -> day (no data rewritten)")
        r = lh.process_batch(f"batch-{day.isoformat()}", arrivals[d], as_of=day, leases=leases if d == 0 else None)
        if r.new_columns:
            print(f"day {d:>3}  schema evolution: new column(s) {', '.join(r.new_columns)}")
        if d % 15 == 0 or d == max(arrivals):
            print(
                f"day {d:>3}  bronze +{r.bronze_rows:>5,}  silver +{r.silver_inserted:>5,}  "
                f"dupes dropped {r.silver_duplicates:>3}  daily rows {r.daily_rows:>4}  {r.seconds:.1f}s"
            )
    print(f"\nloaded in {time.perf_counter() - t0:.0f} s")

    again = lh.process_batch(f"batch-{generator.START.isoformat()}", arrivals[0], as_of=generator.START)
    print(f"re-running the first batch: {'skipped (already committed)' if again.skipped else 'LOADED TWICE'}")

    bronze = lh.table(BRONZE)
    files = bronze.inspect.files().to_pylist()
    by_spec = {s: sum(1 for f in files if f["spec_id"] == s) for s in sorted({f["spec_id"] for f in files})}
    one_day = generator.START + timedelta(days=20)
    flt = And(
        GreaterThanOrEqual("event_ts", f"{one_day}T00:00:00+00:00"),
        LessThan("event_ts", f"{one_day + timedelta(days=1)}T00:00:00+00:00"),
    )
    planned = len(list(bronze.scan(row_filter=flt).plan_files()))
    print(f"\nbronze files by partition spec: {by_spec}; a one-day query plans {planned} of {len(files)} files")

    as_of = generator.START + timedelta(days=PARTITION_EVOLUTION_DAY - 1)
    then = {r["vin"]: r["risk_tier"] for r in lh.gold_as_of(as_of).to_pylist()}
    now = {r["vin"]: r["risk_tier"] for r in lh.table(GOLD).scan().to_arrow().to_pylist()}
    moved = sum(1 for v in now if then.get(v) and then[v] != now[v])
    print(f"time travel: {moved} leases changed risk tier between {as_of} and today")

    print("\ncurrent residual-value exposure:")
    for row in query(
        lh,
        """
        SELECT risk_tier, COUNT(*) AS leases, ROUND(AVG(projected_soh_at_return), 1) AS avg_projected_soh,
               ROUND(AVG(dc_fast_share) * 100, 0) AS dc_fast_pct, ROUND(SUM(residual_at_risk)) AS residual_at_risk
        FROM gold_residual_risk GROUP BY 1 ORDER BY CASE risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END""",
    ).fetchall():
        print(f"  {row[0]:<7} {row[1]:>4} leases  projected SOH {row[2]:>5}%  DC fast {row[3]:>3.0f}%  ${row[4]:>10,.0f} at risk")

    c = compact(lh.table(BRONZE), min_files=4)
    expired = expire_snapshots(lh.table(BRONZE), keep_last=10)
    print(
        f"\nmaintenance: compacted {c.partitions_rewritten} partitions, bronze files {c.files_before} -> {c.files_after}, "
        f"expired {expired} old snapshots"
    )
    print(f"warehouse: {workdir.resolve()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m lakehouse")
    ap.add_argument("--warehouse", type=Path, default=Path("data/lakehouse"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo")
    d.add_argument("--days", type=int, default=90)
    d.add_argument("--vehicles", type=int, default=250)
    q = sub.add_parser("query")
    q.add_argument("sql")
    args = ap.parse_args(argv)
    if args.cmd == "demo":
        return demo(args.warehouse, args.days, args.vehicles)
    lh = Lakehouse(catalog(args.warehouse))
    print(query(lh, args.sql).fetchdf().to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
