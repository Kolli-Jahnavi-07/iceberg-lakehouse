"""Bronze -> silver -> gold on Iceberg, one arrival batch at a time.

bronze.telemetry_events   append-only, exactly as received (duplicates included); idempotent per batch
silver.telemetry          de-duplicated on event_id (events are immutable: anti-join + append, no rewrites)
silver.vehicle_daily      one row per vehicle per day, recomputed only for the (vin, day) pairs a batch touched
gold.residual_risk        residual-value risk per lease; every refresh is a snapshot you can time-travel to
"""

from __future__ import annotations

import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.compute  # noqa: F401 - pa.compute
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThan
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from .tables import BRONZE, DAILY, GOLD, LEASES, SILVER

BATCH_PROP = "lakehouse.batch-id"
AS_OF_PROP = "lakehouse.as-of-day"


@dataclass
class BatchResult:
    batch_id: str
    skipped: bool = False
    bronze_rows: int = 0
    silver_inserted: int = 0
    silver_duplicates: int = 0
    daily_rows: int = 0
    gold_rows: int = 0
    new_columns: list[str] = field(default_factory=list)
    commit_retries: int = 0
    seconds: float = 0.0


def commit_with_retry(load: Callable[[], Table], write: Callable[[Table], object], attempts: int = 5) -> tuple[object, int]:
    """Optimistic concurrency: if another writer committed first, reload the table and try again.

    Iceberg commits are compare-and-swap on the metadata pointer, so a conflicting writer gets
    CommitFailedException instead of silently overwriting someone else's snapshot. PyIceberg already
    re-applies plain appends; this covers every other operation (overwrites, upserts, schema changes).
    """
    for i in range(attempts):
        try:
            return write(load()), i
        except CommitFailedException:
            if i == attempts - 1:
                raise
            time.sleep(0.05 * 2**i)
    raise RuntimeError("unreachable")


def _arrow(rows: list[dict], table: Table) -> pa.Table:
    schema = table.schema().as_arrow()
    return pa.Table.from_pylist([{f.name: r.get(f.name) for f in schema} for r in rows], schema=schema)


def batch_already_loaded(table: Table, batch_id: str) -> bool:
    return any(s.summary and s.summary.additional_properties.get(BATCH_PROP) == batch_id for s in table.snapshots())


class Lakehouse:
    def __init__(self, cat: Catalog):
        self.cat = cat
        self.db = duckdb.connect()

    def table(self, name: str) -> Table:
        return self.cat.load_table(name)

    # ------------------------------------------------------------------ dimensions
    def upsert_leases(self, rows: list[dict]) -> None:
        t = self.table(LEASES)
        t.upsert(_arrow(rows, t), join_cols=["vin"])

    # ------------------------------------------------------------------ schema / partition evolution
    def evolve_schema(self, events: list[dict]) -> list[str]:
        """Add any new event attributes (e.g. firmware_version) to bronze and silver as optional columns."""
        known = {f.name for f in self.table(BRONZE).schema().fields}
        new = sorted({k for e in events for k in e} - known - {"ingest_batch"})
        for name in (BRONZE, SILVER):
            if new:
                with self.table(name).update_schema() as upd:
                    for col in new:
                        upd.add_column(col, _iceberg_type(events, col), doc="added by schema evolution")
        return new

    def evolve_bronze_to_daily_partitions(self) -> None:
        """Volume grew: new data is partitioned by day. Existing monthly files stay as they are and
        are still pruned correctly, because Iceberg plans each file against its own partition spec."""
        t = self.table(BRONZE)
        if any(f.name == "event_day" for f in t.spec().fields):
            return
        with t.update_spec() as spec:
            spec.remove_field("event_month")
            spec.add_field("event_ts", DayTransform(), "event_day")

    # ------------------------------------------------------------------ batch processing
    def process_batch(self, batch_id: str, events: list[dict], as_of: date, leases: list[dict] | None = None) -> BatchResult:
        t0 = time.perf_counter()
        res = BatchResult(batch_id)
        if batch_already_loaded(self.table(BRONZE), batch_id):
            res.skipped = True
            return res
        if leases:
            self.upsert_leases(leases)
        if not events:  # quiet day: nothing arrived, the published report stays as it is
            res.seconds = round(time.perf_counter() - t0, 2)
            return res
        res.new_columns = self.evolve_schema(events)
        rows = [{**e, "ingest_batch": batch_id} for e in events]

        # Bronze: raw append; the batch id in the snapshot summary makes a re-run a no-op.
        def append_bronze(t: Table) -> None:
            t.append(_arrow(rows, t), snapshot_properties={BATCH_PROP: batch_id})

        _, retries = commit_with_retry(lambda: self.table(BRONZE), append_bronze)
        res.commit_retries += retries
        res.bronze_rows = len(rows)

        # Silver: de-duplicate within the batch, then upsert on event_id against everything seen before.
        silver = self.table(SILVER)
        batch = _arrow(rows, silver)
        self.db.register("batch", batch)
        deduped = self.db.execute(
            "SELECT * FROM batch QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts) = 1"
        ).to_arrow_table()
        self.db.unregister("batch")
        # Only files whose event_ts range overlaps this batch can hold a duplicate: prune by time first.
        ts = deduped.column("event_ts")
        window = And(
            GreaterThanOrEqual("event_ts", pa.compute.min(ts).as_py().isoformat()),
            LessThan("event_ts", (pa.compute.max(ts).as_py() + timedelta(seconds=1)).isoformat()),
            In("event_id", deduped.column("event_id").to_pylist()),
        )
        existing = silver.scan(row_filter=window, selected_fields=("event_id",))
        seen = set(existing.to_arrow().column("event_id").to_pylist())
        fresh = deduped.filter(pa.compute.invert(pa.compute.is_in(deduped.column("event_id"), pa.array(list(seen), pa.string()))))
        if fresh.num_rows:
            silver.append(fresh.cast(silver.schema().as_arrow()), snapshot_properties={BATCH_PROP: batch_id})
        res.silver_inserted = fresh.num_rows
        res.silver_duplicates = len(rows) - fresh.num_rows

        # Daily: recompute only the (vin, day) pairs that received events, late ones included.
        if fresh.num_rows:
            res.daily_rows = self._refresh_daily(fresh)
        res.gold_rows = self.refresh_gold(as_of)
        res.seconds = round(time.perf_counter() - t0, 2)
        return res

    def _refresh_daily(self, fresh: pa.Table) -> int:
        self.db.register("fresh", fresh)
        touched = self.db.execute("SELECT DISTINCT vin, CAST(event_ts AS DATE) AS day FROM fresh").fetchall()
        self.db.unregister("fresh")
        days = sorted({d for _, d in touched})
        vins = sorted({v for v, _ in touched})
        lo = datetime.combine(days[0], datetime.min.time(), UTC)
        hi = datetime.combine(days[-1] + timedelta(days=1), datetime.min.time(), UTC)
        scan = (
            self.table(SILVER)
            .scan(
                row_filter=And(
                    GreaterThanOrEqual("event_ts", lo.isoformat()), LessThan("event_ts", hi.isoformat()), In("vin", vins)
                )
            )
            .to_arrow()
        )
        self.db.register("events", scan)
        pairs = pa.table({"vin": [v for v, _ in touched], "day": pa.array([d for _, d in touched], pa.date32())})
        self.db.register("pairs", pairs)
        daily = self.db.execute("""
            SELECT e.vin, CAST(e.event_ts AS DATE) AS day,
                   COALESCE(SUM(trip_km), 0)                                                 AS km_driven,
                   COALESCE(SUM(energy_kwh) FILTER (WHERE event_type = 'charge'), 0)        AS kwh_charged,
                   COALESCE(SUM(energy_kwh) FILTER (WHERE event_type = 'charge' AND dc_fast_charge), 0) AS dc_fast_kwh,
                   CAST(COUNT(*) FILTER (WHERE event_type = 'charge') AS INTEGER)            AS charge_sessions,
                   MIN(battery_soc_pct)                                                      AS min_soc_pct,
                   ARG_MAX(battery_soh_pct, event_ts)                                         AS soh_pct,
                   MAX(odometer_km)                                                          AS odometer_km,
                   AVG(ambient_temp_c)                                                       AS avg_temp_c
            FROM events e JOIN pairs p ON p.vin = e.vin AND p.day = CAST(e.event_ts AS DATE)
            GROUP BY 1, 2
        """).to_arrow_table()
        self.db.unregister("events")
        self.db.unregister("pairs")
        t = self.table(DAILY)
        t.upsert(daily.cast(t.schema().as_arrow()), join_cols=["vin", "day"])
        return daily.num_rows

    def refresh_gold(self, as_of: date) -> int:
        daily = self.table(DAILY).scan(row_filter=GreaterThanOrEqual("day", (as_of - timedelta(days=45)).isoformat())).to_arrow()
        leases = self.table(LEASES).scan().to_arrow()
        if daily.num_rows == 0 or leases.num_rows == 0:
            return 0
        self.db.register("daily", daily)
        self.db.register("leases", leases)
        gold = self.db.execute(
            """
            WITH recent AS (
              SELECT vin,
                     ARG_MAX(soh_pct, day)                                    AS soh_now,
                     ARG_MIN(soh_pct, day)                                    AS soh_then,
                     MAX(odometer_km) - MIN(odometer_km)                      AS km_window,
                     SUM(km_driven) FILTER (WHERE day > ?::DATE - 30)         AS km_30,
                     SUM(dc_fast_kwh) / NULLIF(SUM(kwh_charged), 0)           AS dc_share
              FROM daily WHERE day <= ?::DATE
              GROUP BY vin
            ), scored AS (
              SELECT l.vin, l.model, l.state, r.soh_now AS soh_pct, COALESCE(r.km_30, 0) AS km_last_30d,
                     CASE WHEN r.km_window > 500 THEN (r.soh_then - r.soh_now) / r.km_window * 10000 END AS soh_loss_per_10k_km,
                     COALESCE(r.dc_share, 0) AS dc_fast_share,
                     CAST(GREATEST(l.lease_months - DATE_DIFF('month', l.lease_start, ?::DATE), 0) AS INTEGER)
                       AS months_remaining,
                     l.residual_value
              FROM leases l JOIN recent r USING (vin)
            ), projected AS (
              SELECT *, GREATEST(soh_pct - COALESCE(soh_loss_per_10k_km, 0) * km_last_30d * months_remaining / 10000, 0)
                          AS projected_soh_at_return
              FROM scored
            )
            SELECT vin, model, state, ROUND(soh_pct, 2) AS soh_pct, ROUND(km_last_30d, 0) AS km_last_30d,
                   ROUND(soh_loss_per_10k_km, 3) AS soh_loss_per_10k_km, ROUND(dc_fast_share, 3) AS dc_fast_share,
                   months_remaining, ROUND(projected_soh_at_return, 2) AS projected_soh_at_return,
                   CASE WHEN projected_soh_at_return < 88 THEN 'HIGH'
                        WHEN projected_soh_at_return < 92 THEN 'MEDIUM' ELSE 'LOW' END AS risk_tier,
                   residual_value,
                   -- rule of thumb used here: each point of SOH below 92% at return costs ~1.5% of resale value
                   ROUND(residual_value * GREATEST(92 - projected_soh_at_return, 0) * 0.015, 2) AS residual_at_risk,
                   ?::DATE AS as_of_day
            FROM projected
            """,
            [as_of, as_of, as_of, as_of],
        ).to_arrow_table()
        self.db.unregister("daily")
        self.db.unregister("leases")
        t = self.table(GOLD)
        with warnings.catch_warnings():  # first refresh: nothing to replace yet
            warnings.filterwarnings("ignore", message="Delete operation did not match any records")
            t.overwrite(gold.cast(t.schema().as_arrow()), snapshot_properties={AS_OF_PROP: as_of.isoformat()})
        return gold.num_rows

    # ------------------------------------------------------------------ time travel
    def gold_as_of(self, day: date) -> pa.Table:
        """The risk report exactly as it was published for `day` (audit / reproducibility)."""
        t = self.table(GOLD)
        snaps = [s for s in t.snapshots() if s.summary and s.summary.additional_properties.get(AS_OF_PROP) == day.isoformat()]
        if not snaps:
            raise KeyError(f"no gold snapshot for {day}")
        return t.scan(snapshot_id=snaps[-1].snapshot_id).to_arrow()


def _iceberg_type(events: list[dict], col: str):
    from pyiceberg.types import BooleanType, DoubleType, LongType, StringType

    sample = next((e[col] for e in events if e.get(col) is not None), "")
    if isinstance(sample, bool):
        return BooleanType()
    if isinstance(sample, int):
        return LongType()
    if isinstance(sample, float):
        return DoubleType()
    return StringType()
