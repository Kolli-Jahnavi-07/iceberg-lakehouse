"""Table maintenance: what keeps an Iceberg lakehouse fast and cheap over time.

* compaction: daily batches and late events leave many small files per partition; rewriting a
  partition into one file cuts planning time and per-file overhead for every reader
* snapshot expiry: every commit keeps old data files reachable for time travel; expiring old
  snapshots lets that storage be reclaimed while keeping the history you still need
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table


@dataclass
class CompactionResult:
    partitions_rewritten: int
    files_before: int
    files_after: int
    rows: int


def _partition_filter(t: Table, spec_id: int, partition: dict):
    """Row filter selecting exactly one partition of a given spec (identity or time transforms)."""
    spec = t.specs()[spec_id]
    schema = t.schema()
    clauses = []
    for pf in spec.fields:
        value = partition[pf.name]
        src = schema.find_field(pf.source_id).name
        transform = str(pf.transform)
        if transform == "identity":
            clauses.append(EqualTo(src, value))
        elif transform in ("day", "month"):
            if transform == "day":
                if isinstance(value, date):  # PyIceberg reports day partitions as dates
                    start = datetime(value.year, value.month, value.day, tzinfo=UTC)
                else:
                    start = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=value)
                end = start + timedelta(days=1)
            else:
                y, m = divmod(value, 12)
                start = datetime(1970 + y, m + 1, 1, tzinfo=UTC)
                end = datetime(start.year + (start.month == 12), start.month % 12 + 1, 1, tzinfo=UTC)
            clauses += [GreaterThanOrEqual(src, start.isoformat()), LessThan(src, end.isoformat())]
        else:
            raise ValueError(f"compaction does not support transform {transform}")
    return clauses[0] if len(clauses) == 1 else And(*clauses)


def compact(t: Table, min_files: int = 4) -> CompactionResult:
    """Rewrite every partition that has at least `min_files` data files into a single file."""
    files = t.inspect.files().to_pylist()
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for f in files:
        groups[(f["spec_id"], tuple(sorted(f["partition"].items())))].append(f)
    before = len(files)
    rewritten, rows = 0, 0
    for (spec_id, part), group in sorted(groups.items(), key=lambda kv: str(kv[0])):
        if len(group) < min_files:
            continue
        flt = _partition_filter(t, spec_id, dict(part))
        data = t.scan(row_filter=flt).to_arrow()
        # Rows written now land in the *current* spec, so old-spec partitions also migrate forward.
        t.overwrite(data, overwrite_filter=flt, snapshot_properties={"lakehouse.operation": "compaction"})
        rewritten += 1
        rows += data.num_rows
        t = t.refresh()
    return CompactionResult(rewritten, before, len(t.inspect.files()), rows)


def expire_snapshots(t: Table, keep_last: int = 10) -> int:
    """Expire all but the newest `keep_last` snapshots. Returns how many were expired."""
    snaps = sorted(t.snapshots(), key=lambda s: s.timestamp_ms)
    old = [s.snapshot_id for s in snaps[:-keep_last]] if len(snaps) > keep_last else []
    if old:
        t.maintenance.expire_snapshots().by_ids(old).commit()
    return len(old)
