import shutil
import threading

import pytest

from lakehouse.maintenance import compact, expire_snapshots
from lakehouse.pipeline import commit_with_retry
from lakehouse.tables import BRONZE, catalog


@pytest.fixture()
def copy(loaded, tmp_path):
    """A private copy of the warehouse, so destructive maintenance doesn't affect other tests."""
    _, _, _, wh = loaded
    dst = tmp_path / "wh"
    shutil.copytree(wh, dst)
    # The SQLite catalog stores absolute file:// paths; point them at the copy.
    import sqlite3

    con = sqlite3.connect(dst / "catalog.db")
    for col in ("metadata_location", "previous_metadata_location"):
        con.execute(f"UPDATE iceberg_tables SET {col} = REPLACE({col}, ?, ?)", (str(wh), str(dst)))
    con.commit()
    con.close()
    for meta in dst.rglob("*.json"):
        meta.write_text(meta.read_text().replace(str(wh), str(dst)))
    for avro in dst.rglob("*.avro"):
        avro.write_bytes(avro.read_bytes().replace(str(wh).encode(), str(dst).encode()))
    return catalog(dst)


def rows(t):
    return sorted(t.scan().to_arrow().drop_columns(["ingest_batch"]).to_pylist(), key=lambda r: r["event_id"])


def test_compaction_reduces_files_and_keeps_every_row(copy):
    t = copy.load_table(BRONZE)
    before_rows, before_files = rows(t), len(t.inspect.files())
    res = compact(t, min_files=3)
    t = copy.load_table(BRONZE)
    assert res.partitions_rewritten > 0
    assert len(t.inspect.files()) < before_files
    assert rows(t) == before_rows


def test_expiring_snapshots_keeps_current_data(copy):
    t = copy.load_table(BRONZE)
    before = rows(t)
    n = len(t.snapshots())
    expired = expire_snapshots(t, keep_last=3)
    t = copy.load_table(BRONZE)
    assert expired == n - 3 and len(t.snapshots()) == 3
    assert rows(t) == before


def test_concurrent_writers_never_lose_a_commit(copy):
    a, b = copy.load_table(BRONZE), copy.load_table(BRONZE)  # two writers holding the same metadata
    schema = a.schema().as_arrow()
    base = a.scan().to_arrow().num_rows
    sample = a.scan(limit=5).to_arrow().cast(schema)

    a.append(sample)
    # b's view of the table is now stale. Its commit is a compare-and-swap on the metadata pointer, so it
    # is rejected and re-applied on top of a's snapshot instead of replacing it: neither write is lost.
    b.append(sample)
    assert copy.load_table(BRONZE).scan().to_arrow().num_rows == base + 10

    errors = []

    def writer():
        try:
            commit_with_retry(lambda: copy.load_table(BRONZE), lambda t: t.append(sample), attempts=10)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors
    assert copy.load_table(BRONZE).scan().to_arrow().num_rows == base + 10 + 4 * 5
