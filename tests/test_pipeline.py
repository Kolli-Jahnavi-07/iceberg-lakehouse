from collections import defaultdict
from datetime import timedelta

import pytest

from lakehouse import generator
from lakehouse.tables import BRONZE, DAILY, GOLD, SILVER


def unique_events(arrivals):
    out = {}
    for d in sorted(arrivals):
        for e in arrivals[d]:
            out.setdefault(e["event_id"], e)
    return out


def test_bronze_keeps_everything_received(loaded):
    lh, arrivals, _, _ = loaded
    assert lh.table(BRONZE).scan().to_arrow().num_rows == sum(len(v) for v in arrivals.values())


def test_rerunning_a_batch_is_a_no_op(loaded):
    lh, arrivals, _, _ = loaded
    before = lh.table(BRONZE).current_snapshot().snapshot_id
    res = lh.process_batch("batch-003", arrivals[3], as_of=generator.START)
    assert res.skipped
    assert lh.table(BRONZE).current_snapshot().snapshot_id == before


def test_silver_has_each_event_exactly_once(loaded):
    lh, arrivals, results, _ = loaded
    ids = lh.table(SILVER).scan(selected_fields=("event_id",)).to_arrow().column("event_id").to_pylist()
    assert len(ids) == len(set(ids)) == len(unique_events(arrivals))
    assert sum(r.silver_duplicates for r in results.values()) > 0  # duplicates really were delivered


def test_daily_aggregates_match_a_reference_including_late_events(loaded):
    lh, arrivals, _, _ = loaded
    ref = defaultdict(lambda: {"km": 0.0, "kwh": 0.0, "sessions": 0})
    for e in unique_events(arrivals).values():
        r = ref[(e["vin"], e["event_ts"].date())]
        if e["event_type"] == "trip":
            r["km"] += e["trip_km"]
        else:
            r["kwh"] += e["energy_kwh"]
            r["sessions"] += 1
    got = {(r["vin"], r["day"]): r for r in lh.table(DAILY).scan().to_arrow().to_pylist()}
    assert set(got) == set(ref)
    for k, want in ref.items():
        g = got[k]
        assert g["km_driven"] == pytest.approx(want["km"], abs=1e-6)
        assert g["kwh_charged"] == pytest.approx(want["kwh"], abs=1e-6)
        assert g["charge_sessions"] == want["sessions"]
    late = [d for d, evs in arrivals.items() for e in evs if (e["event_ts"].date() - generator.START).days < d]
    assert late, "the feed must contain late events for this test to mean anything"


def test_schema_evolution_adds_firmware_version_without_rewriting(loaded):
    lh, _, results, _ = loaded
    assert any("firmware_version" in r.new_columns for r in results.values())
    silver = lh.table(SILVER)
    field = silver.schema().find_field("firmware_version")
    assert not field.required
    rows = silver.scan(selected_fields=("event_ts", "firmware_version")).to_arrow().to_pylist()
    early = [r for r in rows if (r["event_ts"].date() - generator.START).days < generator.FIRMWARE_DAY]
    late = [r for r in rows if (r["event_ts"].date() - generator.START).days >= generator.FIRMWARE_DAY]
    assert all(r["firmware_version"] is None for r in early)
    assert any(r["firmware_version"] for r in late)
    # An old snapshot still reads with the schema it was written with.
    first = silver.snapshots()[0]
    old = silver.scan(snapshot_id=first.snapshot_id).to_arrow()
    assert "firmware_version" not in old.column_names and old.num_rows > 0


def test_partition_evolution_keeps_old_files_and_prunes_both_specs(loaded):
    lh, _, _, _ = loaded
    bronze = lh.table(BRONZE)
    files = bronze.inspect.files().to_pylist()
    assert {f["spec_id"] for f in files} == {0, 1}
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    for offset in (10, 50):  # one day under the monthly spec, one under the daily spec
        day = generator.START + timedelta(days=offset)
        flt = And(
            GreaterThanOrEqual("event_ts", f"{day}T00:00:00+00:00"),
            LessThan("event_ts", f"{day + timedelta(days=1)}T00:00:00+00:00"),
        )
        planned = list(bronze.scan(row_filter=flt).plan_files())
        assert 0 < len(planned) < len(files) / 3
        got = bronze.scan(row_filter=flt).to_arrow().num_rows
        all_rows = bronze.scan().to_arrow().to_pylist()
        assert got == sum(1 for r in all_rows if r["event_ts"].date() == day)


def test_gold_time_travel_reproduces_a_past_report(loaded):
    lh, _, _, _ = loaded
    day = generator.START + timedelta(days=30)
    past = lh.gold_as_of(day).to_pylist()
    assert past and {r["as_of_day"] for r in past} == {day}
    current = lh.table(GOLD).scan().to_arrow().to_pylist()
    assert {r["as_of_day"] for r in current} != {day}
    with pytest.raises(KeyError):
        lh.gold_as_of(generator.START - timedelta(days=1))


def test_risk_tiers_are_consistent(loaded):
    lh, _, _, _ = loaded
    for r in lh.table(GOLD).scan().to_arrow().to_pylist():
        p = r["projected_soh_at_return"]
        assert r["risk_tier"] == ("HIGH" if p < 88 else "MEDIUM" if p < 92 else "LOW")
        assert (r["residual_at_risk"] > 0) == (p < 92)
        assert p <= r["soh_pct"] + 1e-9
