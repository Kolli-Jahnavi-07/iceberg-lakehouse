import sys
from datetime import timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse import generator  # noqa: E402
from lakehouse.pipeline import Lakehouse  # noqa: E402
from lakehouse.tables import catalog, create_all  # noqa: E402

DAYS, VEHICLES, EVOLVE_DAY = 64, 25, 45


@pytest.fixture(scope="session")
def loaded(tmp_path_factory):
    """Run the whole pipeline once (partition evolution on day 45, new firmware field from day 60)."""
    wh = tmp_path_factory.mktemp("warehouse")
    cat = catalog(wh)
    create_all(cat)
    lh = Lakehouse(cat)
    fleet = generator.fleet(VEHICLES, seed=5)
    arrivals = generator.telemetry(fleet, DAYS, seed=9)
    results = {}
    for d in sorted(arrivals):
        if d == EVOLVE_DAY:
            lh.evolve_bronze_to_daily_partitions()
        day = generator.START + timedelta(days=d)
        results[d] = lh.process_batch(
            f"batch-{d:03d}", arrivals[d], as_of=day, leases=generator.lease_rows(fleet) if d == 0 else None
        )
    return lh, arrivals, results, wh
