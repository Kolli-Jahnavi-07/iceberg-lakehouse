"""Synthetic telemetry from a fleet of leased electric vehicles.

Why this data: a lessor carries the residual-value risk of every EV it leases, and battery health
(state of health, SOH) is the biggest driver of an EV's resale value. Vehicles report trips and
charging sessions; the lakehouse turns that into daily usage and a residual-risk score per lease.

The feed behaves like real connected-car data:
  * events arrive in daily batches, but ~3% show up 1-3 days late (no connectivity)
  * ~1% are delivered twice (same event_id)
  * on day 60 a firmware update starts reporting a new field, `firmware_version`
  * a small share of vehicles degrade much faster (heavy DC fast charging, hot climates)

All values are fictional. Same seed, same data.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

START = date(2026, 1, 1)
MODELS = {  # model: (battery_kwh, efficiency_km_per_kwh, msrp)
    "Equinox EV": (85, 5.4, 34995),
    "Blazer EV": (85, 5.0, 48795),
    "Silverado EV": (205, 3.1, 73100),
    "Lyriq": (102, 4.8, 58590),
    "Hummer EV": (212, 2.5, 96550),
}
CLIMATES = {"AZ": 1.6, "TX": 1.4, "FL": 1.3, "CA": 1.1, "GA": 1.2, "OH": 0.9, "MI": 0.85, "MA": 0.85, "NY": 0.9, "IL": 0.9}
FIRMWARE_DAY = 60


@dataclass
class Vehicle:
    vin: str
    model: str
    state: str
    lease_start: date
    lease_months: int
    residual_value: float
    odometer: float
    soh: float
    degradation: float  # % SOH lost per 10,000 km before climate and charging modifiers
    dcfc_habit: float  # probability a charge is DC fast charging
    daily_km: float


def fleet(n: int = 400, seed: int = 42) -> list[Vehicle]:
    rng = random.Random(seed)
    out = []
    for i in range(n):
        model = rng.choice(list(MODELS))
        state = rng.choice(list(CLIMATES))
        months = rng.choice([24, 36, 39])
        msrp = MODELS[model][2]
        heavy = rng.random() < 0.08
        lease_start = START - timedelta(days=rng.randint(30, 700))
        per_10k = rng.uniform(2.6, 4.0) if heavy else rng.uniform(0.6, 1.3)
        age_km = (START - lease_start).days * rng.uniform(35, 70)
        out.append(
            Vehicle(
                vin=f"1GEV{rng.choice('ABCDEFGHJK')}{i:012d}",
                model=model,
                state=state,
                lease_start=lease_start,
                lease_months=months,
                residual_value=round(msrp * {24: 0.62, 36: 0.52, 39: 0.49}[months], -1),
                odometer=age_km + rng.uniform(5, 300),
                soh=100 - rng.uniform(0.5, 2.0) - per_10k * age_km / 10_000 * CLIMATES[state],
                degradation=per_10k,
                dcfc_habit=rng.uniform(0.55, 0.9) if heavy else rng.uniform(0.02, 0.3),
                daily_km=max(8.0, rng.gauss(55, 22)),
            )
        )
    return out


def lease_rows(vehicles: list[Vehicle]) -> list[dict]:
    return [
        {
            "vin": v.vin,
            "model": v.model,
            "state": v.state,
            "lease_start": v.lease_start,
            "lease_months": v.lease_months,
            "residual_value": v.residual_value,
            "battery_kwh": float(MODELS[v.model][0]),
        }
        for v in vehicles
    ]


def telemetry(vehicles: list[Vehicle], days: int = 90, seed: int = 7) -> dict[int, list[dict]]:
    """Events keyed by the day (0-based) on which they *arrive*."""
    rng = random.Random(seed)
    arrivals: dict[int, list[dict]] = {d: [] for d in range(days + 4)}
    seq = 0
    for d in range(days):
        day = START + timedelta(days=d)
        for v in vehicles:
            kwh, eff, _ = MODELS[v.model]
            if rng.random() < 0.12:  # parked all day
                continue
            trips = rng.choice([1, 2, 2, 3])
            km_today = max(1.0, rng.gauss(v.daily_km, v.daily_km * 0.35))
            soc = rng.uniform(55, 95)
            minute = rng.randint(6 * 60, 9 * 60)
            events = []
            for _ in range(trips):
                km = km_today / trips
                v.odometer += km
                soc = max(4.0, soc - km / eff / kwh * 100)
                minute += rng.randint(20, 240)
                events.append(("trip", minute, km / eff, False, km))
            if soc < 45 or rng.random() < 0.3:
                dcfc = rng.random() < v.dcfc_habit
                added = min(100 - soc, rng.uniform(25, 70))
                soc += added
                minute += rng.randint(15, 120)
                events.append(("charge", minute, added / 100 * kwh, dcfc, None))
            v.soh -= v.degradation * km_today / 10_000 * CLIMATES[v.state] * (1 + v.dcfc_habit)
            v.soh = max(60.0, v.soh)
            for etype, mins, energy, dcfc, trip_km in events:
                seq += 1
                ts = datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(minutes=min(mins, 1439))
                e = {
                    "event_id": f"EV{seq:09d}",
                    "vin": v.vin,
                    "event_ts": ts,
                    "event_type": etype,
                    "trip_km": round(trip_km, 1) if trip_km is not None else None,
                    "odometer_km": round(v.odometer, 1),
                    "battery_soc_pct": round(soc, 1),
                    "battery_soh_pct": round(v.soh + rng.gauss(0, 0.05), 2),
                    "energy_kwh": round(energy, 2),
                    "dc_fast_charge": dcfc,
                    "ambient_temp_c": round(rng.gauss(18 + 8 * (CLIMATES[v.state] - 1) * 5, 7), 1),
                }
                if d >= FIRMWARE_DAY and rng.random() < 0.7:
                    e["firmware_version"] = rng.choice(["24.3.1", "24.3.2"])
                delay = rng.randint(1, 3) if rng.random() < 0.03 else 0
                arrivals[d + delay].append(e)
                if rng.random() < 0.01:
                    arrivals[min(d + delay + rng.randint(0, 2), days + 3)].append(dict(e))
    return arrivals
