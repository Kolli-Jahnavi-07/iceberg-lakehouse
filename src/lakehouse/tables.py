"""Catalog and table definitions.

Locally the catalog is a SQLite-backed `SqlCatalog` with the warehouse on disk. In production the
same code points at AWS Glue or an Iceberg REST catalog (Snowflake Open Catalog / Polaris) through
PyIceberg's standard configuration, e.g.

    PYICEBERG_CATALOG__LAKEHOUSE__TYPE=glue
    PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE=s3://acme-lakehouse/warehouse
"""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

BRONZE = "bronze.telemetry_events"
SILVER = "silver.telemetry"
DAILY = "silver.vehicle_daily"
LEASES = "silver.leases"
GOLD = "gold.residual_risk"


def catalog(warehouse: str | Path | None = None) -> Catalog:
    if os.environ.get("PYICEBERG_CATALOG__LAKEHOUSE__TYPE"):
        return load_catalog("lakehouse")
    root = Path(warehouse or "data/lakehouse").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("lakehouse", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=f"file://{root / 'warehouse'}")


def _telemetry_fields() -> list[NestedField]:
    return [
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "vin", StringType(), required=True),
        NestedField(3, "event_ts", TimestamptzType(), required=True),
        NestedField(4, "event_type", StringType(), required=True),
        NestedField(5, "odometer_km", DoubleType()),
        NestedField(6, "battery_soc_pct", DoubleType()),
        NestedField(7, "battery_soh_pct", DoubleType()),
        NestedField(8, "energy_kwh", DoubleType()),
        NestedField(9, "dc_fast_charge", BooleanType()),
        NestedField(10, "ambient_temp_c", DoubleType()),
        NestedField(11, "ingest_batch", StringType(), required=True),
        NestedField(12, "trip_km", DoubleType()),
    ]


def _create(cat: Catalog, name: str, schema: Schema, spec: PartitionSpec, sort: SortOrder | None = None) -> Table:
    try:
        return cat.load_table(name)
    except NoSuchTableError:
        return cat.create_table(
            name,
            schema=schema,
            partition_spec=spec,
            sort_order=sort or SortOrder(),
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
                "write.metadata.delete-after-commit.enabled": "true",
                "write.metadata.previous-versions-max": "50",
            },
        )


def create_all(cat: Catalog) -> dict[str, Table]:
    for ns in ("bronze", "silver", "gold"):
        try:
            cat.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass

    # Bronze starts partitioned by month: early volume is small and daily partitions would mean
    # thousands of tiny files. It is evolved to daily partitions later without rewriting data.
    bronze = _create(
        cat, BRONZE, Schema(*_telemetry_fields()), PartitionSpec(PartitionField(3, 1000, MonthTransform(), "event_month"))
    )
    silver = _create(
        cat,
        SILVER,
        Schema(*_telemetry_fields(), identifier_field_ids=[1]),
        PartitionSpec(PartitionField(3, 1000, DayTransform(), "event_day")),
        SortOrder(SortField(source_id=2, transform=IdentityTransform())),
    )
    daily = _create(
        cat,
        DAILY,
        Schema(
            NestedField(1, "vin", StringType(), required=True),
            NestedField(2, "day", DateType(), required=True),
            NestedField(3, "km_driven", DoubleType()),
            NestedField(4, "kwh_charged", DoubleType()),
            NestedField(5, "dc_fast_kwh", DoubleType()),
            NestedField(6, "charge_sessions", IntegerType()),
            NestedField(7, "min_soc_pct", DoubleType()),
            NestedField(8, "soh_pct", DoubleType()),
            NestedField(9, "odometer_km", DoubleType()),
            NestedField(10, "avg_temp_c", DoubleType()),
            identifier_field_ids=[1, 2],
        ),
        PartitionSpec(PartitionField(2, 1000, MonthTransform(), "day_month")),
    )
    leases = _create(
        cat,
        LEASES,
        Schema(
            NestedField(1, "vin", StringType(), required=True),
            NestedField(2, "model", StringType()),
            NestedField(3, "state", StringType()),
            NestedField(4, "lease_start", DateType()),
            NestedField(5, "lease_months", IntegerType()),
            NestedField(6, "residual_value", DoubleType()),
            NestedField(7, "battery_kwh", DoubleType()),
            identifier_field_ids=[1],
        ),
        PartitionSpec(),
    )
    gold = _create(
        cat,
        GOLD,
        Schema(
            NestedField(1, "vin", StringType(), required=True),
            NestedField(2, "model", StringType()),
            NestedField(3, "state", StringType()),
            NestedField(4, "soh_pct", DoubleType()),
            NestedField(5, "km_last_30d", DoubleType()),
            NestedField(6, "soh_loss_per_10k_km", DoubleType()),
            NestedField(7, "dc_fast_share", DoubleType()),
            NestedField(8, "months_remaining", IntegerType()),
            NestedField(9, "projected_soh_at_return", DoubleType()),
            NestedField(10, "risk_tier", StringType()),
            NestedField(11, "residual_value", DoubleType()),
            NestedField(12, "residual_at_risk", DoubleType()),
            NestedField(13, "as_of_day", DateType(), required=True),
        ),
        PartitionSpec(PartitionField(10, 1000, IdentityTransform(), "risk_tier")),
    )
    return {BRONZE: bronze, SILVER: silver, DAILY: daily, LEASES: leases, GOLD: gold}
