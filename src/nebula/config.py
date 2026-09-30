from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration for the Nebula pipeline."""

    input_path: str = "data/raw/data_group_*.csv"
    output_path: str = "output"
    timezone: str = "UTC"
    expected_start: datetime | None = None
    expected_end: datetime | None = None
    iqr_multiplier: float = 1.5

    @property
    def bronze_path(self) -> str:
        return str(Path(self.output_path) / "bronze" / "turbine_measurements")

    @property
    def silver_path(self) -> str:
        return str(Path(self.output_path) / "silver" / "turbine_measurements")

    @property
    def statistics_path(self) -> str:
        return str(Path(self.output_path) / "gold" / "turbine_daily_statistics")

    @property
    def anomalies_path(self) -> str:
        return str(Path(self.output_path) / "gold" / "turbine_daily_anomalies")
