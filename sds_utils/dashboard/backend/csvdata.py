import datetime
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from .data import DataSchema, QuerySpec


class CSVDataSchema(pa.DataFrameModel):
    """Schema of the unmodified dashboard CSV rows."""

    Instrument: str
    DataLevel: str = pa.Field(alias="Data level")
    Descriptor: str
    Partition: str
    Updated: str = pa.Field(alias="Updated (UTC)", nullable=True)
    Status: str
    MissingFile: str = pa.Field(alias="Missing file", nullable=True)
    SkipReason: str = pa.Field(alias="Skip reason", nullable=True)
    MissingFiles: str = pa.Field(alias="Missing files", nullable=True)
    PartitionLink: str = pa.Field(alias="Partition link")

    class Config:
        """Require the CSV to contain exactly the declared columns."""

        strict = True


    @classmethod
    def convert_data(cls, raw_df: pd.DataFrame["CSVDataSchema"]) -> pd.DataFrame["DataSchema"]:
        """Convert raw CSV columns and partition strings into normalized data."""
        raw_df = CSVDataSchema.validate(raw_df, lazy=True)
        partition_parts = raw_df["Partition"].str.extract(
            r"^(?P<partition_label>.+)_"
            r"(?P<start_time>\d{4}-\d{2}-\d{2}T[^_]+)_to_"
            r"(?P<end_time>\d{4}-\d{2}-\d{2}T.+)$"
        )

        if partition_parts.isna().any(axis=None):
            invalid_partitions = raw_df.loc[
                partition_parts.isna().any(axis=1), "Partition"
            ].tolist()
            raise ValueError(
                f"Unable to parse partition values: {invalid_partitions[:3]}"
            )

        partition_labels = partition_parts["partition_label"]
        is_repoint = partition_labels.str.startswith("repoint")
        repoint_values = partition_labels.where(is_repoint).str.removeprefix(
            "repoint"
        )
        invalid_repoints = is_repoint & ~repoint_values.str.fullmatch(
            r"[+-]?\d+", na=False
        )
        if invalid_repoints.any():
            invalid_labels = partition_labels.loc[invalid_repoints].unique().tolist()
            raise ValueError(f"Invalid repoint labels: {invalid_labels}")
        repoint = pd.to_numeric(repoint_values).astype("Int64")
        partition_labels = partition_labels.where(~is_repoint, "repoint")

        converted = pd.DataFrame(
            {
                "asset": (
                    raw_df["Instrument"]
                    + "_"
                    + raw_df["Data level"]
                    + "_"
                    + raw_df["Descriptor"]
                ),
                "instrument": raw_df["Instrument"],
                "data_level": raw_df["Data level"],
                "descriptor": raw_df["Descriptor"],
                "partition": raw_df["Partition"],
                "partition_label": partition_labels,
                "repoint": repoint,
                "start_time": pd.to_datetime(
                    partition_parts["start_time"], utc=True
                ),
                "end_time": pd.to_datetime(partition_parts["end_time"], utc=True),
                "updated": pd.to_datetime(raw_df["Updated (UTC)"], utc=True),
                "status": raw_df["Status"],
                "missing_file": raw_df["Missing file"],
                "skip_reason": raw_df["Skip reason"],
                "missing_files": raw_df["Missing files"],
                "partition_link": raw_df["Partition link"],
            }
        )
        return DataSchema.validate(converted, lazy=True)


class CSVDataSource:
    def query(self, query: QuerySpec) -> pd.DataFrame[DataSchema]:
        assert query.start_time >= datetime.datetime(2026, 8, 1)
        assert query.end_time < datetime.datetime(2026, 8, 30)

        start_time_pd = pd.to_datetime(query.start_time, utc=True, unit="ns")
        end_time_pd = pd.to_datetime(query.end_time, utc=True, unit="ns")

        path = Path(
            "dashboard-output/august-only/aug_1-29/run_2026-08-31_06-57/imap-run-status-all-rows-20260831T105726Z.csv"
        )
        _raw_df = pd.read_csv(path)
        raw_df = CSVDataSchema.validate(_raw_df, lazy=True)
        data_df = CSVDataSchema.convert_data(raw_df)
        date_mask = (
            (data_df["start_time"] >= start_time_pd) &
            (data_df["end_time"] <= end_time_pd)
        )
        data_df_filtered = data_df[date_mask]
        return data_df_filtered
