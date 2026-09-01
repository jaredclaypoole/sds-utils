import pandas as pd

from .data import DataSchema, DataSourceBase, QuerySpec


class FilteredTable:
    def __init__(self, data_source: DataSourceBase) -> None:
        self._data_source = data_source
        self._query_spec: QuerySpec | None = None
        self._full_data_df: pd.DataFrame[DataSchema] | None = None

    def set_query(self, query_spec: QuerySpec) -> None:
        self._query_spec = query_spec

    def refresh_data(self) -> None:
        assert self._query_spec is not None
        self._full_data_df = self._data_source.query(self._query_spec)

    def transform_data(self) -> pd.DataFrame:
        assert self._full_data_df is not None
        return self._full_data_df

    def data(self, refresh: bool = False) -> pd.DataFrame:
        if refresh or self._full_data_df is None:
            self.refresh_data()
        return self.transform_data()
