import pandas as pd
from pydantic import BaseModel

from .data import DataSchema, DataSourceBase, QuerySpec
from .filtersbase import (
    FilterArguments,
    FiltersBase,
    StrHierarchySpec,
    StringRegisteredFilter,
)


class Filters(FiltersBase):
    status = StringRegisteredFilter.property()
    instrument = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy={
                "ENA": "lo hi ultra".split(),
                "In-situ": "codice glows hit idex mag swapi swe".split(),
            },
            missing="Other",
        ),
    )


class FilteredTable:
    def __init__(
        self,
        data_source: DataSourceBase,
        filters: Filters | None = None,
    ) -> None:
        self._filters = filters or Filters()
        self._data_source = data_source
        self._query_spec: QuerySpec | None = None
        self._full_data_df: pd.DataFrame[DataSchema] | None = None

    def set_query(self, query_spec: QuerySpec) -> None:
        self._query_spec = query_spec

    def refresh_data(self) -> None:
        assert self._query_spec is not None
        self._full_data_df = self._data_source.query(self._query_spec)

    def transform_data(self, filter_kwargs: FilterArguments | None = None) -> pd.DataFrame:
        if self._full_data_df is None:
            self.refresh_data()
        return self._filters.apply(self._full_data_df, filter_kwargs)
