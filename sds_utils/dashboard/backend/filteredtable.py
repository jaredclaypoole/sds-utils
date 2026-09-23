"""Filtering and aggregation orchestration for dashboard tables."""

from collections import defaultdict
from collections.abc import Iterable

import pandas as pd

from .aggbase import (
    Aggregator,
    AggSpec,
)
from .data import DataSourceBase, QuerySpec
from .filtersbase import (
    FilterArguments,
    FiltersBase,
    StrHierarchySpec,
    StringRegisteredFilter,
)


def _make_data_level_hierachy(values: Iterable[str]) -> dict[str, list[str]]:
    values = [value for value in values if value.lower().startswith("l")]
    hierarchy_sets: dict[str, set[str]] = defaultdict(set)
    for value in values:
        prefix = value[:2]
        hierarchy_sets[prefix].add(value)
    return {key: sorted(values_set) for key, values_set in hierarchy_sets.items()}


class Filters(FiltersBase):
    """Declare the filters available to dashboard table consumers."""

    status = StringRegisteredFilter.property()
    instrument = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy={
                "ENA": "lo hi ultra".split(),
                "In-situ": "codice glows hit idex mag swapi swe".split(),
            },
        ),
    )
    data_level = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy=_make_data_level_hierachy,
        ),
    )
    partition_label = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy=dict(
                Short="daily repoint".split(),
            ),
            other="Long",
        ),
    )


class FilteredTable:
    """Coordinate data loading, registered filters, and aggregations."""

    def __init__(
        self,
        data_source: DataSourceBase,
        filters: Filters | None = None,
        agg: Aggregator | None = None,
    ) -> None:
        self._filters = filters or Filters()
        self._agg = agg or Aggregator()
        self._data_source = data_source
        self._query_spec: QuerySpec | None = None
        self._full_data_df: pd.DataFrame | None = None

    @property
    def filters(self) -> Filters:
        """Return the filters available for this table."""
        return self._filters

    def set_query(self, query_spec: QuerySpec) -> None:
        """Set the query used by the next data refresh."""
        self._query_spec = query_spec

    def refresh_data(self) -> None:
        """Reload the complete dataframe for the configured query."""
        assert self._query_spec is not None
        self._full_data_df = self._data_source.query(self._query_spec)

    def transform_data(
        self,
        filter_kwargs: FilterArguments | None = None,
        agg_spec: AggSpec | None = None,
    ) -> pd.DataFrame:
        """Apply requested filters and aggregation to the loaded dataframe."""
        if self._full_data_df is None:
            self.refresh_data()
        data_df = self._full_data_df
        data_df = self._filters.apply(data_df, filter_kwargs)
        if agg_spec is not None:
            data_df = self._agg.apply(data_df, agg_spec)
        return data_df
