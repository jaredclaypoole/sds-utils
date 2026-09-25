"""Tests for reusable dashboard filters."""

import numpy as np
import pandas as pd

from sds_utils.dashboard.backend.filteredtable import Filters


def test_string_filter_handles_missing_values() -> None:
    data_df = pd.DataFrame({"instrument": ["hi", "lo", np.nan]})
    instrument_filter = Filters().instrument

    excluded_df = instrument_filter.apply(data_df, excluded_values_regex="hi")
    included_df = instrument_filter.apply(data_df, included_values_regex="hi")

    assert excluded_df.index.tolist() == [1, 2]
    assert included_df.index.tolist() == [0]


def test_string_filter_can_match_null_exactly() -> None:
    data_df = pd.DataFrame({"skipped_reason": [None, "dependency", "other"]})

    result = Filters().skipped_reason.apply(data_df, included_values=[None])

    assert result.index.tolist() == [0]


def test_date_filters_match_calendar_dates() -> None:
    data_df = pd.DataFrame(
        {
            "start_date": pd.to_datetime(
                ["2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z"]
            ),
            "end_date": pd.to_datetime(
                ["2026-08-01T23:59:59Z", "2026-08-02T23:59:59Z"]
            ),
        }
    )

    result = Filters().start_date.apply(data_df, value="2026-08-02")
    result = Filters().end_date.apply(result, value="2026-08-02")

    assert result.index.tolist() == [1]
