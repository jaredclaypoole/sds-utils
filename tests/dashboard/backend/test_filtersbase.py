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
