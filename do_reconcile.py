import marimo

__generated_with = "0.24.0"
app = marimo.App()

with app.setup:
    # Initialization code that runs before all other cells
    from collections.abc import Sequence
    import datetime
    from pathlib import Path
    from typing import Iterable, TypeVar, Protocol, Any, Self

    import numpy as np
    import pandas as pd
    from pandas.api.typing import NAType
    import marimo as mo

    # from tqdm.contrib.concurrent import thread_map
    from moutils.concurrent import thread_map

    from sds_utils.scrubber import DAILY_INSTRUMENTS
    # from reconcile import latest_attempt_failed_after_previous_success_cached
    from reconcile import latest_attempt_failed_after_previous_success_db


    index_cols = "Instrument Level Descriptor".split()


@app.cell
def _():
    # scrubber_base_dir = Path("output/july-august-only/no-combine-missing")
    scrubber_base_dir = Path("output/scrubber/full/run_2026-08-26/no-combine-missing")
    fpath_missing_days = scrubber_base_dir / "missing_days.csv"
    fpath_missing_repoints = scrubber_base_dir / "missing_repoints.csv"
    fpath_all_products = scrubber_base_dir / "all_products.csv"

    dashboard_base_dir = Path("dashboard-output/july-only")
    fpath_dashboard_output = dashboard_base_dir / "imap-run-status-all-rows-20260820T172016Z.csv"
    return (
        fpath_all_products,
        fpath_dashboard_output,
        fpath_missing_days,
        fpath_missing_repoints,
    )


@app.function
def extract_day(partition: str) -> datetime.date:
    split = partition.split("_")
    if len(split) < 4:
        raise ValueError("TODO")
    day = datetime.datetime.strptime(split[-3][:10], "%Y-%m-%d").date()
    return day


@app.function
def extract_desc(partition: str) -> str:
    split = partition.split("_")
    if len(split) < 4:
        raise ValueError("TODO")
    desc = "_".join(split[:-3])
    if desc.startswith("repoint"):
        return "repoint"
    return desc


@app.function
def extract_repoint_number(partition: str) -> int | NAType:
    split = partition.split("_")
    if len(split) < 4:
        raise ValueError("TODO")
    desc = "_".join(split[:-3])
    prefix = "repoint"
    if not desc.startswith(prefix):
        return pd.NA
    return int(desc.removeprefix(prefix))


@app.function
def post_process_scrubber(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if str(col).endswith("Date"):
            df[col] = df[col].map(lambda s: datetime.datetime.strptime(str(s), '%Y%m%d').date())
    # drop anomalous single-day row that breaks uniqueness
    mask = (
        (df.Instrument == "lo") &
        (df.Descriptor == "good-times") # NOT goodtimes
    )
    df = df[~mask]
    df['Descriptor'] = df.Descriptor.str.replace('-', '')
    return df


@app.function
def post_process_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns={"Data level": "Level"})
    df['Date'] = df.Partition.map(extract_day)
    df['Type'] = df.Partition.map(extract_desc)
    df['Repoint'] = df.Partition.map(extract_repoint_number)
    df['Asset'] = df.Instrument + "_" + df.Level + "_" + df.Descriptor
    return df


@app.cell
def _(
    fpath_all_products,
    fpath_dashboard_output,
    fpath_missing_days,
    fpath_missing_repoints,
):
    missing_days_df = post_process_scrubber(pd.read_csv(fpath_missing_days))
    missing_repoints_df = post_process_scrubber(pd.read_csv(fpath_missing_repoints))
    all_products_df = post_process_scrubber(pd.read_csv(fpath_all_products))
    dashboard_df = post_process_dashboard(
        pd.read_csv(fpath_dashboard_output))
    return all_products_df, dashboard_df, missing_days_df, missing_repoints_df


@app.function
def mark_scrubber_out_of_range(dashboard_df: pd.DataFrame, all_products_df: pd.DataFrame) -> pd.DataFrame:
    """Return a marked copy of dashboard_df"""

    _apdf = all_products_df.copy()
    _ddf = dashboard_df.copy()

    _apdf = _apdf.set_index(index_cols)
    _ddf = _ddf.set_index(index_cols)

    _apdf = _apdf.sort_index()
    _ddf = _ddf.sort_index()

    for _idx, _row in _ddf.iterrows():
        _in_scrubber = bool(_idx in _apdf.index)
        _ddf.loc[_idx, "product_in_scrubber"] = _in_scrubber
        if not _in_scrubber:
            _scrubber_oor = True
        else:
            _scrubber_oor = not (
                _apdf.loc[_idx].MinStartDate <= _row.Date <= _apdf.loc[_idx].MaxStartDate
            )
        _ddf.loc[_idx, "scrubber_out_of_range"] = _scrubber_oor

    for col in "product_in_scrubber scrubber_out_of_range".split():
        _ddf[col] = _ddf[col].astype(bool)

    return _ddf.reset_index()


@app.function
def cache_fwps(dashboard_df: pd.DataFrame) -> None:
    _df = dashboard_df
    _assets = list(_df.Asset)
    _partitions = list(_df.Partition)
    thread_map(
        lambda tup: latest_attempt_failed_after_previous_success_db(*tup),
        list(zip(_assets, _partitions, strict=True)),
        max_workers=10,
    )


@app.function
def mark_fwps(dashboard_df: pd.DataFrame) -> pd.DataFrame:
    """Return a marked copy of dashboard_df"""
    _ddf = dashboard_df.copy()

    raise NotImplementedError()

    return _ddf


@app.cell
def _(dashboard_df):
    cache_fwps(dashboard_df)
    return


@app.cell
def _(dashboard_df):
    len(dashboard_df)
    return


@app.cell
def _(all_products_df, dashboard_df):
    ddf_markup = dashboard_df.copy()

    ddf_markup = mark_scrubber_out_of_range(ddf_markup, all_products_df)
    ddf_markup = ddf_markup[~ddf_markup.scrubber_out_of_range]

    len(dashboard_df), len(ddf_markup)
    return (ddf_markup,)


@app.cell
def _(ddf_markup):
    ddf_markup.scrubber_out_of_range.value_counts()
    return


@app.cell
def _(ddf_markup):
    _ddf = ddf_markup
    len(_ddf), _ddf.product_in_scrubber.sum(), _ddf.scrubber_out_of_range.sum()
    return


@app.cell
def _(all_products_df):

    _apdf = all_products_df
    _apdf
    return


@app.cell
def _(dashboard_df):
    ddf = dashboard_df
    ddf = ddf[ddf["Status"] != 'materialized']
    daily_instruments = set(DAILY_INSTRUMENTS)
    _df = ddf['Instrument,Level,Descriptor,Date,Repoint'.split(',')]
    _df = _df[_df.Instrument.map(lambda instr: instr in daily_instruments)]
    _df = _df[_df.Repoint.isna()]
    _df = _df.drop(columns=['Repoint'])
    mddf_days = _df
    _df = ddf['Instrument,Level,Descriptor,Repoint'.split(',')]
    _df = _df[~_df.Repoint.isna()]
    mddf_repoints = _df
    return ddf, mddf_days, mddf_repoints


@app.cell
def _(ddf):
    ddf;
    return


@app.cell
def _(mddf_days, missing_days_df):
    ddf_1 = mddf_days
    _df = missing_days_df
    days_index_cols = 'Instrument,Level,Descriptor,Date'.split(',')
    _jdf = ddf_1.set_index(days_index_cols).assign(from_dashboard=True).join(_df.set_index(days_index_cols).assign(from_scrubber=True), how='outer')
    _jdf = _jdf.fillna(False)
    joined_df_days = _jdf
    _jdf;
    return days_index_cols, joined_df_days


@app.cell
def _(days_index_cols, joined_df_days):
    _df = joined_df_days
    _df = _df.reset_index()
    _df = _df[_df.Date >= datetime.date(2026, 7, 1)]
    _df = _df[_df.Date < datetime.date(2026, 8, 1)]
    _df = _df[_df.from_dashboard != _df.from_scrubber]
    print(f'len(df)={len(_df)!r}')
    days_index_not_in_scrubber = _df.set_index(days_index_cols).index

    assert _df.from_scrubber.sum() == 0
    return (days_index_not_in_scrubber,)


@app.cell
def _(missing_days_df):
    _df = missing_days_df
    _df = _df[_df.Date >= datetime.date(2026, 7, 1)]
    _df = _df[_df.Date < datetime.date(2026, 8, 1)]
    print(f'len(df)={len(_df)!r}')
    _df;
    return


@app.cell
def _(dashboard_df, days_index_cols, days_index_not_in_scrubber):
    ddf_days_not_in_scrubber = dashboard_df.set_index(days_index_cols).loc[days_index_not_in_scrubber].reset_index()
    return (ddf_days_not_in_scrubber,)


@app.cell
def _():
    class SupportsSubtraction(Protocol):
        def __sub__(self, other: Self) -> Any: ...

    T = TypeVar("T")

    def value_ranges[T: SupportsSubtraction](values: Iterable[T], *, unit: T, collapse: bool = False) \
        -> Sequence[tuple[T, T] | T]:
        prev = None
        start = None
        end = None
        runs: list[tuple[T, T]] = []
        for val in values:
            if prev is None:
                prev = val
                start = val
                end = val
            elif prev == val - unit:
                prev = val
                end = val
            else:
                assert start is not None
                assert end is not None
                runs.append((start, end))
                prev = val
                start = val
                end = val
        else:
            if start is not None and end is not None:
                runs.append((start, end))

        if not collapse:
            return runs
        return [
            (start, end) if start != end else start
            for start, end in runs
        ]

    def partition_ranges(values: Iterable[int], *, unit: int = 1, collapse: bool = False) \
        -> list[tuple[int, int] | int]:
        return value_ranges(values, unit=unit, collapse=collapse)  # type: ignore

    def date_ranges(dates: Iterable[datetime.date], collapse: bool = False) \
        -> Sequence[tuple[datetime.date, datetime.date] | datetime.date]:
        return value_ranges(dates, collapse=collapse, unit=datetime.timedelta(days=1))  # type: ignore

    return date_ranges, partition_ranges


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    * We've shown all missing days indicated by the scrubber (starting 2026-08-01) are also indicated by the dashboard
    * Now we filter the missing days indicated by the dashboard but not the scrubber
      * First we aggregate dates into consecutive ranges
      * Next we drop products that are missing for a single range that includes the end date, as those will be invisible to the scrubber
      * We're left with a single product that happened to be materialized in the past but had a failed latest run (with updated dependencies)
        * This would be invisible to the scrubber
    """)
    return


@app.cell
def _(date_ranges, ddf_days_not_in_scrubber):
    ddf_2 = ddf_days_not_in_scrubber
    start_date = ddf_2.Date.min()
    end_date = ddf_2.Date.max()
    full_date_range = [(start_date, end_date)]
    print((start_date, end_date))
    _grouped = ddf_2.groupby('Instrument,Level,Descriptor'.split(','))

    def _make_df(_df: pd.DataFrame) -> pd.Series:
        return pd.Series(
            dict(
                date_ranges=_df.Date.apply(date_ranges, by_row=False, collapse=False),
                first_link=_df['Partition link'].iloc[0]
            ),
        )
    _df = _grouped.apply(_make_df)
    _mask = _df.date_ranges.map(lambda r: r != full_date_range)
    _df = _df[_mask]
    _mask = _df.date_ranges.map(lambda r: len(r) > 1 or r[0][1] != end_date)
    _df = _df[_mask]
    ddf_days_not_in_scrubber_filtered = _df
    _df
    return (ddf_days_not_in_scrubber_filtered,)


@app.cell
def _(mddf_repoints, missing_repoints_df):
    ddf_3 = mddf_repoints
    _df = missing_repoints_df
    repoints_index_cols = 'Instrument,Level,Descriptor,Repoint'.split(',')
    _jdf = ddf_3.set_index(repoints_index_cols).assign(from_dashboard=True).join(_df.set_index(repoints_index_cols).assign(from_scrubber=True), how='outer')
    _jdf = _jdf.fillna(False)
    joined_df_repoints = _jdf
    _jdf;
    return joined_df_repoints, repoints_index_cols


@app.cell
def _(dashboard_df, joined_df_repoints, repoints_index_cols):
    _df = joined_df_repoints
    _df = _df.reset_index()
    _min_repoint = dashboard_df.Repoint.min()
    _max_repoint = dashboard_df.Repoint.max()
    _df = _df[_df.Repoint >= _min_repoint]
    _df = _df[_df.Repoint <= _max_repoint]
    _df = _df[_df.from_dashboard != _df.from_scrubber]
    print(f'len(df)={len(_df)!r}')
    repoints_index_not_in_scrubber = _df.set_index(repoints_index_cols).index

    assert _df.from_scrubber.sum() == 0

    _df;
    return (repoints_index_not_in_scrubber,)


@app.cell
def _(dashboard_df, repoints_index_cols, repoints_index_not_in_scrubber):
    ddf_repoints_not_in_scrubber = dashboard_df.set_index(repoints_index_cols).loc[repoints_index_not_in_scrubber].reset_index()
    return (ddf_repoints_not_in_scrubber,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    * We've shown all missing repoints indicated by the scrubber (starting with the min repoint reported by the dashboard) are also indicated by the dashboard
    * Now we filter the missing repoints indicated by the dashboard but not the scrubber
      * First we aggregate repoints into consecutive ranges
      * Next we drop products that are missing for a single range that includes the final repoint, as those will be invisible to the scrubber
      * We're left with just an l0 product, which the scrubber specifically excludes
    """)
    return


@app.cell
def _(ddf_repoints_not_in_scrubber, partition_ranges):
    ddf_4 = ddf_repoints_not_in_scrubber
    _min_repoint = ddf_4.Repoint.min().item()
    max_repoint = ddf_4.Repoint.max().item()
    full_repoint_range = [(_min_repoint, max_repoint)]
    print(full_repoint_range[0])
    _grouped = ddf_4.groupby('Instrument,Level,Descriptor'.split(','))

    def _make_repoints_df(_df: pd.DataFrame) -> pd.Series:
        return pd.Series(
            dict(
                repoint_ranges=_df.Repoint.apply(partition_ranges, by_row=False, collapse=False),
                first_link=_df['Partition link'].iloc[0],
            )
        )
    _df = _grouped.apply(_make_repoints_df)
    # _mask = _df.repoint_ranges.map(lambda r: r != full_repoint_range)
    _mask = _df.repoint_ranges.map(lambda r: len(r) > 1 or r[0][-1] != max_repoint)
    _df = _df[_mask]
    _df = _df.sort_values('repoint_ranges')

    def f_mask(ranges: list[tuple[int, int]]) -> bool:
        if len(ranges) > 1:
            return True
        return ranges[0][-1] != max_repoint
    _df = _df[_df.repoint_ranges.map(f_mask)]

    _df = _df.reset_index()

    # _levels_to_exclude = set("l1a l1b".split())
    # _mask = (
    #     (_df.Instrument == "ultra")
    #     &
    #     _df.Level.map(lambda s: s in _levels_to_exclude)
    #     &
    #     _df.repoint_ranges.map(lambda rr: rr == [(295, 300)])
    # )
    # _df = _df[~_mask]

    _df = _df.sort_values(by="Instrument Level Descriptor".split())



    # with pd.option_context('display.max_colwidth', None):
    #     display(_df)

    ddf4_grouped = _df

    _df
    return ddf4_grouped, ddf_4


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    * Possible improvements to the scrubber
      * Get a list of all products rather than only sensing data products available to imap-data-access
        * This will catch products that have never been materialized
      * Revise the logic so missing products with no "bookending" presence are caught
        * This will catch products never materialized before or after a certain date
    """)
    return


@app.cell
def _(ddf4_grouped, ddf_4):
    _dfg = ddf4_grouped
    _df = ddf_4.copy()


    _dfg = _dfg.set_index(index_cols)
    _df = _df.set_index(index_cols)

    _dfg

    _dfg.repoint_ranges
    _df.loc[_dfg.index, "repoint_ranges"] = _dfg.repoint_ranges
    _df
    return


@app.cell
def _(ddf_4):
    _df = ddf_4
    _assets = list(_df.Asset)
    _partitions = list(_df.Partition)
    _do_thread_map = False
    if _do_thread_map:
        thread_map(
            lambda tup: latest_attempt_failed_after_previous_success_db(*tup),
            list(zip(_assets, _partitions, strict=True)),
            max_workers=10,
        )
    return


@app.cell
def _(ddf_4):
    _df = ddf_4
    _df = _df.iloc[-18:]
    _assets = list(_df.Asset)
    _partitions = list(_df.Partition)
    _the_iter = list(zip(_assets, _partitions, strict=True))

    print(len(_the_iter))

    # for _asset, _partition in mo.status.progress_bar(_the_iter):
    for _asset, _partition in _the_iter:
        break
    print(_asset, _partition)
    _do_fcn_call = False
    if _do_fcn_call:
        latest_attempt_failed_after_previous_success_db(_asset, _partition)
    return


@app.cell
def _(ddf_days_not_in_scrubber_filtered):
    _df = ddf_days_not_in_scrubber_filtered
    _df
    return


@app.cell
def _(ddf_4):
    _to_write = False
    _df = ddf_4
    if _to_write:
        pd.DataFrame.to_csv(_df, "temp.csv")
    # _df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
