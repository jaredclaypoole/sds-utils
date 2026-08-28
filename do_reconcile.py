import marimo

__generated_with = "0.24.0"
app = marimo.App()

with app.setup:
    # Initialization code that runs before all other cells
    import datetime
    import os
    import urllib.parse
    from collections.abc import Iterable, Sequence
    from pathlib import Path
    from typing import Any, Protocol, Self, TypeVar, cast

    import marimo as mo
    import pandas as pd

    # from tqdm.contrib.concurrent import thread_map
    from moutils.concurrent import thread_map
    from pandas.api.typing import NAType

    # from reconcile import latest_attempt_failed_after_previous_success_cached
    from reconcile import latest_attempt_failed_after_previous_success_db
    from sds_utils.scrubber import DAILY_INSTRUMENTS

    index_cols = "Instrument Level Descriptor".split()
    days_index_cols = "Instrument,Level,Descriptor,Date".split(",")
    repoints_index_cols = "Instrument,Level,Descriptor,Repoint".split(",")

    repoint0_date: datetime.date = datetime.date(2025, 9, 8)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Define paths and constants
    """)
    return


@app.cell
def _():
    # scrubber_base_dir = Path("output/july-august-only/no-combine-missing")
    # scrubber_base_dir = Path("output/scrubber/full/run_2026-08-26/no-combine-missing")
    scrubber_base_dir = Path("output/scrubber/full/run_2026-08-28_15-30")
    fpath_missing_days = scrubber_base_dir / "missing_days.csv"
    fpath_missing_repoints = scrubber_base_dir / "missing_repoints.csv"
    fpath_all_products = scrubber_base_dir / "all_products.csv"

    # dashboard_base_dir = Path("dashboard-output/july-only/run_mid-august")
    # fpath_dashboard_output = dashboard_base_dir / "imap-run-status-all-rows-20260820T172016Z.csv"
    # dashboard_base_dir = Path("dashboard-output/july-only/run_2026-08-28_07-50")
    # fpath_dashboard_output = (
    #     dashboard_base_dir / "imap-run-status-all-rows-20260828T115625Z.csv"
    # )
    fpath_dashboard_output = Path(
        "dashboard-output/august-only/aug_1-14/run_2026-08-28_15-10/imap-run-status-all-rows-20260828T192006Z.csv"
    )
    dashboard_base_dir = fpath_dashboard_output.parent

    # dashboard_start_date = pd.to_datetime(datetime.date(2026, 7, 1))
    # dashboard_end_date = pd.to_datetime(datetime.date(2026, 7, 31))
    # dashboard_start_repoint = 296
    # dashboard_end_repoint = 325

    dashboard_start_date = pd.to_datetime(datetime.date(2026, 8, 1))
    dashboard_end_date = pd.to_datetime(datetime.date(2026, 8, 14))
    dashboard_start_repoint = 327
    dashboard_end_repoint = 340
    return (
        dashboard_end_date,
        dashboard_end_repoint,
        dashboard_start_date,
        dashboard_start_repoint,
        fpath_all_products,
        fpath_dashboard_output,
        fpath_missing_days,
        fpath_missing_repoints,
    )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### defns
    """)
    return


@app.function
def extract_day(partition: str) -> pd.Timestamp:
    split = partition.split("_")
    if len(split) < 4:
        raise ValueError("TODO")
    return pd.to_datetime(split[-3][:10], format="%Y-%m-%d")


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
            df[col] = pd.to_datetime(
                df[col].astype("string"),
                format="%Y%m%d",
            )
    # drop anomalous single-day row that breaks uniqueness
    mask = (
        (df.Instrument == "lo") & (df.Descriptor == "good-times")  # NOT goodtimes
    )
    df = df[~mask]
    df["Descriptor"] = df.Descriptor.str.replace("-", "")
    return df


@app.function
def post_process_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns={"Data level": "Level"})
    df["Date"] = pd.to_datetime(
        df.Partition.str.split("_").str[-3].str[:10],
        format="%Y-%m-%d",
    )
    df["Type"] = df.Partition.map(extract_desc)
    df["Repoint"] = df.Partition.map(extract_repoint_number)
    df["Asset"] = df.Instrument + "_" + df.Level + "_" + df.Descriptor

    if "Partition link" not in df.columns:

        def make_url(_row: pd.Series) -> str:
            urllib.parse.quote(_row.Partition)
            params_str = urllib.parse.urlencode(
                dict(view="partitions", partition=_row.Partition)
            )
            _asset = urllib.parse.quote(_row.Asset)
            _base_url = os.environ["DAGSTER_BASE_URL"].rstrip("/")
            _url = f"{_base_url}/assets/{_asset}?{params_str}"
            return _url

        df["Partition link"] = df.apply(make_url, axis=1)

    return df


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Load csvs
    """)
    return


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
    dashboard_df = post_process_dashboard(pd.read_csv(fpath_dashboard_output))
    return all_products_df, dashboard_df, missing_days_df, missing_repoints_df


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### defns
    """)
    return


@app.function
def mark_scrubber_out_of_range(
    dashboard_df: pd.DataFrame, all_products_df: pd.DataFrame
) -> pd.DataFrame:
    """Return a marked copy of dashboard_df"""
    _apdf = all_products_df.copy()
    _ddf = dashboard_df.copy()

    _apdf = _apdf.set_index(index_cols)
    _ddf = _ddf.set_index(index_cols)

    _apdf = _apdf.sort_index()
    _ddf = _ddf.sort_index()

    _scrubber_ranges_df = _apdf[["MinStartDate", "MaxStartDate"]].assign(
        product_in_scrubber=True,
    )
    _ddf = _ddf.join(_scrubber_ranges_df, how="left", validate="many_to_one")
    _ddf["product_in_scrubber"] = _ddf["product_in_scrubber"].eq(True)

    _date_in_range = _ddf["Date"].ge(_ddf["MinStartDate"]) & _ddf["Date"].le(
        _ddf["MaxStartDate"]
    )
    _ddf["scrubber_out_of_range"] = ~(_ddf["product_in_scrubber"] & _date_in_range)

    # _ddf = _ddf.drop(columns=["MinStartDate", "MaxStartDate"])

    return _ddf.reset_index()


@app.function
def mark_product_in_scrubber(
    dashboard_df: pd.DataFrame,
    missing_days_df: pd.DataFrame,
    missing_repoints_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return a marked copy of a joined df."""
    _ddf = dashboard_df.copy()
    _mdf_days = missing_days_df
    _mdf_repoints = missing_repoints_df

    _has_repoint_mask = ~_ddf.Repoint.isna()
    _ddf_days: pd.DataFrame = _ddf[~_has_repoint_mask]
    _ddf_repoints: pd.DataFrame = _ddf[_has_repoint_mask]

    _jdf_days = (
        _ddf_days.set_index(days_index_cols)
        .assign(from_dashboard=True)
        .join(
            _mdf_days.set_index(days_index_cols).assign(from_scrubber=True),
            how="outer",
        )
    )
    _jdf_repoints = (
        _ddf_repoints.set_index(repoints_index_cols)
        .assign(from_dashboard=True)
        .join(
            _mdf_repoints.set_index(repoints_index_cols).assign(from_scrubber=True),
            how="outer",
        )
    )
    _jdf = pd.concat([_jdf_days.reset_index(), _jdf_repoints.reset_index()], axis=0)

    _jdf.from_scrubber = _jdf.from_scrubber.fillna(False).astype(bool)
    _jdf.from_dashboard = _jdf.from_dashboard.fillna(False).astype(bool)

    return _jdf


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

    _df = _ddf["Asset Partition".split()].dropna()
    _s_fwps = _df.apply(
        lambda row: latest_attempt_failed_after_previous_success_db(
            row.Asset, row.Partition
        ),
        axis=1,
    )

    return _ddf.assign(fwps=_s_fwps)


@app.cell
def _():
    class SupportsSubtraction(Protocol):
        def __sub__(self, other: Self) -> Any: ...

    T = TypeVar("T")

    def value_ranges[T: SupportsSubtraction](
        values: Iterable[T], *, unit: T, collapse: bool = False
    ) -> Sequence[tuple[T, T] | T]:
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
        if start is not None and end is not None:
            runs.append((start, end))

        if not collapse:
            return runs
        return [(start, end) if start != end else start for start, end in runs]

    def partition_ranges(
        values: Iterable[int], *, unit: int = 1, collapse: bool = False
    ) -> list[tuple[int, int] | int]:
        return value_ranges(values, unit=unit, collapse=collapse)  # type: ignore

    def date_ranges(
        dates: Iterable[pd.Timestamp], collapse: bool = False
    ) -> Sequence[tuple[pd.Timestamp, pd.Timestamp] | pd.Timestamp]:
        return value_ranges(dates, collapse=collapse, unit=datetime.timedelta(days=1))  # type: ignore

    return date_ranges, partition_ranges


@app.function
def format_date_ranges(
    dranges: Sequence[tuple[pd.Timestamp, pd.Timestamp] | pd.Timestamp],
) -> Sequence[tuple[str, str] | str]:
    def _format_dt(dt: pd.Timestamp) -> str:
        return dt.strftime("%Y-%m-%d")

    return [
        (
            _format_dt(drange)
            if isinstance(drange, pd.Timestamp)
            else cast(tuple[str, str], tuple(_format_dt(dt) for dt in drange))
        )
        for drange in dranges
    ]


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Filter dfs
    """)
    return


@app.cell
def _(
    all_products_df,
    dashboard_df,
    dashboard_end_date,
    dashboard_end_repoint,
    dashboard_start_date,
    dashboard_start_repoint,
    missing_days_df,
    missing_repoints_df,
):
    ddf_markup = dashboard_df.copy()

    ddf_markup = mark_scrubber_out_of_range(ddf_markup, all_products_df)
    ddf_markup = mark_product_in_scrubber(
        ddf_markup, missing_days_df, missing_repoints_df
    )
    for col, fillval in {
        "product_in_scrubber": True,
        "scrubber_out_of_range": False,
    }.items():
        if col not in ddf_markup.columns:
            continue
        ddf_markup[col] = ddf_markup[col].fillna(fillval).astype(bool)

    # drop products unknown to the scrubber
    ddf_markup = ddf_markup[~ddf_markup.scrubber_out_of_range]

    _df = ddf_markup
    _date_in_range_mask = (
        ((dashboard_start_date <= _df.Date) & (_df.Date <= dashboard_end_date))
        .fillna(False)
        .astype(bool)
    )
    _repoint_in_range_mask = (
        (
            (dashboard_start_repoint <= _df.Repoint)
            & (_df.Repoint <= dashboard_end_repoint)
        )
        .fillna(False)
        .astype(bool)
    )
    _in_range_mask = _date_in_range_mask | _repoint_in_range_mask

    # assert that the dashboard misses nothing from the scrubber
    _df = _df[_in_range_mask]
    _df = _df[~_df.from_dashboard]
    assert len(_df) == 0

    ddf_markup_full = ddf_markup

    # drop products out of the dashboard's date/repoint range
    ddf_markup = ddf_markup[_in_range_mask]

    # drop products in the scrubber
    ddf_markup = ddf_markup[~ddf_markup.from_scrubber]

    # drop materialized products
    ddf_markup = ddf_markup[ddf_markup.Status != "materialized"]

    cache_fwps(ddf_markup)

    ddf_markup = mark_fwps(ddf_markup)

    ddf_markup = ddf_markup[~ddf_markup.fwps]

    # ddf_markup

    # len(dashboard_df), len(ddf_markup)
    return (ddf_markup,)


@app.cell
def _():
    return


@app.cell
def _(date_ranges, ddf_markup):
    _df = ddf_markup
    _df = _df[~_df.Date.isna()]

    def _make_df(_df: pd.DataFrame) -> pd.Series:
        return pd.Series(
            dict(
                date_ranges_full=_df.Date.apply(
                    date_ranges, by_row=False, collapse=False
                ),
                first_link=_df["Partition link"].iloc[0],
            ),
        )

    _df = _df.groupby(index_cols).apply(_make_df)
    # _df = _df.sort_values("date_ranges_full")
    _df

    _gdf = _df
    mddf = ddf_markup.copy()
    mddf = mddf.set_index(index_cols)

    mddf["date_ranges_full"] = _gdf.date_ranges_full
    mddf["date_ranges"] = mddf.date_ranges_full.map(format_date_ranges)
    mddf = mddf.reset_index()
    mddf
    return (mddf,)


@app.cell
def _(dashboard_start_date, mddf):
    def _f_mask(date_ranges):
        if date_ranges[0][0] == dashboard_start_date:
            return True
        return False

    mddf_after = mddf[~mddf.date_ranges_full.map(_f_mask)]
    mddf_after
    return (mddf_after,)


@app.function
def group_by_date_ranges(ddf: pd.DataFrame) -> pd.DataFrame:
    def _f_make_df(df: pd.DataFrame):
        return pd.Series(
            dict(
                date_ranges=df.date_ranges.iloc[0],
                first_link=df["Partition link"].iloc[0],
            )
        )

    return ddf.groupby(index_cols).apply(_f_make_df)


@app.cell
def _(mddf):
    group_by_date_ranges(mddf)
    return


@app.cell
def _(mddf_after):
    group_by_date_ranges(mddf_after)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Sandbox
    """)
    return


@app.cell
def _(ddf_markup):
    _df = ddf_markup
    _df = _df[_df.Instrument == "mag"]
    _df = _df[_df.Level == "l1c"]
    _df = _df[_df.Descriptor == "normmago"]
    _df = _df[_df.Status == "failed"]
    _df
    return


@app.cell
def _(ddf_markup):
    _df = ddf_markup
    _df
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Old
    """)
    return


@app.cell
def _(ddf_markup):
    ddf_markup.scrubber_out_of_range.value_counts()
    return


@app.cell
def _(ddf_markup):
    _mddf = ddf_markup
    len(_mddf), _mddf.product_in_scrubber.sum(), _mddf.scrubber_out_of_range.sum()
    return


@app.cell
def _(all_products_df):

    _apdf = all_products_df
    _apdf
    return


@app.cell
def _(dashboard_df):
    ddf = dashboard_df
    ddf = ddf[ddf["Status"] != "materialized"]
    daily_instruments = set(DAILY_INSTRUMENTS)
    _df = ddf["Instrument,Level,Descriptor,Date,Repoint".split(",")]
    _df = _df[_df.Instrument.map(lambda instr: instr in daily_instruments)]
    _df = _df[_df.Repoint.isna()]
    _df = _df.drop(columns=["Repoint"])
    mddf_days = _df
    _df = ddf["Instrument,Level,Descriptor,Repoint".split(",")]
    _df = _df[~_df.Repoint.isna()]
    mddf_repoints = _df
    return ddf, mddf_days, mddf_repoints


@app.cell
def _(ddf):
    ddf
    return


@app.cell
def _(mddf_days, missing_days_df):
    ddf_1 = mddf_days
    _df = missing_days_df
    _jdf = (
        ddf_1.set_index(days_index_cols)
        .assign(from_dashboard=True)
        .join(_df.set_index(days_index_cols).assign(from_scrubber=True), how="outer")
    )
    _jdf = _jdf.fillna(False)
    joined_df_days = _jdf
    _jdf
    return (joined_df_days,)


@app.cell
def _(joined_df_days):
    _df = joined_df_days
    _df = _df.reset_index()
    _df = _df[_df.Date >= pd.Timestamp("2026-07-01")]
    _df = _df[_df.Date < pd.Timestamp("2026-08-01")]
    _df = _df[_df.from_dashboard != _df.from_scrubber]
    print(f"len(df)={len(_df)!r}")
    days_index_not_in_scrubber = _df.set_index(days_index_cols).index

    assert _df.from_scrubber.sum() == 0
    return (days_index_not_in_scrubber,)


@app.cell
def _(missing_days_df):
    _df = missing_days_df
    _df = _df[_df.Date >= pd.Timestamp("2026-07-01")]
    _df = _df[_df.Date < pd.Timestamp("2026-08-01")]
    print(f"len(df)={len(_df)!r}")
    _df
    return


@app.cell
def _(dashboard_df, days_index_not_in_scrubber):
    ddf_days_not_in_scrubber = (
        dashboard_df.set_index(days_index_cols)
        .loc[days_index_not_in_scrubber]
        .reset_index()
    )
    return (ddf_days_not_in_scrubber,)


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
    _grouped = ddf_2.groupby("Instrument,Level,Descriptor".split(","))

    def _make_df(_df: pd.DataFrame) -> pd.Series:
        return pd.Series(
            dict(
                date_ranges=_df.Date.apply(date_ranges, by_row=False, collapse=False),
                first_link=_df["Partition link"].iloc[0],
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
    _jdf = (
        ddf_3.set_index(repoints_index_cols)
        .assign(from_dashboard=True)
        .join(
            _df.set_index(repoints_index_cols).assign(from_scrubber=True), how="outer"
        )
    )
    _jdf = _jdf.fillna(False)
    joined_df_repoints = _jdf
    _jdf
    return (joined_df_repoints,)


@app.cell
def _(dashboard_df, joined_df_repoints):
    _df = joined_df_repoints
    _df = _df.reset_index()
    _min_repoint = dashboard_df.Repoint.min()
    _max_repoint = dashboard_df.Repoint.max()
    _df = _df[_df.Repoint >= _min_repoint]
    _df = _df[_df.Repoint <= _max_repoint]
    _df = _df[_df.from_dashboard != _df.from_scrubber]
    print(f"len(df)={len(_df)!r}")
    repoints_index_not_in_scrubber = _df.set_index(repoints_index_cols).index

    assert _df.from_scrubber.sum() == 0

    _df
    return (repoints_index_not_in_scrubber,)


@app.cell
def _(dashboard_df, repoints_index_not_in_scrubber):
    ddf_repoints_not_in_scrubber = (
        dashboard_df.set_index(repoints_index_cols)
        .loc[repoints_index_not_in_scrubber]
        .reset_index()
    )
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
    _grouped = ddf_4.groupby("Instrument,Level,Descriptor".split(","))

    def _make_repoints_df(_df: pd.DataFrame) -> pd.Series:
        return pd.Series(
            dict(
                repoint_ranges=_df.Repoint.apply(
                    partition_ranges, by_row=False, collapse=False
                ),
                first_link=_df["Partition link"].iloc[0],
            )
        )

    _df = _grouped.apply(_make_repoints_df)
    # _mask = _df.repoint_ranges.map(lambda r: r != full_repoint_range)
    _mask = _df.repoint_ranges.map(lambda r: len(r) > 1 or r[0][-1] != max_repoint)
    _df = _df[_mask]
    _df = _df.sort_values("repoint_ranges")

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
        pd.DataFrame.to_csv(_df, "temp.csv")    # _df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
