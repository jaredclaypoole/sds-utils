"""Derive logical job keys from Dagster job names or selected assets."""

import logging
import re
from functools import lru_cache
from typing import NamedTuple

import httpx
import yaml

logger = logging.getLogger(__name__)

_DEPENDENCIES_URL = (
    "https://raw.githubusercontent.com/IMAP-Science-Operations-Center/"
    "sds-data-manager/dev/sds_data_manager/orchestration/dependencies/"
    "imap_{instrument}_dependencies.yaml"
)
_JOB_NAME_PATTERN = re.compile(
    r"^(?P<instrument>[^_]+)_(?P<data_level>[^_]+)_"
    r"(?P<descriptor>[^_]+)(?:_.+)?$"
)
_YAML_JOB_PATTERN = re.compile(r"^\(([^,]+),\s*([^)]+)\)$")
_INSTRUMENT_PATTERN = re.compile(r"^[a-z][a-z0-9-]*$")
_ASSET_COMPONENT_COUNT = 3


class JobKeyParts(NamedTuple):
    """The inferred logical job identity and its component fields."""

    job_key: str | None
    instrument: str | None
    data_level: str | None
    descriptor: str | None


def _asset_parts(path: list[str]) -> tuple[str, str] | None:
    if len(path) != 1:
        return None
    parts = path[0].split("_", _ASSET_COMPONENT_COUNT - 1)
    if len(parts) != _ASSET_COMPONENT_COUNT or not all(parts):
        return None
    return parts[0], parts[1]


@lru_cache(maxsize=32)
def _job_outputs_for_instrument(
    instrument: str,
) -> dict[frozenset[str], tuple[str, str] | None]:
    """Load and cache output sets from the current dev dependency YAML."""
    if not _INSTRUMENT_PATTERN.fullmatch(instrument):
        return {}
    url = _DEPENDENCIES_URL.format(instrument=instrument)
    try:
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        dependencies = yaml.safe_load(response.text)
    except (httpx.HTTPError, yaml.YAMLError) as error:
        logger.warning("Could not load job definitions for %s: %s", instrument, error)
        return {}

    output_sets: dict[frozenset[str], tuple[str, str] | None] = {}
    if not isinstance(dependencies, dict):
        logger.warning("Unexpected job definitions for %s at %s", instrument, url)
        return output_sets
    for job_name, spec in dependencies.items():
        match = _YAML_JOB_PATTERN.fullmatch(str(job_name))
        if match is None or not isinstance(spec, dict):
            continue
        outputs = spec.get("outputs")
        if not isinstance(outputs, list):
            continue
        names = frozenset(
            f"{output['source']}_{output['data_type']}_{output['descriptor']}".replace(
                "-", ""
            )
            for output in outputs
            if isinstance(output, dict)
            and all(
                isinstance(output.get(field), str)
                for field in ("source", "data_type", "descriptor")
            )
        )
        if not names:
            continue
        identity = (match.group(1).strip(), match.group(2).strip().replace("-", ""))
        if names in output_sets and output_sets[names] != identity:
            output_sets[names] = None
        else:
            output_sets[names] = identity
    return output_sets


def derive_job_key(
    job_name: str | None, selected_assets: list[list[str]]
) -> JobKeyParts:
    """Prefer a named job, then exact YAML outputs, then asset-name inference."""
    if job_name and job_name != "__ASSET_JOB":
        match = _JOB_NAME_PATTERN.fullmatch(job_name)
        if match is not None:
            instrument, data_level, descriptor = match.groups()
            return JobKeyParts(
                f"{instrument}_{data_level}_{descriptor}",
                instrument,
                data_level,
                descriptor,
            )

    return _derive_from_assets(selected_assets)


def _derive_from_assets(selected_assets: list[list[str]]) -> JobKeyParts:
    if not selected_assets:
        return JobKeyParts(None, None, None, None)
    parsed_assets = [_asset_parts(path) for path in selected_assets]
    if any(parts is None for parts in parsed_assets):
        return JobKeyParts(None, None, None, None)
    asset_parts = [parts for parts in parsed_assets if parts is not None]
    instruments = {parts[0] for parts in asset_parts}
    if len(instruments) != 1:
        return JobKeyParts(None, None, None, None)
    instrument = instruments.pop()

    selected_names = frozenset(path[0] for path in selected_assets)
    job_identity = _job_outputs_for_instrument(instrument).get(selected_names)
    if job_identity is not None:
        data_level, descriptor = job_identity
        return JobKeyParts(
            f"{instrument}_{data_level}_{descriptor}",
            instrument,
            data_level,
            descriptor,
        )

    levels = [level for _, level in asset_parts if level != "ancillary"]
    if not levels:
        return JobKeyParts(None, instrument, None, None)
    data_level = min(levels)
    return JobKeyParts(f"{instrument}_{data_level}", instrument, data_level, None)
