# sds-utils

## scrubber

Setup:  `poetry install`

Run: `poetry run python -m sds_utils.scrubber`

## dashboard asset cache

The asset-status dashboard persists Dagster results in its application database and
renders covered date ranges from the cache before checking Dagster for updates.
Partially covered requests query only the uncovered date intervals and merge them with
the cached portion.

Set `QUERY_DAGSTER_CACHE_NAMESPACE` to a stable deployment name such as
`imap-production`. Cache identity does not depend on the Dagster URL: endpoint changes
are recorded as namespace metadata and retain the existing cached data. If it is not
set, the namespace defaults to `QUERY_APP_DB_SUFFIX`, then to `default` when neither
variable has a value.

Asset definitions are refreshed every five minutes by default. Override this with
`QUERY_DAGSTER_DEFINITION_CACHE_SECONDS`.

An authoritative reconciliation runs hourly by default to detect changes that are not
visible in the incremental run-update feed. Override this with
`QUERY_DAGSTER_CACHE_RECONCILE_SECONDS`.
