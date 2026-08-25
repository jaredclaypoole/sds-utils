"""Resolve Dagster API endpoints and authentication settings."""

import os


def graphql_url() -> str:
    """Return DAGSTER_GRAPHQL_URL, or derive it from DAGSTER_BASE_URL."""
    if url := os.getenv("DAGSTER_GRAPHQL_URL"):
        return url
    return f"{os.environ['DAGSTER_BASE_URL'].rstrip('/')}/graphql"


def dagster_ui_url() -> str:
    """Return the Dagster UI base URL associated with the GraphQL endpoint."""
    if url := os.getenv("DAGSTER_BASE_URL"):
        return url.rstrip("/")
    return graphql_url().removesuffix("/graphql").rstrip("/")


def client_headers() -> dict[str, str] | None:
    """Build the authentication header used by Dagster Cloud, when configured."""
    if token := os.getenv("DAGSTER_API_TOKEN"):
        return {"Dagster-Cloud-Api-Token": token}
    return None
