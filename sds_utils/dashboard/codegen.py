"""Project entry point for generating the Dagster GraphQL client."""

import os

from ariadne_codegen.main import main as ariadne_codegen_main


def normalize_dagster_base_url() -> None:
    """Remove trailing slashes before Ariadne expands its schema URL setting."""
    base_url = os.getenv("DAGSTER_BASE_URL")
    if base_url:
        os.environ["DAGSTER_BASE_URL"] = base_url.rstrip("/")


def main() -> None:
    """Run Ariadne Codegen with a normalized Dagster base URL."""
    normalize_dagster_base_url()
    ariadne_codegen_main()
