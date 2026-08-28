"""Load and parse instrument dependency graphs from sds-data-manager."""

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.request import urlopen

import yaml

DEPENDENCY_YAML_ROOT = (
    "https://raw.githubusercontent.com/IMAP-Science-Operations-Center/"
    "sds-data-manager/dev/sds_data_manager/orchestration/dependencies"
)
DEPENDENCY_GRAPH_INSTRUMENTS = (
    "codice",
    "glows",
    "hi",
    "hit",
    "idex",
    "lo",
    "mag",
    "spacecraft",
    "swapi",
    "swe",
    "ultra",
)


@dataclass(frozen=True, order=True)
class DependencyNode:
    """One dependency asset identified by its three display dimensions."""

    instrument: str
    level: str
    descriptor: str


@dataclass(frozen=True)
class DependencyGraph:
    """Unique dependency nodes and directed input-to-output edges."""

    nodes: tuple[DependencyNode, ...]
    edges: tuple[tuple[DependencyNode, DependencyNode], ...]


def dependency_yaml_url(instrument: str) -> str:
    """Return the raw dependency configuration URL for an instrument."""
    if instrument not in DEPENDENCY_GRAPH_INSTRUMENTS:
        raise ValueError(f"Unsupported dependency graph instrument: {instrument}")
    return f"{DEPENDENCY_YAML_ROOT}/imap_{instrument}_dependencies.yaml"


def load_dependency_graph(
    instrument: str, timeout_seconds: float = 15
) -> DependencyGraph:
    """Fetch and parse one instrument's dependency configuration."""
    url = dependency_yaml_url(instrument)
    with urlopen(url, timeout=timeout_seconds) as response:  # noqa: S310
        content = response.read().decode("utf-8")
    return parse_dependency_yaml(content)


def parse_dependency_yaml(content: str) -> DependencyGraph:
    """Parse dependency YAML into unique asset nodes and directed edges."""
    document = yaml.safe_load(content)
    if not isinstance(document, Mapping):
        raise ValueError("Dependency YAML must contain a mapping of processing blocks")

    nodes: set[DependencyNode] = set()
    edges: set[tuple[DependencyNode, DependencyNode]] = set()
    for block in document.values():
        if not isinstance(block, Mapping):
            continue
        inputs = _asset_nodes(block.get("inputs"))
        outputs = _asset_nodes(block.get("outputs"))
        nodes.update(inputs)
        nodes.update(outputs)
        edges.update(
            (input_node, output_node)
            for input_node in inputs
            for output_node in outputs
        )

    return DependencyGraph(
        nodes=tuple(sorted(nodes)),
        edges=tuple(sorted(edges)),
    )


def dependency_graph_mermaid(graph: DependencyGraph, instrument: str) -> str:
    """Render a dependency graph as a Mermaid flowchart definition."""
    node_ids = {node: f"n{index}" for index, node in enumerate(graph.nodes)}
    lines = ["flowchart LR"]
    for node, node_id in node_ids.items():
        label = " / ".join(
            _escape_mermaid_label(value)
            for value in (node.instrument, node.level, node.descriptor)
        )
        lines.append(f'    {node_id}["{label}"]')
    lines.extend(
        f"    {node_ids[source]} --> {node_ids[target]}"
        for source, target in graph.edges
    )
    selected = [node_ids[node] for node in graph.nodes if node.instrument == instrument]
    if selected:
        lines.append(
            "    classDef selected fill:#dbeafe,stroke:#2563eb,stroke-width:2px"
        )
        lines.append(f"    class {','.join(selected)} selected")
    return "\n".join(lines)


def _asset_nodes(value: object) -> tuple[DependencyNode, ...]:
    if not isinstance(value, list):
        return ()
    result = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        source = item.get("source")
        level = item.get("data_type")
        descriptor = item.get("descriptor")
        if (
            isinstance(source, str)
            and isinstance(level, str)
            and isinstance(descriptor, str)
        ):
            result.append(DependencyNode(source, level, descriptor))
    return tuple(result)


def _escape_mermaid_label(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
