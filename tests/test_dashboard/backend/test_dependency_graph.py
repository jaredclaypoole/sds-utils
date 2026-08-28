from unittest import TestCase

from sds_utils.dashboard.backend.dependency_graph import (
    DependencyNode,
    dependency_graph_mermaid,
    dependency_yaml_url,
    parse_dependency_yaml,
)


class DependencyGraphTests(TestCase):
    def test_parses_nodes_and_input_to_output_edges(self) -> None:
        graph = parse_dependency_yaml(
            """
(l1b, science):
  inputs:
    - source: mag
      data_type: l1a
      descriptor: norm
    - source: spacecraft_clock
      data_type: spice
      descriptor: historical
  outputs:
    - source: mag
      data_type: l1b
      descriptor: science
"""
        )

        output = DependencyNode("mag", "l1b", "science")
        self.assertEqual(len(graph.nodes), 3)
        self.assertEqual(
            set(graph.edges),
            {
                (DependencyNode("mag", "l1a", "norm"), output),
                (DependencyNode("spacecraft_clock", "spice", "historical"), output),
            },
        )

    def test_mermaid_labels_contain_instrument_level_and_descriptor(self) -> None:
        graph = parse_dependency_yaml(
            """
job:
  inputs: [{source: mag, data_type: l1a, descriptor: norm}]
  outputs: [{source: mag, data_type: l1b, descriptor: science}]
"""
        )

        mermaid = dependency_graph_mermaid(graph, "mag")

        self.assertIn('"mag / l1a / norm"', mermaid)
        self.assertIn('"mag / l1b / science"', mermaid)
        self.assertIn("n0 --> n1", mermaid)

    def test_spacecraft_has_a_selectable_url(self) -> None:
        self.assertTrue(
            dependency_yaml_url("spacecraft").endswith(
                "/imap_spacecraft_dependencies.yaml"
            )
        )
