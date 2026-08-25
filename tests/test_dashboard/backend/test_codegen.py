import os
from unittest import TestCase
from unittest.mock import patch

from sds_utils.dashboard.codegen import main, normalize_dagster_base_url


class CodegenConfigurationTests(TestCase):
    def test_removes_trailing_slashes_from_base_url(self) -> None:
        with patch.dict(
            os.environ,
            {"DAGSTER_BASE_URL": "https://dagster.example///"},
        ):
            normalize_dagster_base_url()

            self.assertEqual(
                os.environ["DAGSTER_BASE_URL"], "https://dagster.example"
            )

    def test_preserves_base_url_without_trailing_slash(self) -> None:
        with patch.dict(
            os.environ,
            {"DAGSTER_BASE_URL": "https://dagster.example"},
        ):
            normalize_dagster_base_url()

            self.assertEqual(
                os.environ["DAGSTER_BASE_URL"], "https://dagster.example"
            )

    @patch("sds_utils.dashboard.codegen.ariadne_codegen_main")
    def test_entry_point_normalizes_url_before_running_codegen(
        self, ariadne_codegen_main
    ) -> None:
        with patch.dict(
            os.environ,
            {"DAGSTER_BASE_URL": "https://dagster.example/"},
        ):
            main()

            ariadne_codegen_main.assert_called_once_with()
            self.assertEqual(
                os.environ["DAGSTER_BASE_URL"], "https://dagster.example"
            )
