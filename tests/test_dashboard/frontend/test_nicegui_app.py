from unittest import TestCase

from sds_utils.dashboard.frontend.nicegui_app import index


class NiceGuiRoutesTests(TestCase):
    def test_root_redirects_to_landing_page(self) -> None:
        response = index()

        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], "/landing")
