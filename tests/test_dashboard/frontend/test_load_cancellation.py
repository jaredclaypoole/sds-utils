from unittest import TestCase
from unittest.mock import MagicMock

from sds_utils.dashboard.frontend.elems import AssetsStatusView


class LoadCancellationTests(TestCase):
    def test_cancel_discards_rows_and_cancels_active_task(self) -> None:
        view = object.__new__(AssetsStatusView)
        view._load_generation = 4
        view._load_task = MagicMock()
        view._load_task.done.return_value = False
        task = view._load_task
        view.table = MagicMock()
        view.loading_label = MagicMock()
        view.toolbar = MagicMock()
        view._update_summary = MagicMock()

        view._cancel_load()

        self.assertEqual(view._load_generation, 5)
        self.assertIsNone(view._load_task)
        task.cancel.assert_called_once_with()
        view.table.set_rows.assert_called_once_with([])
        view._update_summary.assert_called_once_with()
        view.loading_label.set_text.assert_called_once_with(
            "Load cancelled · 0 partitions loaded"
        )
        view.toolbar.set_loading.assert_called_once_with(False)
