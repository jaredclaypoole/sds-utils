import os

from fastapi.responses import RedirectResponse
from nicegui import ui

from .backfills import BackfillDetailView, BackfillsView
from .db import create_db_and_tables
from .elems import AssetsStatusView, UIElem

ROOT_MODE = os.getenv("QUERY_APP_ROOT_MODE", "assets")


class LandingView(UIElem):
    def render(self) -> None:
        with ui.column().classes(
            "w-[95vw] max-w-5xl mx-auto min-h-screen justify-center p-6 gap-8"
        ):
            with ui.column().classes("gap-2"):
                ui.label("Dagster query tools").classes(
                    "text-4xl font-semibold text-slate-900"
                )
                ui.label("Choose a view to begin.").classes("text-lg text-slate-500")
            with ui.row().classes("w-full gap-5 items-stretch"):
                self._navigation_card(
                    title="Asset status",
                    description=(
                        "Inspect materialization and processing status by asset "
                        "partition."
                    ),
                    icon="table_view",
                    target="/assets",
                )
                self._navigation_card(
                    title="Backfills",
                    description=(
                        "Browse Dagster backfills, partition counts, and detailed runs."
                    ),
                    icon="dynamic_feed",
                    target="/backfills",
                )

    @staticmethod
    def _navigation_card(
        *, title: str, description: str, icon: str, target: str
    ) -> None:
        card = ui.card().classes(
            "w-96 max-w-full min-h-52 p-6 border-2 border-slate-200 shadow-none "
            "cursor-pointer hover:border-blue-400 hover:bg-blue-50 transition-colors"
        )
        card.props("role=link tabindex=0")
        card.on("click", lambda: ui.navigate.to(target))
        card.on(
            "keydown.enter",
            lambda: ui.navigate.to(target),
        )
        with card:
            ui.icon(icon).classes("text-4xl text-blue-700")
            ui.label(title).classes("text-2xl font-semibold text-slate-900")
            ui.label(description).classes("text-sm text-slate-600")


@ui.page("/")
def index() -> RedirectResponse:
    match ROOT_MODE:
        case "landing":
            url = "/landing"
        case "backfills" | "debug":
            url = "/backfills"
        case "assets" | _:
            url = "/assets"

    return RedirectResponse(url=url)


@ui.page("/landing")
def landing() -> None:
    LandingView().build()


@ui.page("/assets")
def assets() -> None:
    AssetsStatusView().build()


@ui.page("/backfills")
def backfills() -> None:
    BackfillsView().build()


@ui.page("/backfills/{backfill_id}")
def backfill_detail(backfill_id: str) -> None:
    BackfillDetailView(backfill_id).build()


if __name__ in {"__main__", "__mp_main__"}:
    create_db_and_tables()
    ui.run(
        title="Dagster asset status",
        favicon="📊",
        reload=False,
        uvicorn_logging_level="info",
    )
