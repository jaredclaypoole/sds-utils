"""Simple username login and profile creation UI."""

from urllib.parse import urlencode

from nicegui import ui
from nicegui.elements.dialog import Dialog
from nicegui.elements.input import Input
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from ..backend.db.models import UserProfile
from .uielem import UIElem


def dashboard_url(username: str) -> str:
    """Return the filtered-table URL for a username."""
    return f"/filteredtableview?{urlencode({'username': username})}"


def ensure_user_profile(engine: Engine, username: str) -> None:
    """Create a user profile unless it already exists."""
    with Session(engine) as session:
        existing = session.exec(
            select(UserProfile).where(UserProfile.username == username)
        ).first()
        if existing is not None:
            return
        try:
            session.add(UserProfile(username=username))
            session.commit()
        except IntegrityError:
            # Another session may have created the same username after the lookup.
            session.rollback()


class LoginView(UIElem):
    """Authenticate locally by selecting or creating a username."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def render(self) -> None:
        """Render the username field and login action."""
        with ui.card().classes("absolute-center w-96 p-6 gap-4"):
            ui.label("IMAP Processing Status Dashboard").classes("text-h5")
            self.username_input: Input = ui.input(label="Username").classes("w-full")
            ui.button("Login", on_click=self._login).classes("self-end")

    def _login(self) -> None:
        username = (self.username_input.value or "").strip()
        if not username:
            ui.notify("Enter a username", type="warning")
            return
        if self._user_exists(username):
            self._open_dashboard(username)
            return
        self._confirm_creation(username)

    def _user_exists(self, username: str) -> bool:
        with Session(self.engine) as session:
            profile = session.exec(
                select(UserProfile).where(UserProfile.username == username)
            ).first()
        return profile is not None

    def _confirm_creation(self, username: str) -> None:
        with ui.dialog() as dialog, ui.card().classes("w-96"):
            ui.label(f"Username {username!r} does not exist.").classes("text-body1")
            ui.label("Would you like to create it?")
            with ui.row().classes("w-full justify-end"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                ui.button(
                    "Create",
                    on_click=lambda: self._create_user(username, dialog),
                )
        dialog.open()

    def _create_user(self, username: str, dialog: Dialog) -> None:
        ensure_user_profile(self.engine, username)
        dialog.close()
        self._open_dashboard(username)

    @staticmethod
    def _open_dashboard(username: str) -> None:
        ui.navigate.to(dashboard_url(username))
