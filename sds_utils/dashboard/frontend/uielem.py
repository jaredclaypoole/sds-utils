"""Base protocol for composable NiceGUI elements."""

from abc import ABC, abstractmethod
from typing import Self


class UIElem(ABC):
    """Base class for UI components that render and return themselves."""

    @abstractmethod
    def render(self) -> None:
        """Create the UI element."""

    def build(self) -> Self:
        """Render this component and return it for fluent construction."""
        self.render()
        return self
