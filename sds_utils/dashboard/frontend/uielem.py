from abc import ABC, abstractmethod
from typing import Self


class UIElem(ABC):
    @abstractmethod
    def render(self) -> None:
        """Create the UI element."""

    def build(self) -> Self:
        self.render()
        return self
