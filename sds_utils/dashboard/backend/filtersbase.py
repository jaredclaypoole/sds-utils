import inspect
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic, Protocol, Self, TypeVar, cast, overload

import numpy as np
import pandas as pd
from pydantic import BaseModel

FilterArguments = dict[str, dict[str, Any]]


class FilterBase(Protocol):
    """Common interface exposed by registered filters."""

    name: str
    signature: inspect.Signature

    def apply(
        self,
        data_df: pd.DataFrame,
        **arguments: Any,
    ) -> pd.DataFrame:
        """Apply the filter using its filter-specific arguments."""


@dataclass(frozen=True)
class RegisteredFilter:
    """A bound filter function and its public argument signature."""

    name: str
    function: Callable[..., pd.DataFrame]
    signature: inspect.Signature

    def apply(
        self,
        data_df: pd.DataFrame,
        **arguments: Any,
    ) -> pd.DataFrame:
        """Invoke the filter function with its supplied arguments."""
        return self.function(data_df, **arguments)


RegisteredFilterT = TypeVar("RegisteredFilterT", bound=RegisteredFilter)


class FilterProperty(
    cached_property[RegisteredFilterT],
    Generic[RegisteredFilterT],
):
    """Descriptor that lazily creates a registered, instance-bound filter."""

    def __init__(
        self,
        function: Callable[..., pd.DataFrame],
        filter_factory: Callable[..., RegisteredFilterT],
        **filter_fields: Any,
    ) -> None:
        signature = inspect.signature(function)
        parameters = tuple(signature.parameters.values())
        if len(parameters) < 2:
            raise TypeError(
                f"Filter {function.__name__!r} must accept self and data_df"
            )

        self.filter_name = function.__name__
        self.filter_signature = signature.replace(parameters=parameters[2:])
        self.filter_factory = filter_factory
        self.filter_fields = filter_fields

        def register(instance: object) -> RegisteredFilterT:
            bound_function = function.__get__(instance, type(instance))
            return self.filter_factory(
                name=self.filter_name,
                function=bound_function,
                signature=self.filter_signature,
                **self.filter_fields,
            )

        super().__init__(register)

    def __set_name__(self, owner: type, name: str) -> None:
        """Use the declared attribute name as the registered filter name."""
        super().__set_name__(owner, name)
        self.filter_name = name


class StrHierarchySpec(BaseModel):
    hierarchy: dict[str, list[str]] | Callable[[Iterator[str]], dict[str, list[str]]]
    other: str | None = "Other"
    all: str | None = "All"

    def build_hierarchy(self, values: Iterable[str]) -> dict[str, list[str]]:
        if callable(self.hierarchy):
            return self.hierarchy(values)
        else:
            return self.hierarchy


@dataclass(frozen=True)
class StringRegisteredFilter(RegisteredFilter):
    """A registered string filter with optional hierarchy metadata."""

    hierarchy: StrHierarchySpec | None = None

    @classmethod
    def property(
        cls: type[Self],
        hierarchy: StrHierarchySpec | None = None,
    ) -> FilterProperty[Self]:
        """Declare a regex filter whose column matches its attribute name."""
        property_holder: dict[str, FilterProperty[Self]] = {}

        def filter_string_values(
            _filters: object,
            data_df: pd.DataFrame,
            included_values_regex: str | None = None,
            excluded_values_regex: str | None = None,
        ) -> pd.DataFrame:
            column = property_holder["property"].filter_name
            mask = np.ones(len(data_df), dtype=bool)
            if included_values_regex is not None:
                mask &= data_df[column].str.fullmatch(included_values_regex)
            if excluded_values_regex is not None:
                mask &= ~data_df[column].str.fullmatch(excluded_values_regex)
            return data_df[mask]

        property_ = FilterProperty(
            filter_string_values,
            cls,
            hierarchy=hierarchy,
        )
        property_holder["property"] = property_
        return property_


FilterFunction = Callable[..., pd.DataFrame]
FilterDecorator = Callable[[FilterFunction], FilterProperty[RegisteredFilter]]


@overload
def filter_property(
    _func: FilterFunction, /
) -> FilterProperty[RegisteredFilter]: ...


@overload
def filter_property(_func: None = None, /) -> FilterDecorator: ...


def filter_property(
    _func: FilterFunction | None = None,
    /,
) -> FilterDecorator | FilterProperty[RegisteredFilter]:
    """Declare a filter method that is automatically registered by `Filters`."""

    def deco(function: FilterFunction) -> FilterProperty[RegisteredFilter]:
        return FilterProperty(function, RegisteredFilter)

    if _func is not None:
        return deco(_func)
    else:
        return deco


class FiltersBase:
    def __iter__(self) -> Iterator[FilterBase]:
        """Iterate over filters in their class-definition order."""
        filter_names: dict[str, None] = {}
        for base in reversed(type(self).__mro__):
            for name, attribute in base.__dict__.items():
                if isinstance(attribute, FilterProperty):
                    filter_names[name] = None

        for name in filter_names:
            yield cast(FilterBase, getattr(self, name))

    def apply(
        self,
        data_df: pd.DataFrame,
        filter_arguments: FilterArguments | None,
    ) -> pd.DataFrame:
        """Apply requested filters in their class-definition order."""
        if filter_arguments is None:
            filter_arguments = {}
        registered = {filter_.name: filter_ for filter_ in self}
        unknown_filters = filter_arguments.keys() - registered.keys()
        if unknown_filters:
            names = ", ".join(sorted(unknown_filters))
            raise ValueError(f"Unknown filters: {names}")

        result = data_df
        for name, filter_ in registered.items():
            if name in filter_arguments:
                result = filter_.apply(result, **filter_arguments[name])
        return result
