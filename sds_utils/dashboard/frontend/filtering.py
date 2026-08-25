"""Search-expression matching for table column filters."""

import re
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True, slots=True)
class SearchTerm:
    pattern: str
    exclude: bool
    anchor_start: bool
    anchor_end: bool

    def matches(self, value: str) -> bool:
        regex = _wildcard_regex(self.pattern)
        if not self.anchor_start:
            regex = f".*{regex}"
        if not self.anchor_end:
            regex = f"{regex}.*"
        # ``.*`` should span the entire cell, including line breaks in metadata
        # fields such as skip reasons and missing-file details.
        matched = (
            re.fullmatch(regex, value, flags=re.IGNORECASE | re.DOTALL) is not None
        )
        return not matched if self.exclude else matched


@lru_cache(maxsize=256)
def parse_search_expression(expression: str) -> tuple[SearchTerm, ...]:
    """Parse whitespace-separated wildcard terms into an AND expression."""
    terms: list[SearchTerm] = []
    for raw_token in expression.split():
        token = raw_token
        exclude = token.startswith("-")
        if exclude:
            token = token[1:]

        anchor_start = token.startswith("^")
        if anchor_start:
            token = token[1:]

        anchor_end = token.endswith("$")
        if anchor_end:
            token = token[:-1]

        if token or (anchor_start and anchor_end):
            terms.append(
                SearchTerm(
                    pattern=token,
                    exclude=exclude,
                    anchor_start=anchor_start,
                    anchor_end=anchor_end,
                )
            )
    return tuple(terms)


def matches_search_expression(value: str, expression: str) -> bool:
    """Return whether a value satisfies every term in a search expression."""
    return all(term.matches(value) for term in parse_search_expression(expression))


def matches_tag_search_expression(tags: str, expression: str) -> bool:
    """Match each search term against individual semicolon-delimited tags.

    Every positive term must match at least one tag. An excluded term succeeds only
    when it matches none of the tags.
    """
    values = tuple(tag.strip() for tag in tags.split(";") if tag.strip())
    return all(
        (
            all(term.matches(value) for value in values)
            if term.exclude
            else any(term.matches(value) for value in values)
        )
        for term in parse_search_expression(expression)
    )


def is_valid_regex(expression: str) -> bool:
    """Return whether an expression is a valid Python regular expression."""
    try:
        _compile_regex(expression)
    except re.error:
        return False
    return True


def matches_regex(value: str, expression: str) -> bool:
    """Apply an unmodified, case-sensitive Python regular expression."""
    try:
        return _compile_regex(expression).search(value) is not None
    except re.error:
        return False


@lru_cache(maxsize=256)
def _compile_regex(expression: str) -> re.Pattern[str]:
    return re.compile(expression)


def _wildcard_regex(pattern: str) -> str:
    """Translate * and ** to unrestricted and non-underscore regex wildcards."""
    parts: list[str] = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**", index):
            parts.append("[^_]*")
            index += 2
        elif pattern[index] == "*":
            parts.append(".*")
            index += 1
        else:
            parts.append(re.escape(pattern[index]))
            index += 1
    return "".join(parts)
