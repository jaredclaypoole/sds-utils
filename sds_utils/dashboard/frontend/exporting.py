"""CSV and readable plain-text table exports."""

import csv
from io import StringIO
from textwrap import wrap


def csv_table(headers: list[str], rows: list[list[object]]) -> str:
    """Serialize tabular values as RFC-compatible CSV text."""
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(_stringify_row(row) for row in rows)
    return output.getvalue()


def plain_text_table(
    headers: list[str],
    rows: list[list[object]],
    *,
    maximum_column_width: int = 48,
) -> str:
    """Render an ASCII grid suitable for monospaced email or chat text."""
    string_rows = [_stringify_row(row) for row in rows]
    widths = [
        min(
            maximum_column_width,
            max(
                [len(header)]
                + [
                    max((len(line) for line in value.splitlines()), default=0)
                    for value in (row[index] for row in string_rows)
                ]
            ),
        )
        for index, header in enumerate(headers)
    ]
    border = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    output = [border, _render_line(headers, widths), border]
    for row in string_rows:
        cells = [
            _wrapped_lines(value, width)
            for value, width in zip(row, widths, strict=True)
        ]
        height = max((len(cell) for cell in cells), default=1)
        for line_index in range(height):
            output.append(
                _render_line(
                    [
                        cell[line_index] if line_index < len(cell) else ""
                        for cell in cells
                    ],
                    widths,
                )
            )
        output.append(border)
    return "\n".join(output) + "\n"


def _stringify_row(row: list[object]) -> list[str]:
    return ["" if value is None else str(value) for value in row]


def _wrapped_lines(value: str, width: int) -> list[str]:
    lines = value.splitlines() or [""]
    return [piece for line in lines for piece in (wrap(line, width) or [""])]


def _render_line(values: list[str], widths: list[int]) -> str:
    return (
        "| "
        + " | ".join(
            value.ljust(width) for value, width in zip(values, widths, strict=True)
        )
        + " |"
    )
