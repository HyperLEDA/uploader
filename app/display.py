from collections.abc import Sequence

import click


def print_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str | int | float]],
    *,
    title: str = "",
    right_align_last_n: int = 2,
    percent_last_column: bool = True,
) -> None:
    ncols = len(headers)
    if ncols == 0:
        return

    def cell_str(val: str | int | float, col_index: int) -> str:
        if percent_last_column and col_index == ncols - 1:
            return f"{float(val):>5.1f}%"
        return str(val)

    def col_width(col_index: int) -> int:
        width = len(headers[col_index])
        for row in rows:
            width = max(width, len(cell_str(row[col_index], col_index)))
        if percent_last_column and col_index == ncols - 1:
            width = max(width, 6)
        return width

    widths = [col_width(i) for i in range(ncols)]
    right_align_from = ncols - right_align_last_n

    if title:
        click.echo(title)

    header_parts = []
    for i, h in enumerate(headers):
        w = widths[i]
        if i >= right_align_from:
            header_parts.append(f"{h:>{w}}")
        else:
            header_parts.append(f"{h:<{w}}")
    click.echo("  ".join(header_parts))
    click.echo("-" * (sum(widths) + 2 * (ncols - 1)))

    for row in rows:
        parts = []
        for i, val in enumerate(row):
            w = widths[i]
            s = cell_str(val, i)
            if i >= right_align_from:
                parts.append(f"{s:>{w}}")
            else:
                parts.append(f"{s:<{w}}")
        click.echo("  ".join(parts))
