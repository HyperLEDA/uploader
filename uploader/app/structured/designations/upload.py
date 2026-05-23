from collections.abc import Callable

import matplotlib.pyplot as plt
from psycopg import sql

import uploader.app.report as report
from uploader.app import log
from uploader.app.display import format_table
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.storage import PgStorage
from uploader.app.structured.designations.rules import RULES, match
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)

CHART_FIGSIZE = (8, 6)


def _rule_distribution_bars(
    rule_counts: dict[str, int],
    unmatched: int,
) -> list[tuple[str, int]]:
    sorted_rules = sorted(rule_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    top = [(name, count) for name, count in sorted_rules[:10] if count > 0]
    other_total = sum(count for _, count in sorted_rules[10:])
    bars: list[tuple[str, int]] = list(top)
    if other_total > 0:
        bars.append(("(other rules)", other_total))
    if unmatched > 0:
        bars.append(("(unparsed)", unmatched))
    return bars


def _emit_rule_distribution_image(
    report_func: Callable[[report.Event], None],
    rule_counts: dict[str, int],
    unmatched: int,
    *,
    caption: str,
) -> None:
    bars = _rule_distribution_bars(rule_counts, unmatched)
    if not bars:
        return
    labels = [name for name, _ in bars]
    counts = [count for _, count in bars]
    fig, ax = plt.subplots(figsize=CHART_FIGSIZE)
    ax.barh(labels, counts)
    ax.invert_yaxis()
    ax.set_xlabel("Count")
    ax.set_title("Designation rule distribution")
    report_func(report.image_event_from_figure(fig, caption=caption))


def _report_batch_progress(
    report_func: Callable[[report.Event], None],
    *,
    rows_read: int,
    total_so_far: int,
    matched: int,
    unmatched: int,
    progress_pct: int,
    rule_counts: dict[str, int],
) -> None:
    report_func(report.ProgressEvent(percent=min(99, progress_pct)))
    report_func(
        report.LogEvent(
            message=(
                f"batch: rows_read={rows_read} cumulative_names={total_so_far} matched={matched} unmatched={unmatched}"
            ),
        ),
    )
    _emit_rule_distribution_image(
        report_func,
        rule_counts,
        unmatched,
        caption=f"{total_so_far} names processed",
    )


def _report_rule_distribution(
    report_func: Callable[[report.Event], None],
    rule_counts: dict[str, int],
    unmatched: int,
    total: int,
) -> None:
    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows = [
        (name, rule_counts[name], pct(rule_counts[name]))
        for name in sorted(rule_counts.keys(), key=lambda n: (-rule_counts[n], n))
        if rule_counts[name] > 0
    ]
    table_rows.append(("(no rule matched)", unmatched, pct(unmatched)))

    report_func(report.ProgressEvent(percent=100))

    _emit_rule_distribution_image(
        report_func,
        rule_counts,
        unmatched,
        caption=f"Final: {total} names",
    )

    summary = format_table(
        ("Rule", "Count", "%"),
        table_rows,
        title=f"Total names: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))


def upload_designations(
    storage: PgStorage,
    table_name: str,
    column_name: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    print_unmatched: bool = False,
    report_func: Callable[[report.Event], None],
) -> int:
    rule_counts: dict[str, int] = {r.name: 0 for r in RULES}
    unmatched = 0
    total_count = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0

    processed_rows = 0

    for rows in rawdata_batches(storage, table_name, [column_name], batch_size):
        batch_ids: list[str] = []
        batch_names: list[list[str]] = []

        for row in rows:
            internal_id = row["hyperleda_internal_id"]
            name_val = row[column_name]
            if name_val is None or (isinstance(name_val, str) and not name_val.strip()):
                unmatched += 1
                continue
            name_str = str(name_val).strip()
            match_result = match(name_str)
            if match_result is not None:
                transformed, rule_name = match_result
                rule_counts[rule_name] += 1
            else:
                unmatched += 1
                transformed = name_str
                if print_unmatched:
                    report_func(report.LogEvent(message=name_str))
            batch_ids.append(internal_id)
            batch_names.append([transformed])

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=SaveStructuredDataRequest(
                        catalog="designation",
                        columns=["design"],
                        ids=batch_ids,
                        data=batch_names,
                    ),
                )
            )

        processed_rows += len(rows)
        total_so_far = sum(rule_counts.values()) + unmatched

        def total_pct(n: int, t: int = total_so_far) -> float:
            return (100.0 * n / t) if t else 0.0

        log.logger.info(
            "processed batch",
            total=total_so_far,
            matched=sum(rule_counts.values()),
            matched_pct=round(total_pct(sum(rule_counts.values())), 1),
            unmatched=unmatched,
            unmatched_pct=round(total_pct(unmatched), 1),
        )
        progress_pct = int(100 * processed_rows / total_count) if total_count else 0
        _report_batch_progress(
            report_func,
            rows_read=len(rows),
            total_so_far=total_so_far,
            matched=sum(rule_counts.values()),
            unmatched=unmatched,
            progress_pct=progress_pct,
            rule_counts=rule_counts,
        )

    total = sum(rule_counts.values()) + unmatched
    _report_rule_distribution(report_func, rule_counts, unmatched, total)

    return total
