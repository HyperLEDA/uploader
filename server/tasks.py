import json
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel

import app.report_events as report_events


@dataclass
class TaskDefinition:
    id: str
    title: str
    description: str
    form_model: type[BaseModel]
    handler: Callable[[BaseModel, Callable[[report_events.ReportEvent], None]], None]
    group: str = "default"


TASKS: dict[str, TaskDefinition] = {}


def register_task(defn: TaskDefinition) -> None:
    TASKS[defn.id] = defn


@dataclass
class TaskRun:
    run_id: str
    events: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: threading.Event = field(default_factory=threading.Event)
    error: str | None = None

    def append(self, event: dict[str, Any]) -> None:
        with self.lock:
            self.events.append(event)

    def snapshot_from(self, start: int) -> tuple[list[dict[str, Any]], int]:
        with self.lock:
            return list(self.events[start:]), len(self.events)


RUNS: dict[str, TaskRun] = {}
RUNS_LOCK = threading.Lock()


def start_task(task_id: str, form_data: dict[str, Any]) -> str:
    defn = TASKS[task_id]
    form = defn.form_model.model_validate(form_data)
    run_id = str(uuid.uuid4())
    run = TaskRun(run_id=run_id)

    def append_report_event(event: report_events.ReportEvent) -> None:
        match event:
            case report_events.ReportLog(message=msg):
                run.append({"type": "log", "message": msg})
            case report_events.ReportProgress(percent=pct):
                run.append({"type": "progress", "percent": pct})
            case report_events.ReportDone(total_rows=n):
                run.append({"type": "done", "total_rows": n})
            case report_events.ReportError(message=msg):
                run.append({"type": "error", "message": msg})

    def report(event: report_events.ReportEvent) -> None:
        append_report_event(event)

    def worker() -> None:
        try:
            defn.handler(form, report)
        except Exception as e:
            append_report_event(report_events.ReportError(message=str(e)))
        finally:
            run.done.set()

    with RUNS_LOCK:
        RUNS[run_id] = run

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return run_id


def get_run(run_id: str) -> TaskRun | None:
    with RUNS_LOCK:
        return RUNS.get(run_id)


def sse_format(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event)}\n\n"
