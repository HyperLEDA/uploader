import json
import threading
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import BaseModel

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader import history
from uploader.app.log import logger


@dataclass
class TaskDefinition:
    id: str
    title: str
    description: str
    form_model: type[BaseModel]
    handler: Callable[[BaseModel, Callable[[report.Event], None]], None]
    group: str = "default"
    rerunnable: bool = True


TASKS: dict[str, TaskDefinition] = {}


def register_task(defn: TaskDefinition) -> None:
    TASKS[defn.id] = defn


@dataclass
class TaskRun:
    run_id: str
    events: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: threading.Event = field(default_factory=threading.Event)
    cancel_requested: threading.Event = field(default_factory=threading.Event)
    error: str | None = None

    def append(self, event: dict[str, Any]) -> None:
        with self.lock:
            self.events.append(event)

    def snapshot_from(self, start: int) -> tuple[list[dict[str, Any]], int]:
        with self.lock:
            return list(self.events[start:]), len(self.events)

    def request_cancel(self) -> None:
        self.cancel_requested.set()


class TaskCancelledError(Exception):
    pass


RUNS: dict[str, TaskRun] = {}
RUNS_LOCK = threading.Lock()


def _log_message_with_time(message: str) -> str:
    t = datetime.now().astimezone().strftime("%H:%M:%S")
    return f"[{t}] {message}"


def start_task(task_id: str, form_data: dict[str, Any]) -> str:
    defn = TASKS[task_id]
    form = defn.form_model.model_validate(form_data)
    run_id = str(uuid.uuid4())
    run = TaskRun(run_id=run_id)
    final_status: history.HistoryStatus | None = None
    final_message: str = ""

    def append_report_event(event: report.Event) -> None:
        nonlocal final_status, final_message
        match event:
            case report.LogEvent(message=msg):
                out = _log_message_with_time(msg)
                logger.info(
                    "log event",
                    task_id=task_id,
                    message=out,
                )
                run.append({"type": "log", "message": out})
            case report.ProgressEvent(percent=pct):
                logger.info(
                    "progress event",
                    task_id=task_id,
                    percent=pct,
                )
                run.append({"type": "progress", "percent": pct})
            case report.DoneEvent(message=msg):
                logger.info(
                    "finish event",
                    task_id=task_id,
                    message=msg,
                )
                final_status = "success"
                final_message = msg
                run.append({"type": "done", "message": msg})
            case report.ErrorEvent(message=msg):
                logger.error(
                    "error event",
                    task_id=task_id,
                    message=msg,
                )
                final_status = "error"
                final_message = msg
                run.append({"type": "error", "message": msg})
            case report.ImageEvent(data_url=url, caption=cap):
                run.append(
                    {
                        "type": "image",
                        "data_url": url,
                        "caption": cap,
                        "timestamp": datetime.now().astimezone().isoformat(),
                    },
                )

    def report_func(event: report.Event) -> None:
        if run.cancel_requested.is_set():
            raise TaskCancelledError()
        append_report_event(event)

    def worker() -> None:
        nonlocal final_status, final_message
        token = action_description.set_current(
            action_description.build(task_id, run_id, form.model_dump(mode="json")),
        )
        try:
            defn.handler(form, report_func)
        except TaskCancelledError:
            final_status = "cancelled"
            final_message = "Task was cancelled by user."
            run.append({"type": "cancelled", "message": final_message})
        except Exception as e:
            message = f"{e}\n\n{traceback.format_exc()}"
            append_report_event(report.ErrorEvent(message=message))
        finally:
            action_description.reset_current(token)
            run.done.set()
            if defn.rerunnable and final_status is not None:
                history.append_entry(
                    history.HistoryEntry(
                        timestamp=datetime.now().astimezone().isoformat(),
                        task_id=defn.id,
                        task_title=defn.title,
                        inputs=form_data,
                        status=final_status,
                        message=final_message,
                    ),
                )

    with RUNS_LOCK:
        RUNS[run_id] = run

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return run_id


def get_run(run_id: str) -> TaskRun | None:
    with RUNS_LOCK:
        return RUNS.get(run_id)


def cancel_run(run_id: str) -> bool:
    run = get_run(run_id)
    if run is None:
        return False
    if run.done.is_set():
        return True
    run.request_cancel()
    return True


def sse_format(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event)}\n\n"
