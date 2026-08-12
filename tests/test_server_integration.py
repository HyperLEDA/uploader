import json
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

import uploader.app.report as report
import uploader.history as history
import uploader.tasks as tasks
from uploader.app.lib.formula import ExpressionStr
from uploader.cli import app


class FakeTaskForm(BaseModel):
    name: str


@pytest.fixture
def isolated_task_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Iterator[None]:
    original_tasks = tasks.TASKS.copy()
    original_runs = tasks.RUNS.copy()
    monkeypatch.setattr(history, "HISTORY_PATH", tmp_path / "history.jsonl")
    tasks.TASKS.clear()
    tasks.TASKS.update(original_tasks)
    tasks.RUNS.clear()
    yield
    tasks.TASKS.clear()
    tasks.TASKS.update(original_tasks)
    tasks.RUNS.clear()
    tasks.RUNS.update(original_runs)


def test_task_integration_flow(isolated_task_state: None) -> None:
    def fake_handler(form: FakeTaskForm, emit: Callable[[report.Event], None]) -> None:
        emit(report.LogEvent(message=f"Start for {form.name}"))
        emit(report.ProgressEvent(percent=50))
        emit(report.DoneEvent(message=f"Completed {form.name}"))

    fake_task = tasks.TaskDefinition(
        id="fake-task",
        title="Fake Task",
        description="Task used for integration testing.",
        form_model=FakeTaskForm,
        handler=fake_handler,
        group="Tests",
    )
    tasks.register_task(fake_task)

    client = TestClient(app)

    list_response = client.get("/api/tasks")
    assert list_response.status_code == 200
    task_ids = {item["id"] for item in list_response.json()}
    assert "fake-task" in task_ids

    schema_response = client.get("/api/tasks/fake-task/schema")
    assert schema_response.status_code == 200
    assert schema_response.json()["title"] == "Fake Task"
    assert schema_response.json()["description"] == "Task used for integration testing."

    submit_response = client.post("/api/tasks/fake-task/submit", json={"name": "alpha"})
    assert submit_response.status_code == 200
    run_id = submit_response.json()["run_id"]

    events: list[dict[str, Any]] = []
    with client.stream("GET", f"/api/runs/{run_id}/stream") as stream_response:
        assert stream_response.status_code == 200
        for line in stream_response.iter_lines():
            if not line or not line.startswith("data: "):
                continue
            event = json.loads(line.removeprefix("data: "))
            events.append(event)
            if event.get("type") == "done":
                break

    event_types = [event["type"] for event in events]
    assert "log" in event_types
    assert "progress" in event_types
    assert "done" in event_types
    assert any(event.get("message") == "Completed alpha" for event in events)

    deadline = time.time() + 2.0
    history_data: list[dict[str, Any]] = []
    while time.time() < deadline:
        history_response = client.get("/api/history")
        assert history_response.status_code == 200
        history_data = history_response.json()
        if any(item["task_id"] == "fake-task" for item in history_data):
            break
        time.sleep(0.05)

    fake_entry = next(item for item in history_data if item["task_id"] == "fake-task")
    assert fake_entry["task_title"] == "Fake Task"
    assert fake_entry["inputs"] == {"name": "alpha"}
    assert fake_entry["status"] == "success"
    assert fake_entry["message"] == "Completed alpha"


def test_task_cancel_flow(isolated_task_state: None) -> None:
    def cancellable_handler(form: FakeTaskForm, emit: Callable[[report.Event], None]) -> None:
        for idx in range(20):
            emit(report.LogEvent(message=f"Step {idx} for {form.name}"))
            emit(report.ProgressEvent(percent=(idx + 1) * 5))
            time.sleep(0.05)
        emit(report.DoneEvent(message=f"Completed {form.name}"))

    fake_task = tasks.TaskDefinition(
        id="fake-task-cancel",
        title="Fake Task Cancel",
        description="Task used for cancellation integration testing.",
        form_model=FakeTaskForm,
        handler=cancellable_handler,
        group="Tests",
    )
    tasks.register_task(fake_task)

    client = TestClient(app)
    submit_response = client.post("/api/tasks/fake-task-cancel/submit", json={"name": "beta"})
    assert submit_response.status_code == 200
    run_id = submit_response.json()["run_id"]

    cancel_response = client.post(f"/api/runs/{run_id}/cancel")
    assert cancel_response.status_code == 200

    events: list[dict[str, Any]] = []
    with client.stream("GET", f"/api/runs/{run_id}/stream") as stream_response:
        assert stream_response.status_code == 200
        for line in stream_response.iter_lines():
            if not line or not line.startswith("data: "):
                continue
            event = json.loads(line.removeprefix("data: "))
            events.append(event)
            if event.get("type") == "cancelled":
                break

    assert any(event.get("type") == "cancelled" for event in events)

    deadline = time.time() + 2.0
    history_data: list[dict[str, Any]] = []
    while time.time() < deadline:
        history_response = client.get("/api/history")
        assert history_response.status_code == 200
        history_data = history_response.json()
        if any(item["task_id"] == "fake-task-cancel" for item in history_data):
            break
        time.sleep(0.05)

    fake_entry = next(item for item in history_data if item["task_id"] == "fake-task-cancel")
    assert fake_entry["status"] == "cancelled"
    assert fake_entry["message"] == "Task was cancelled by user."


def test_validate_task_form_endpoint(isolated_task_state: None) -> None:
    class FakeExpressionForm(BaseModel):
        name: str
        expression: ExpressionStr

    def noop_handler(form: FakeExpressionForm, emit: Callable[[report.Event], None]) -> None:
        emit(report.DoneEvent(message=f"Completed {form.name}"))

    tasks.register_task(
        tasks.TaskDefinition(
            id="fake-expression-task",
            title="Fake Expression Task",
            description="Task used for form validation testing.",
            form_model=FakeExpressionForm,
            handler=noop_handler,
            group="Tests",
        ),
    )

    client = TestClient(app)

    unknown = client.post("/api/tasks/does-not-exist/validate", json={})
    assert unknown.status_code == 404

    ok = client.post(
        "/api/tasks/fake-expression-task/validate",
        json={"expression": 'to_deg(col("RAJ2000"))'},
    )
    assert ok.status_code == 200
    assert ok.json() == []

    bad = client.post(
        "/api/tasks/fake-expression-task/validate",
        json={"expression": 'col("a)'},
    )
    assert bad.status_code == 200
    errors = bad.json()
    assert len(errors) == 1
    assert errors[0]["path"] == ["expression"]
    assert "string" in errors[0]["message"].lower()
    assert errors[0]["start_line"] == 1
    assert errors[0]["start_column"] >= 1
    assert "end_line" in errors[0]
    assert "end_column" in errors[0]

    submit_bad = client.post(
        "/api/tasks/fake-expression-task/submit",
        json={"name": "alpha", "expression": 'col("a)'},
    )
    assert submit_bad.status_code == 422
    detail = submit_bad.json()["detail"]
    assert any(item.get("type") == "expression" for item in detail)
