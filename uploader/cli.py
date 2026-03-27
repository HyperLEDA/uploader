import asyncio
import importlib.metadata
import json
import pathlib
from typing import Any

import click
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import ValidationError

from uploader.history import load_history
from uploader.task_registry import register_all_tasks
from uploader.tasks import TASKS, cancel_run, get_run, start_task

register_all_tasks()

app = FastAPI(title="HyperLEDA Uploader")
STATIC_DIR = pathlib.Path(__file__).parent / "static"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/tasks")
def list_tasks() -> list[dict[str, Any]]:
    return [
        {
            "id": t.id,
            "title": t.title,
            "description": t.description,
            "group": t.group,
            "rerunnable": t.rerunnable,
        }
        for t in TASKS.values()
    ]


@app.get("/api/history")
def list_history() -> list[dict[str, object]]:
    return [entry.model_dump() for entry in load_history()]


@app.get("/api/tasks/{task_id}/schema")
def task_schema(task_id: str) -> dict[str, object]:
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Unknown task")
    task = TASKS[task_id]
    schema = task.form_model.model_json_schema()
    schema.pop("title", None)
    return {"title": task.title, "schema": schema}


@app.post("/api/tasks/{task_id}/submit")
def submit_task(task_id: str, body: dict[str, object]) -> dict[str, str]:
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Unknown task")
    try:
        run_id = start_task(task_id, body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors()) from e
    return {"run_id": run_id}


@app.get("/api/runs/{run_id}/stream")
async def run_stream(run_id: str) -> StreamingResponse:
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run")

    async def event_gen() -> Any:
        idx = 0
        while True:
            chunk, idx = run.snapshot_from(idx)
            for ev in chunk:
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("type") in ("done", "error", "cancelled"):
                    return
            if run.done.is_set():
                tail, _ = run.snapshot_from(idx)
                for ev in tail:
                    yield f"data: {json.dumps(ev)}\n\n"
                return
            await asyncio.sleep(0.15)

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.post("/api/runs/{run_id}/cancel")
def cancel_task_run(run_id: str) -> dict[str, str]:
    ok = cancel_run(run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Unknown run")
    return {"status": "ok"}


if STATIC_DIR.is_dir():

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str) -> FileResponse:
        file_path = STATIC_DIR / full_path
        if file_path.is_file() and file_path.resolve().is_relative_to(STATIC_DIR.resolve()):
            return FileResponse(path=file_path)
        return FileResponse(path=STATIC_DIR / "index.html")


@click.group()
@click.version_option(version=importlib.metadata.version("uploader"))
def cli() -> None:
    return None


@cli.command("serve")
@click.option("--reload", is_flag=True, default=False)
@click.option("--port", type=int, default=8000)
def serve_command(reload: bool, port: int) -> None:
    uvicorn.run("uploader.cli:app", host="0.0.0.0", port=port, reload=reload)
