import asyncio
import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from server.task_registry import register_all_tasks
from server.tasks import TASKS, get_run, start_task

register_all_tasks()

app = FastAPI(title="HyperLEDA Uploader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/tasks")
def list_tasks() -> list[dict[str, str]]:
    return [{"id": t.id, "title": t.title, "description": t.description, "group": t.group} for t in TASKS.values()]


@app.get("/api/tasks/{task_id}/schema")
def task_schema(task_id: str) -> dict[str, object]:
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Unknown task")
    task = TASKS[task_id]
    schema = task.form_model.model_json_schema()
    schema.pop("title", None)
    return {"title": task.title, "schema": schema}


@app.post("/api/tasks/{task_id}/submit")
def submit_task(task_id: str, body: dict) -> dict[str, str]:
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

    async def event_gen():
        idx = 0
        while True:
            chunk, idx = run.snapshot_from(idx)
            for ev in chunk:
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("type") in ("done", "error"):
                    return
            if run.done.is_set():
                tail, _ = run.snapshot_from(idx)
                for ev in tail:
                    yield f"data: {json.dumps(ev)}\n\n"
                return
            await asyncio.sleep(0.15)

    return StreamingResponse(event_gen(), media_type="text/event-stream")
