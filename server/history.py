import json
import threading
from typing import Literal

from pydantic import BaseModel

import server.paths as paths

HistoryStatus = Literal["success", "error"]


class HistoryEntry(BaseModel):
    timestamp: str
    task_id: str
    task_title: str
    inputs: dict[str, object]
    status: HistoryStatus
    message: str


HISTORY_PATH = paths.DATA_DIR / "history.jsonl"
HISTORY_LOCK = threading.Lock()


def append_entry(entry: HistoryEntry) -> None:
    with HISTORY_LOCK:
        with HISTORY_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry.model_dump(), ensure_ascii=True))
            f.write("\n")


def load_history() -> list[HistoryEntry]:
    if not HISTORY_PATH.exists():
        return []
    items: list[HistoryEntry] = []
    with HISTORY_LOCK:
        with HISTORY_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                data = line.strip()
                if not data:
                    continue
                items.append(HistoryEntry.model_validate_json(data))
    items.reverse()
    return items
