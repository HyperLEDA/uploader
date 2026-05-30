import json
from contextvars import ContextVar, Token

import attrs

_action_description: ContextVar[str | None] = ContextVar("action_description", default=None)


def build(task_id: str, run_id: str, parameters: dict[str, object]) -> str:
    return json.dumps(
        {"task_id": task_id, "run_id": run_id, "parameters": parameters},
        sort_keys=True,
        separators=(",", ":"),
    )


def set_current(description: str) -> Token[str | None]:
    return _action_description.set(description)


def reset_current(token: Token[str | None]) -> None:
    _action_description.reset(token)


def current() -> str | None:
    return _action_description.get()


def apply[T](body: T) -> T:
    desc = current()
    if desc is None:
        return body
    return attrs.evolve(body, action_description=desc)
