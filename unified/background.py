"""
Fire-and-forget task helper.

``asyncio.create_task`` returns a task that the event loop only holds a *weak*
reference to — if nothing else keeps a reference, the task can be garbage
collected mid-flight and any exception it raises vanishes silently.

``spawn`` keeps a strong reference until the task finishes and logs any
exception via a done-callback, so background failures are no longer swallowed.
"""

from __future__ import annotations

import asyncio
import logging

_log = logging.getLogger("background")

# Strong references to in-flight tasks; discarded when each completes.
_tasks: "set[asyncio.Task]" = set()


def _on_done(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _log.error("background task %r failed: %r", task.get_name(), exc, exc_info=exc)


def spawn(coro, name: str | None = None) -> asyncio.Task:
    """Schedule ``coro`` on the running loop, retaining a reference and logging
    any exception it raises."""
    task = asyncio.ensure_future(coro)
    if name:
        task.set_name(name)
    _tasks.add(task)
    task.add_done_callback(_on_done)
    return task
