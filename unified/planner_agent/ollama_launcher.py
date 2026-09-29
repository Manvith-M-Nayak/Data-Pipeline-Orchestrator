"""
Start the local Ollama server with the backend.

When PLANNER_BACKEND is "ollama" and OLLAMA_HOST points at this machine,
`ensure_ollama()` starts `ollama serve` if nothing is answering, waits for it,
checks the planner model exists, and loads it into memory so the first plan
request doesn't pay the model-load delay.

Ollama is started in its own session so it survives uvicorn --reload restarts
and Ctrl-C of the backend (like a normal daemon). Set OLLAMA_AUTOSTART=0 to
disable. Server output goes to data/ollama.log.
"""

import asyncio
import os
import shutil
import subprocess
from urllib.parse import urlparse

import requests

import settings

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
_LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ollama.log"
)


def _host() -> str:
    return settings.get("OLLAMA_HOST", "http://localhost:11434").rstrip("/")


def _model() -> str:
    return settings.get("PLANNER_MODEL", "planner-agent")


def _is_up(host: str) -> bool:
    try:
        return requests.get(f"{host}/api/tags", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def _has_model(host: str, model: str) -> bool:
    names = {m.get("name", "") for m in requests.get(f"{host}/api/tags", timeout=5).json().get("models", [])}
    return model in names or f"{model}:latest" in names


def _start_server(host: str) -> None:
    os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
    env = dict(os.environ)
    p = urlparse(host)
    env.setdefault("OLLAMA_HOST", f"{p.hostname}:{p.port or 11434}")
    with open(_LOG_PATH, "ab") as log:
        subprocess.Popen(
            ["ollama", "serve"],
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            env=env, start_new_session=True,
        )


async def ensure_ollama(timeout: float = 30.0) -> None:
    if settings.get("PLANNER_BACKEND", "ollama").lower() != "ollama":
        return
    if settings.get("OLLAMA_AUTOSTART", "1").lower() in ("0", "false", "no"):
        return
    host = _host()
    if urlparse(host).hostname not in _LOCAL_HOSTS:
        return  # remote Ollama — not ours to start

    if not await asyncio.to_thread(_is_up, host):
        if shutil.which("ollama") is None:
            print("[ollama] not installed — planner will use the default-config fallback")
            return
        print(f"[ollama] starting `ollama serve` (log: {_LOG_PATH})")
        await asyncio.to_thread(_start_server, host)
        deadline = asyncio.get_running_loop().time() + timeout
        while not await asyncio.to_thread(_is_up, host):
            if asyncio.get_running_loop().time() > deadline:
                print(f"[ollama] did not come up within {timeout:.0f}s — see {_LOG_PATH}")
                return
            await asyncio.sleep(1)
        print("[ollama] server is up")

    model = _model()
    if not await asyncio.to_thread(_has_model, host, model):
        print(f"[ollama] model '{model}' not found — create it with `ollama create {model} -f <Modelfile>`")
        return
    # An empty generate request just loads the model into memory.
    await asyncio.to_thread(
        requests.post, f"{host}/api/generate",
        json={"model": model, "keep_alive": "30m"}, timeout=300,
    )
    print(f"[ollama] model '{model}' loaded")
