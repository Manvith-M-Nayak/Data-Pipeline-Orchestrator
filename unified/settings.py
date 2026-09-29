"""
Single place to resolve configuration values.

Lookup order: environment (including `.env`) → legacy `config.py` → default.
Modules should read secrets through `get()` instead of `from config import ...`
so the app runs with only a `.env` file and no plaintext `config.py`.
"""

import os

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

try:
    import config as _cfg
except ImportError:
    _cfg = None


def get(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value:
        return value
    value = getattr(_cfg, name, None) if _cfg is not None else None
    return str(value) if value else default
