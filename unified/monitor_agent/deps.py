"""Shared service references, set once at startup by main.py."""

_adf     = None
_db      = None
_groq    = None
_monitor = None


def init(adf, db, groq, monitor):
    global _adf, _db, _groq, _monitor
    _adf, _db, _groq, _monitor = adf, db, groq, monitor


def get_adf():     return _adf
def get_db():      return _db
def get_groq():    return _groq
def get_monitor(): return _monitor
