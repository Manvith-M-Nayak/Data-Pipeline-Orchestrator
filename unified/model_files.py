"""
Change detection for on-disk model files.

The ML predictors cache their loaded models per process. Without a change
check, a retrained model (the Learning agent retrains the Performance model in
the background) was never used until the server restarted, and a failed load —
e.g. a file missing or half-written during a retrain — was cached forever.
Predictors compare files_signature() with the one they loaded and reload when
it differs. One os.stat per file per prediction.
"""

import os
from typing import Tuple


def files_signature(*paths: str) -> Tuple:
    sig = []
    for p in paths:
        try:
            st = os.stat(p)
            sig.append((p, st.st_mtime_ns, st.st_size))
        except FileNotFoundError:
            sig.append((p, None, None))
    return tuple(sig)
