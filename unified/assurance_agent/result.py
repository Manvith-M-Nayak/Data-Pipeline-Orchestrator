"""
Result data structures + tiered presentation for the Assurance Agent.

Three render tiers, all derived from one AssuranceResult:
  - SUMMARY : one compact line (always shown)
  - DETAIL  : full per-check breakdown + semantic reasoning (on demand)
  - FAILURE : prominent, specific violations (shown when something fails)

This module is pure data + formatting — no validation logic, no model calls.
"""

from dataclasses import dataclass, field, asdict
from typing import List, Optional


# ── Per-check result (structural layer) ──────────────────────────────────────
@dataclass
class CheckResult:
    """Outcome of a single deterministic structural check."""
    check: str                 # machine name, e.g. "column_references"
    label: str                 # human label, e.g. "Column references"
    passed: bool
    message: str               # specific violation on fail; confirmation on pass
    tier: str = "structure"    # presentation grouping: structure | schema

    def to_dict(self) -> dict:
        return asdict(self)


# ── Semantic layer result (probabilistic) ────────────────────────────────────
@dataclass
class SemanticResult:
    flagged: bool                       # True  = model thinks plan != request
    reasoning: str                      # model's explanation
    model: str = ""                     # which base model judged
    available: bool = True              # False = Ollama/model was unreachable
    # specific findings: [{"stage": ..., "problem": ..., "suggestion": ...}]
    issues: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ── Combined result ──────────────────────────────────────────────────────────
@dataclass
class AssuranceResult:
    overall_status: str                              # "pass" | "fail"
    structural_results: List[CheckResult] = field(default_factory=list)
    semantic_result: Optional[SemanticResult] = None
    # whether a semantic flag is allowed to drive overall_status to "fail"
    semantic_blocks_overall: bool = False

    # ── tier helpers ─────────────────────────────────────────────────────────
    def _glyph(self, ok: bool) -> str:
        return "✓" if ok else "✗"   # ✓ / ✗

    def _structure_ok(self) -> bool:
        return all(c.passed for c in self.structural_results if c.tier == "structure")

    def _schema_ok(self) -> bool:
        return all(c.passed for c in self.structural_results if c.tier == "schema")

    def _intent_ok(self) -> bool:
        if not self.semantic_result:
            return True
        if not self.semantic_result.available:
            return True  # advisory layer unavailable -> not counted against the plan
        return not self.semantic_result.flagged

    def summary_line(self) -> str:
        """SUMMARY tier — one compact line, always shown."""
        struct = self._glyph(self._structure_ok())
        schema = self._glyph(self._schema_ok())
        if self.semantic_result is None or not self.semantic_result.available:
            intent = "—"     # — skipped or unavailable, not actually judged
        else:
            intent = self._glyph(self._intent_ok())
        verb = "passed" if self.overall_status == "pass" else "FAILED"
        return f"Assurance: {verb} (structure {struct}, schema {schema}, intent {intent})"

    def detail_text(self) -> str:
        """DETAIL tier — full per-check breakdown + semantic reasoning."""
        lines = [self.summary_line(), "", "Structural checks:"]
        for c in self.structural_results:
            lines.append(f"  {self._glyph(c.passed)} {c.label}: {c.message}")
        lines.append("")
        if self.semantic_result:
            s = self.semantic_result
            if not s.available:
                lines.append(f"Semantic check ({s.model or 'base model'}): UNAVAILABLE")
                lines.append(f"  {s.reasoning}")
            else:
                state = "FLAGGED — possible mismatch" if s.flagged else "no mismatch"
                lines.append(f"Semantic check ({s.model}): {state}")
                lines.append(f"  reasoning: {s.reasoning}")
                for it in s.issues:
                    lines.append(f"  - [{it.get('stage', 'plan')}] {it.get('problem', '')}"
                                 + (f" → fix: {it['suggestion']}" if it.get("suggestion") else ""))
        else:
            lines.append("Semantic check: skipped")
        return "\n".join(lines)

    def failure_text(self) -> Optional[str]:
        """FAILURE tier — prominent, specific. None when nothing failed."""
        failures = [c for c in self.structural_results if not c.passed]
        intent_flag = (
            self.semantic_result
            and self.semantic_result.available
            and self.semantic_result.flagged
        )
        if not failures and not intent_flag:
            return None

        lines = ["ASSURANCE FAILURES", "=================="]
        for c in failures:
            lines.append(f"[{c.label}] FAILED")
            lines.append(f"   why: {c.message}")
        if intent_flag:
            tag = "BLOCKING" if self.semantic_blocks_overall else "ADVISORY (human may override)"
            lines.append(f"[Intent match] FLAGGED — {tag}")
            lines.append(f"   why: {self.semantic_result.reasoning}")
            for it in self.semantic_result.issues:
                lines.append(f"   - [{it.get('stage', 'plan')}] {it.get('problem', '')}"
                             + (f" → fix: {it['suggestion']}" if it.get("suggestion") else ""))
        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Full serializable result — what an API / consumer renders from."""
        return {
            "overall_status":     self.overall_status,
            "summary":            self.summary_line(),
            "structural_results": [c.to_dict() for c in self.structural_results],
            "semantic_result":    self.semantic_result.to_dict() if self.semantic_result else None,
            "tiers": {
                "summary": self.summary_line(),
                "detail":  self.detail_text(),
                "failure": self.failure_text(),
            },
        }
