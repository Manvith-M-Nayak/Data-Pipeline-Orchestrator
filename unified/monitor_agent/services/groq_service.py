import os
import json
from typing import Dict, List, Any

from groq import AsyncGroq

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

_ANALYSIS_SCHEMA = """{
  "status_summary": "<one sentence>",
  "anomalies": ["<anomaly>", ...],
  "root_cause": "<root cause or empty string>",
  "performance_insights": ["<insight>", ...],
  "suggestions": ["<suggestion>", ...],
  "severity": "low|medium|high"
}"""

_PREDICTION_SCHEMA = """{
  "predicted_duration_sec": <number>,
  "confidence": "low|medium|high",
  "range_min_sec": <number>,
  "range_max_sec": <number>,
  "reasoning": "<one sentence>"
}"""

_ANOMALY_SCHEMA = """{
  "is_anomaly": true|false,
  "verdict": "<one sentence explanation>"
}"""


class GroqService:
    def __init__(self):
        self._client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY", ""))

    async def _chat(self, system: str, user: str, temperature: float = 0.2) -> str:
        resp = await self._client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", GROQ_MODEL),
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            temperature=temperature,
            max_tokens=1024,
        )
        return resp.choices[0].message.content.strip()

    def _parse_json(self, text: str, fallback: Dict) -> Dict:
        start, end = text.find("{"), text.rfind("}") + 1
        if start == -1 or end == 0:
            return fallback
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            return fallback

    async def analyze_pipeline_run(
        self, run: Dict[str, Any], activity_runs: List[Dict[str, Any]], historical_stats: Dict
    ) -> Dict:
        activities = [
            {
                "name":       a.get("activityName"),
                "type":       a.get("activityType"),
                "status":     a.get("status"),
                "duration_s": round((a.get("durationMs") or 0) / 1000, 1),
                "error":      (a.get("error") or {}).get("message", ""),
            }
            for a in activity_runs
        ]
        user_msg = (
            f"Pipeline: {run.get('pipelineName')}\n"
            f"Status: {run.get('status')}\n"
            f"Duration: {round((run.get('durationMs') or 0) / 1000, 1)}s\n"
            f"Historical avg: {round(historical_stats.get('avg', 0), 1)}s  "
            f"p95: {round(historical_stats.get('p95', 0), 1)}s  "
            f"count: {historical_stats.get('count', 0)}\n"
            f"Activities: {json.dumps(activities, indent=2)}\n"
            f"Message: {run.get('message', '')}\n\n"
            f"Return ONLY this JSON:\n{_ANALYSIS_SCHEMA}"
        )
        text = await self._chat(
            "You are an Azure Data Factory expert. Return structured JSON only.", user_msg
        )
        return self._parse_json(text, {
            "status_summary": f"Pipeline {run.get('status', 'unknown')}.",
            "anomalies": [], "root_cause": run.get("message", ""),
            "performance_insights": [], "suggestions": [],
            "severity": "low" if run.get("status") == "Succeeded" else "high",
        })

    async def predict_runtime(self, pipeline_name: str, historical_runs: List[Dict]) -> Dict:
        if not historical_runs:
            return {
                "predicted_duration_sec": 0, "confidence": "low",
                "range_min_sec": 0, "range_max_sec": 0,
                "reasoning": "No historical data available.",
            }
        summary = [
            {"status": r.get("status"), "duration_s": round((r.get("duration_ms") or 0) / 1000, 1)}
            for r in historical_runs
        ]
        text = await self._chat(
            "You are an ADF performance analyst. Return structured JSON only.",
            f"Pipeline: {pipeline_name}\nHistory: {json.dumps(summary)}\n\nReturn ONLY:\n{_PREDICTION_SCHEMA}",
            temperature=0.1,
        )
        durations = [r.get("duration_ms", 0) / 1000 for r in historical_runs if r.get("duration_ms")]
        avg = sum(durations) / len(durations) if durations else 0
        return self._parse_json(text, {
            "predicted_duration_sec": round(avg, 1), "confidence": "low",
            "range_min_sec": round(avg * 0.8, 1), "range_max_sec": round(avg * 1.2, 1),
            "reasoning": "Fallback to historical average.",
        })

    async def explain_duration(
        self, pipeline_name: str, run: Dict, activity_runs: List[Dict]
    ) -> str:
        slowest = sorted(activity_runs, key=lambda a: a.get("durationMs") or 0, reverse=True)[:3]
        lines = "\n".join(
            f"- {a.get('activityName')} ({a.get('activityType')}): "
            f"{round((a.get('durationMs') or 0) / 1000, 1)}s — {a.get('status')}"
            for a in slowest
        )
        return await self._chat(
            "You are an ADF analyst. Be concise. No headers.",
            f"Pipeline '{pipeline_name}' ran {round((run.get('durationMs') or 0)/1000,1)}s "
            f"and {run.get('status')}.\nTop activities:\n{lines}\n\n"
            "Give a one-paragraph explanation of why it took this long.",
            temperature=0.3,
        )

    async def detect_anomaly(
        self, pipeline_name: str, elapsed_sec: float, historical_stats: Dict
    ) -> Dict:
        if historical_stats.get("count", 0) < 3:
            return {"is_anomaly": False, "verdict": "Insufficient history."}
        text = await self._chat(
            "You are an ADF anomaly detector. Return structured JSON only.",
            f"Pipeline: {pipeline_name}\n"
            f"Elapsed: {round(elapsed_sec,1)}s  avg: {round(historical_stats.get('avg',0),1)}s  "
            f"p95: {round(historical_stats.get('p95',0),1)}s  count: {historical_stats.get('count',0)}\n\n"
            f"Return ONLY:\n{_ANOMALY_SCHEMA}",
            temperature=0.1,
        )
        return self._parse_json(text, {"is_anomaly": False, "verdict": "Analysis unavailable."})
