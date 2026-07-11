"""
Learning and Policy Update Agent
================================

The system's memory and improvement loop. Watches the real outcomes of every
run (Monitor, Assurance, Performance Prediction, Cost Optimization), measures
how wrong each agent was, and:

  1. Adjusts policies/defaults gradually when consistent patterns appear.
  2. Triggers retraining of the Performance Prediction model when error
     crosses a threshold — deploying the new model only if it beats the old
     one on held-out metrics.
  3. Flags persistent problems for human review.
  4. Logs every change (traceable + reversible).

Runs in the background between runs — never during live execution.
"""

from .learning_agent import LearningPolicyAgent, get_learning_agent
from .feedback_collector import FeedbackCollector
from .error_analyzer import ErrorAnalyzer
from .policy_engine import PolicyEngine
from .retraining_manager import RetrainingManager
from .safety import SafetyManager

__all__ = [
    "LearningPolicyAgent",
    "get_learning_agent",
    "FeedbackCollector",
    "ErrorAnalyzer",
    "PolicyEngine",
    "RetrainingManager",
    "SafetyManager",
]