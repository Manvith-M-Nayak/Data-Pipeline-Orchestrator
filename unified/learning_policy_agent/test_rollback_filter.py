"""Rollback review must ignore runs that never succeeded.

Replays the 2026-10-03 cost review from docs/PAPER_RESULTS.md §B5.5:
four real post-change errors (13%, 45%, 8%, 8%) and one aborted run
whose error is 904%. Counting the abort makes MAPE 195.6% and rolls
the factor back. Leaving it out leaves MAPE 18.5%.
"""
import os
import tempfile
import unittest

from learning_policy_agent.feedback_collector import FeedbackCollector
from learning_policy_agent.policy_engine import PolicyEngine
from learning_policy_agent.safety import SafetyManager


def _cost(ape, success, actual=1.0):
    return {
        "timestamp": 200,
        "success": success,
        "actual_cost_usd": actual,
        "estimated_cost_usd": actual * (1.0 + ape),
    }


def _policies(min_runs):
    return {
        "cost_correction_factor": 0.7902,
        "rollback_review_min_runs": min_runs,
        "rollback_tolerance": 0.05,
        "_pending_reviews": {
            "cost_correction_factor": {
                "changed_at": 100,
                "old_value": 0.8644,
                "new_value": 0.7902,
                "pre_change_mape": 0.656,
            }
        },
    }


class RollbackFilterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = PolicyEngine(
            policy_path=os.path.join(self.tmp.name, "policies.json"),
            log_path=os.path.join(self.tmp.name, "learning_log.jsonl"),
            safety=SafetyManager(os.path.join(self.tmp.name, "versions")),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _good_cost_rows(self):
        return [
            _cost(0.13, True),
            _cost(0.45, True),
            _cost(0.08, True),
            _cost(0.08, True),
        ]

    def _abort_cost_row(self, success):
        # |0.01004 - 0.001| / 0.001 = 9.04, the 904% abort in §B5.5.
        return _cost(9.04, success, actual=0.001)

    def test_counting_the_abort_rolls_back_at_195_percent(self):
        # success True stands in for the old filter, which did not look at success.
        policies = _policies(min_runs=5)
        events = self.engine._review_pending_changes(
            self._good_cost_rows() + [self._abort_cost_row(True)], policies
        )
        self.assertEqual(events[0]["action"], "rolled_back")
        self.assertIn("195.6%", events[0]["reason"])
        self.assertEqual(policies["cost_correction_factor"], 0.8644)

    def test_excluding_the_abort_confirms_at_18_percent(self):
        # Four successful runs are the whole of the real evidence. The
        # historical review used them once the abort was removed, so the
        # minimum here is 4 — the number of runs that actually executed.
        policies = _policies(min_runs=4)
        events = self.engine._review_pending_changes(
            self._good_cost_rows() + [self._abort_cost_row(False)], policies
        )
        self.assertEqual(events[0]["action"], "confirmed")
        self.assertIn("18.5%", events[0]["reason"])
        self.assertIn("4 runs", events[0]["reason"])
        self.assertEqual(policies["cost_correction_factor"], 0.7902)

    def test_default_minimum_waits_instead_of_rolling_back(self):
        policies = _policies(min_runs=5)
        events = self.engine._review_pending_changes(
            self._good_cost_rows() + [self._abort_cost_row(False)], policies
        )
        self.assertEqual(events, [])
        self.assertEqual(policies["cost_correction_factor"], 0.7902)
        self.assertIn("cost_correction_factor", policies["_pending_reviews"])

    def test_duration_review_drops_near_zero_abort(self):
        good = [
            {
                "timestamp": 200,
                "success": True,
                "prediction_source": "ml_model",
                "actual_duration_s": 100.0,
                "predicted_duration_s": 110.0,
            }
            for _ in range(5)
        ]
        abort = {
            "timestamp": 200,
            "success": False,
            "prediction_source": "ml_model",
            "actual_duration_s": 0.1,
            "predicted_duration_s": 200.0,
        }
        policies = {
            "duration_correction_factor": 0.9689,
            "rollback_review_min_runs": 5,
            "rollback_tolerance": 0.05,
            "_pending_reviews": {
                "duration_correction_factor": {
                    "changed_at": 100,
                    "old_value": 1.0,
                    "new_value": 0.9689,
                    "pre_change_mape": 0.266,
                }
            },
        }
        events = self.engine._review_pending_changes(good + [abort], policies)
        self.assertEqual(events[0]["action"], "confirmed")
        self.assertIn("10.0%", events[0]["reason"])
        self.assertIn("5 runs", events[0]["reason"])
        self.assertEqual(policies["duration_correction_factor"], 0.9689)

    def test_unknown_outcome_stays_in_the_review(self):
        rows = self._good_cost_rows()
        rows.append(_cost(0.08, None))
        policies = _policies(min_runs=5)
        events = self.engine._review_pending_changes(rows, policies)
        self.assertEqual(events[0]["action"], "confirmed")
        self.assertIn("5 runs", events[0]["reason"])


class NormalizeCostFallbackTest(unittest.TestCase):
    def test_aborted_run_does_not_borrow_the_manager_estimate(self):
        out = FeedbackCollector.normalize({
            "final_status": "aborted",
            "cost_estimate_usd": 0.0105,
            "actual_cost_usd": 0.001,
        })
        self.assertFalse(out["success"])
        self.assertIsNone(out["estimated_cost_usd"])

    def test_completed_run_still_falls_back_for_older_records(self):
        out = FeedbackCollector.normalize({
            "final_status": "completed",
            "cost_estimate_usd": 0.02,
            "actual_cost_usd": 0.018,
        })
        self.assertTrue(out["success"])
        self.assertEqual(out["estimated_cost_usd"], 0.02)

    def test_cost_agent_estimate_wins_when_both_are_present(self):
        out = FeedbackCollector.normalize({
            "final_status": "completed",
            "estimated_cost_usd": 0.04,
            "cost_estimate_usd": 0.01,
        })
        self.assertEqual(out["estimated_cost_usd"], 0.04)


if __name__ == "__main__":
    unittest.main()
