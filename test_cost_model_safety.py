"""Offline regression tests for cost optimization safety and accounting."""
import os
os.environ['OMP_NUM_THREADS'] = '1'
import sys, copy, unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).parent / 'unified'))
from cost_optimization_agent import CostOptimizationAgent, CostMLPredictor
from cost_optimization_agent.cost_optimizer import CostBreakdown, OptimizationSuggestion
from cost_optimization_agent.training.generate_cost_dataset import _deadline_aware_optimal
from resource_agent.ml.feature_spec import stage_features

class SafetyTests(unittest.TestCase):
    def setUp(self):
        self.agent = CostOptimizationAgent()
        self.stage = {'name':'transform', 'type':'notebook', 'transformations':['x = x + 1']}
        self.plan = {'stages':[self.stage], 'schema':{'row_count':1000000,'columns':['x']}, 'csv_size_bytes':140000000}
        self.rp = {'allocations':[{'stage_name':'transform','stage_type':'notebook','workers':4,'diu':0,'node_type':'Standard_D8s_v3','memory_gb':16,'duration_s':150,'shuffle_partitions':200}], 'estimated_total_s':150,'peak_concurrent_workers':4}
        self.perf = {'predicted_total_s':150}
        self.opt = {'workers':1,'diu':0,'node_type':'Standard_DS4_v2','memory_gb':28,'shuffle_partitions':8,'source':'ml_model'}

    def test_model_loads_and_predicts(self):
        self.assertTrue(CostMLPredictor.is_available(), CostMLPredictor._load_error)
        prediction = CostMLPredictor.predict_optimal_config(self.stage,self.plan['schema'],140000000,deadline_s=300)
        self.assertEqual(prediction['source'], 'ml_model')

    def test_real_model_tight_deadline_is_not_applied(self):
        constraints = {'deadline_s':151,'priority':'critical'}
        result = self.agent.apply_optimization(self.plan,self.perf,self.rp,constraints)
        self.assertEqual(result,self.rp)
        self.assertEqual(self.agent.optimize(self.plan,self.perf,self.rp,constraints)['recommendations'],[])

    def test_valid_candidate_recalculates_runtime_and_saves(self):
        original = copy.deepcopy(self.rp)
        with patch.object(CostMLPredictor,'predict_optimal_config',return_value=self.opt):
            result = self.agent.apply_optimization(self.plan,self.perf,self.rp,{'deadline_s':300})
        self.assertEqual(result['allocations'][0]['workers'],1)
        self.assertGreater(result['estimated_total_s'],150)
        self.assertLessEqual(result['estimated_total_s'],300)
        self.assertLess(self.agent._estimate_cost(self.plan,self.perf,result).total_usd,self.agent._estimate_cost(self.plan,self.perf,self.rp).total_usd)
        self.assertEqual(original,self.rp)

    def test_no_deadline_does_not_allow_slowdown(self):
        with patch.object(CostMLPredictor,'predict_optimal_config',return_value=self.opt):
            self.assertEqual(self.agent.apply_optimization(self.plan,self.perf,self.rp),self.rp)

    def test_missing_workload_keeps_plan(self):
        self.plan['schema'] = {}
        with patch.object(CostMLPredictor,'predict_optimal_config',return_value=self.opt):
            self.assertEqual(self.agent.apply_optimization(self.plan,self.perf,self.rp,{'deadline_s':300}),self.rp)

    def test_memory_requirement_preserved(self):
        self.rp['allocations'][0]['memory_gb'] = 64
        with patch.object(CostMLPredictor,'predict_optimal_config',return_value=self.opt):
            self.assertEqual(self.agent.apply_optimization(self.plan,self.perf,self.rp,{'deadline_s':300}),self.rp)

    def test_missing_model_fails_closed(self):
        with patch.object(CostMLPredictor,'is_available',return_value=False):
            self.assertEqual(self.agent.apply_optimization(self.plan,self.perf,self.rp),self.rp)
            self.assertEqual(self.agent.optimize(self.plan,self.perf,self.rp)['recommendations'],[])

    def test_empty_plan(self):
        result = self.agent.optimize({}, {}, {})
        self.assertEqual(result['recommendations'],[])
        self.assertEqual(result['estimated_cost']['total_usd'],0)

    def test_rejected_suggestion_stays_rejected(self):
        suggestion = OptimizationSuggestion('reduce cluster','~50%','slower','test',CostBreakdown(1,1,0,0,2),'low',source='rule')
        self.assertEqual(self.agent._enforce_constraints([suggestion],{'deadline_s':151},self.perf),[])

    def test_node_price_changes_cost(self):
        cheaper = copy.deepcopy(self.rp)
        cheaper['allocations'][0]['node_type'] = 'Standard_DS2_v2'
        self.assertLess(self.agent._estimate_cost(self.plan,self.perf,cheaper).compute_usd,self.agent._estimate_cost(self.plan,self.perf,self.rp).compute_usd)

    def test_copy_cost_tracks_diu_without_notebook_charges(self):
        rp = {'allocations':[{'stage_type':'copy','diu':1,'duration_s':300}]}
        first = self.agent._estimate_cost({},self.perf,rp)
        rp['allocations'][0]['diu'] = 8
        second = self.agent._estimate_cost({},self.perf,rp)
        self.assertEqual(first.compute_usd + first.databricks_dbu_usd,0)
        self.assertGreater(second.adf_usd,first.adf_usd)

    def test_infeasible_labels_are_explicit(self):
        feat = stage_features(self.stage,self.plan['schema'],140000000)
        self.assertIsNone(_deadline_aware_optimal(feat,60))
        self.assertIsNotNone(_deadline_aware_optimal(feat,300))

    def test_copy_deadline_changes_diu(self):
        feat = stage_features({'type':'copy'},{'row_count':1000000},1000*1024*1024)
        self.assertEqual(_deadline_aware_optimal(feat,60)['opt_diu'],7)
        self.assertEqual(_deadline_aware_optimal(feat,600)['opt_diu'],1)
        self.assertIsNone(_deadline_aware_optimal(feat,1))

if __name__ == '__main__':
    unittest.main(verbosity=2)
