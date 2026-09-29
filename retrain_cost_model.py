"""Bounded, reproducible cost model rebuild using feasible deadline-aware labels."""
import os
os.environ['OMP_NUM_THREADS'] = '2'
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'unified'))
from cost_optimization_agent.training.generate_cost_dataset import generate
from cost_optimization_agent.training.train_cost_model import train

if __name__ == '__main__':
    csv_path = 'unified/cost_optimization_agent/training/cost_training_v2.csv'
    generate(csv_path, 30000, 20260929)
    train(csv_path)
