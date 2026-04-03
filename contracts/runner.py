
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ValidationRunner - Executes contract checks with enforcement modes.
Phase 2A of the Data Contract Enforcer.
"""

import json
import yaml
import argparse
import hashlib
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import uuid
import re
import sys


class ValidationRunner:
    def __init__(self, contract_path: str, data_path: str, mode: str = 'AUDIT'):
        self.contract_path = Path(contract_path)
        self.data_path = Path(data_path)
        self.mode = mode.upper()
        self.contract = None
        self.df = None
        self.baselines = {}
        self.results = []
        self.contract_id = None
        self.blocking_violations = False
    
    def load_contract(self) -> Dict:
        with open(self.contract_path, 'r', encoding='utf-8') as f:
            self.contract = yaml.safe_load(f)
        self.contract_id = self.contract.get('id', 'unknown')
        return self.contract
    
    def load_data(self) -> pd.DataFrame:
        records = []
        with open(self.data_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        if not records:
            raise ValueError(f"No records found in {self.data_path}")
        
        if 'extracted_facts' in records[0]:
            rows = []
            for r in records:
                base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
                for fact in r.get('extracted_facts', []):
                    row = base.copy()
                    for k, v in fact.items():
                        row[f'fact_{k}'] = v
                    rows.append(row)
            self.df = pd.DataFrame(rows)
        else:
            self.df = pd.DataFrame(records)
        
        return self.df
    
    def load_baselines(self):
        baseline_path = Path('schema_snapshots/baselines.json')
        if baseline_path.exists():
            with open(baseline_path, 'r') as f:
                data = json.load(f)
                self.baselines = data.get('columns', {})
    
    def save_baseline(self):
        stats = {}
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            series = self.df[col].dropna()
            if len(series) > 0:
                stats[col] = {'mean': float(series.mean()), 'stddev': float(series.std())}
        
        baseline_path = Path('schema_snapshots')
        baseline_path.mkdir(parents=True, exist_ok=True)
        with open(baseline_path / 'baselines.json', 'w') as f:
            json.dump({'written_at': datetime.utcnow().isoformat(), 'columns': stats}, f, indent=2)
    
    def add_result(self, check_id, column_name, check_type, status, actual_value, expected, severity, records_failing=0, message=''):
        self.results.append({
            'check_id': check_id, 'column_name': column_name, 'check_type': check_type,
            'status': status, 'actual_value': actual_value, 'expected': expected,
            'severity': severity, 'records_failing': records_failing, 'message': message
        })
        
        # Check if this violation should block based on mode
        if status == 'FAIL' and self.mode == 'ENFORCE':
            if severity in ['CRITICAL', 'HIGH']:
                self.blocking_violations = True
    
    def check_range(self):
        schema = self.contract.get('schema', {})
        if 'extracted_facts' in schema:
            items = schema['extracted_facts'].get('items', {})
            if 'confidence' in items and 'fact_confidence' in self.df.columns:
                conf_schema = items['confidence']
                actual_min = self.df['fact_confidence'].min()
                actual_max = self.df['fact_confidence'].max()
                min_val = conf_schema.get('minimum', -float('inf'))
                max_val = conf_schema.get('maximum', float('inf'))
                
                if actual_min < min_val or actual_max > max_val:
                    failing_mask = (self.df['fact_confidence'] < min_val) | (self.df['fact_confidence'] > max_val)
                    failing_count = failing_mask.sum()
                    self.add_result(
                        f"{self.contract_id}.confidence.range", 'extracted_facts.confidence', 'range',
                        'FAIL', f'min={actual_min:.3f}, max={actual_max:.3f}',
                        f'min>={min_val}, max<={max_val}', 'CRITICAL', int(failing_count),
                        f'Confidence outside range! Found max={actual_max:.3f}'
                    )
                else:
                    self.add_result(
                        f"{self.contract_id}.confidence.range", 'extracted_facts.confidence', 'range',
                        'PASS', f'min={actual_min:.3f}, max={actual_max:.3f}',
                        f'min>={min_val}, max<={max_val}', 'INFO', 0, 'Confidence within range'
                    )
    
    def check_statistical_drift(self):
        for col in self.df.select_dtypes(include=[np.number]).columns:
            if col in self.baselines:
                current_mean = self.df[col].mean()
                baseline = self.baselines[col]
                z_score = abs(current_mean - baseline['mean']) / max(baseline['stddev'], 1e-9)
                
                if z_score > 3:
                    self.add_result(
                        f"{self.contract_id}.{col}.drift", col, 'statistical_drift',
                        'FAIL', f'mean={current_mean:.3f}, z={z_score:.1f}',
                        f'within 3σ of baseline (mean={baseline["mean"]:.3f})', 'HIGH', 0,
                        f'Statistical drift: mean shifted {z_score:.1f}σ from baseline'
                    )
                elif z_score > 2:
                    self.add_result(
                        f"{self.contract_id}.{col}.drift", col, 'statistical_drift',
                        'WARN', f'mean={current_mean:.3f}, z={z_score:.1f}',
                        f'within 2σ of baseline (mean={baseline["mean"]:.3f})', 'MEDIUM', 0,
                        f'Statistical drift warning: mean shifted {z_score:.1f}σ'
                    )
    
    def run(self) -> Dict:
        print(f"\nValidating {self.data_path} against {self.contract_path}")
        print(f"Mode: {self.mode} (AUDIT=log only, WARN=block on CRITICAL, ENFORCE=block on CRITICAL+HIGH)")
        
        self.load_contract()
        self.load_data()
        self.load_baselines()
        
        self.check_range()
        self.check_statistical_drift()
        
        if not self.baselines and len(self.df) > 0:
            self.save_baseline()
        
        report = {
            'report_id': str(uuid.uuid4()),
            'contract_id': self.contract_id,
            'snapshot_id': hashlib.sha256(open(self.data_path, 'rb').read()).hexdigest(),
            'run_timestamp': datetime.utcnow().isoformat(),
            'validation_mode': self.mode,
            'total_checks': len(self.results),
            'passed': sum(1 for r in self.results if r['status'] == 'PASS'),
            'failed': sum(1 for r in self.results if r['status'] == 'FAIL'),
            'warned': sum(1 for r in self.results if r['status'] == 'WARN'),
            'errored': 0,
            'results': self.results,
            'blocked': self.blocking_violations
        }
        
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract', required=True)
    parser.add_argument('--data', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--mode', choices=['AUDIT', 'WARN', 'ENFORCE'], default='AUDIT')
    args = parser.parse_args()
    
    runner = ValidationRunner(args.contract, args.data, args.mode)
    report = runner.run()
    
    with open(args.output, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nReport saved: {args.output}")
    print(f"Mode: {report['validation_mode']}, Blocked: {report['blocked']}")
    print(f"Checks: {report['total_checks']}, Failed: {report['failed']}")
    
    # Exit with error code if blocked in ENFORCE mode
    if report['blocked']:
        print("\n❌ VALIDATION BLOCKED: CRITICAL/HIGH violations detected in ENFORCE mode")
        sys.exit(1)


if __name__ == '__main__':
    main()
