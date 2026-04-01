#!/usr/bin/env python3
"""
ValidationRunner - Executes contract checks against data.
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

class ValidationRunner:
    def __init__(self, contract_path: str, data_path: str):
        self.contract_path = Path(contract_path)
        self.data_path = Path(data_path)
        self.contract = None
        self.df = None
        self.results = []
    
    def load_contract(self) -> Dict:
        with open(self.contract_path, 'r', encoding='utf-8') as f:
            self.contract = yaml.safe_load(f)
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
    
    def check_range(self):
        """Check confidence range constraint."""
        schema = self.contract.get('schema', {})
        
        if 'extracted_facts' in schema:
            items = schema['extracted_facts'].get('items', {})
            if 'confidence' in items and 'fact_confidence' in self.df.columns:
                conf_schema = items['confidence']
                actual_min = self.df['fact_confidence'].min()
                actual_max = self.df['fact_confidence'].max()
                
                min_ok = actual_min >= conf_schema.get('minimum', -float('inf'))
                max_ok = actual_max <= conf_schema.get('maximum', float('inf'))
                
                if not (min_ok and max_ok):
                    failing = self.df[
                        (self.df['fact_confidence'] < conf_schema.get('minimum', -float('inf'))) |
                        (self.df['fact_confidence'] > conf_schema.get('maximum', float('inf')))
                    ].shape[0]
                    
                    self.results.append({
                        'check_id': f'{self.contract["id"]}.confidence.range',
                        'column_name': 'extracted_facts.confidence',
                        'check_type': 'range',
                        'status': 'FAIL',
                        'actual_value': f'min={actual_min:.3f}, max={actual_max:.3f}',
                        'expected': f'min>={conf_schema.get("minimum")}, max<={conf_schema.get("maximum")}',
                        'severity': 'CRITICAL',
                        'records_failing': failing,
                        'message': f'Confidence outside 0.0-1.0 range! Found max={actual_max:.3f}'
                    })
                else:
                    self.results.append({
                        'check_id': f'{self.contract["id"]}.confidence.range',
                        'column_name': 'extracted_facts.confidence',
                        'check_type': 'range',
                        'status': 'PASS',
                        'actual_value': f'min={actual_min:.3f}, max={actual_max:.3f}',
                        'expected': f'min>={conf_schema.get("minimum")}, max<={conf_schema.get("maximum")}',
                        'severity': 'INFO'
                    })
    
    def run(self) -> Dict:
        print(f"\nValidating {self.data_path}")
        
        self.load_contract()
        self.load_data()
        self.check_range()
        
        report = {
            'report_id': str(uuid.uuid4()),
            'contract_id': self.contract['id'],
            'snapshot_id': hashlib.sha256(open(self.data_path, 'rb').read()).hexdigest(),
            'run_timestamp': datetime.utcnow().isoformat(),
            'total_checks': len(self.results),
            'passed': sum(1 for r in self.results if r['status'] == 'PASS'),
            'failed': sum(1 for r in self.results if r['status'] == 'FAIL'),
            'warned': sum(1 for r in self.results if r['status'] == 'WARN'),
            'errored': 0,
            'results': self.results
        }
        
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract', required=True)
    parser.add_argument('--data', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    
    runner = ValidationRunner(args.contract, args.data)
    report = runner.run()
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nReport saved to: {output_path}")
    print(f"Checks: {report['total_checks']}, Failed: {report['failed']}")


if __name__ == '__main__':
    main()
