#!/usr/bin/env python3
"""
ValidationRunner - Executes all contract checks against data.
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
from typing import Dict, List, Any, Optional
import uuid
import re


class ValidationRunner:
    """
    Executes all contract checks and produces validation reports.
    Supports: required fields, uniqueness, enums, UUID format, date-time, range, statistical drift.
    """
    
    def __init__(self, contract_path: str, data_path: str):
        self.contract_path = Path(contract_path)
        self.data_path = Path(data_path)
        self.contract = None
        self.df = None
        self.baselines = {}
        self.results = []
        self.contract_id = None
    
    def load_contract(self) -> Dict:
        """Load contract YAML."""
        with open(self.contract_path, 'r', encoding='utf-8') as f:
            self.contract = yaml.safe_load(f)
        self.contract_id = self.contract.get('id', 'unknown')
        return self.contract
    
    def load_data(self) -> pd.DataFrame:
        """Load and flatten JSONL data."""
        records = []
        with open(self.data_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        if not records:
            raise ValueError(f"No records found in {self.data_path}")
        
        # Handle Week 3 nested structure (extracted_facts)
        if 'extracted_facts' in records[0]:
            rows = []
            for r in records:
                base = {k: v for k, v in r.items() 
                       if not isinstance(v, (list, dict))}
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
        """Load statistical baselines if they exist."""
        baseline_path = Path('schema_snapshots/baselines.json')
        if baseline_path.exists():
            with open(baseline_path, 'r') as f:
                data = json.load(f)
                self.baselines = data.get('columns', {})
    
    def save_baseline(self):
        """Save current statistics as baseline."""
        stats = {}
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            series = self.df[col].dropna()
            if len(series) > 0:
                stats[col] = {
                    'mean': float(series.mean()),
                    'stddev': float(series.std())
                }
        
        baseline_path = Path('schema_snapshots')
        baseline_path.mkdir(parents=True, exist_ok=True)
        
        with open(baseline_path / 'baselines.json', 'w') as f:
            json.dump({
                'written_at': datetime.utcnow().isoformat(),
                'columns': stats
            }, f, indent=2)
    
    def check_required_fields(self):
        """Check that required fields have no nulls."""
        schema = self.contract.get('schema', {})
        
        for field_name, field_schema in schema.items():
            if field_schema.get('required', False):
                # Handle nested extracted_facts fields
                if field_name == 'extracted_facts':
                    # Check required nested fields
                    items = field_schema.get('items', {})
                    for nested_field, nested_schema in items.items():
                        if nested_schema.get('required', False):
                            col_name = f'fact_{nested_field}'
                            if col_name in self.df.columns:
                                null_count = self.df[col_name].isna().sum()
                                if null_count > 0:
                                    self.results.append({
                                        'check_id': f'{self.contract_id}.{field_name}.{nested_field}.required',
                                        'column_name': f'extracted_facts.{nested_field}',
                                        'check_type': 'required',
                                        'status': 'FAIL',
                                        'actual_value': f'{null_count} null values',
                                        'expected': '0 null values',
                                        'severity': 'CRITICAL',
                                        'records_failing': null_count,
                                        'message': f'Required field {nested_field} has {null_count} null values'
                                    })
                elif field_name in self.df.columns:
                    null_count = self.df[field_name].isna().sum()
                    if null_count > 0:
                        self.results.append({
                            'check_id': f'{self.contract_id}.{field_name}.required',
                            'column_name': field_name,
                            'check_type': 'required',
                            'status': 'FAIL',
                            'actual_value': f'{null_count} null values',
                            'expected': '0 null values',
                            'severity': 'CRITICAL',
                            'records_failing': null_count,
                            'message': f'Required field {field_name} has {null_count} null values'
                        })
                else:
                    # Column doesn't exist in flattened data
                    self.results.append({
                        'check_id': f'{self.contract_id}.{field_name}.required',
                        'column_name': field_name,
                        'check_type': 'required',
                        'status': 'ERROR',
                        'actual_value': 'Column missing',
                        'expected': 'Column should exist',
                        'severity': 'CRITICAL',
                        'message': f'Required field {field_name} not found in data'
                    })
    
    def check_unique_constraints(self):
        """Check that unique fields have no duplicates."""
        schema = self.contract.get('schema', {})
        
        for field_name, field_schema in schema.items():
            if field_schema.get('unique', False):
                if field_name in self.df.columns:
                    duplicates = self.df[field_name].duplicated().sum()
                    if duplicates > 0:
                        self.results.append({
                            'check_id': f'{self.contract_id}.{field_name}.unique',
                            'column_name': field_name,
                            'check_type': 'unique',
                            'status': 'FAIL',
                            'actual_value': f'{duplicates} duplicate values',
                            'expected': '0 duplicates',
                            'severity': 'HIGH',
                            'records_failing': duplicates,
                            'message': f'Field {field_name} has {duplicates} duplicate values'
                        })
    
    def check_enum_constraints(self):
        """Check enum constraints."""
        schema = self.contract.get('schema', {})
        
        for field_name, field_schema in schema.items():
            enum_values = field_schema.get('enum')
            if enum_values and field_name in self.df.columns:
                invalid_mask = ~self.df[field_name].isin(enum_values)
                invalid_count = invalid_mask.sum()
                if invalid_count > 0:
                    sample_invalid = self.df[field_name][invalid_mask].head(3).tolist()
                    self.results.append({
                        'check_id': f'{self.contract_id}.{field_name}.enum',
                        'column_name': field_name,
                        'check_type': 'enum',
                        'status': 'FAIL',
                        'actual_value': f'{invalid_count} invalid values',
                        'expected': f'Must be one of {enum_values}',
                        'severity': 'HIGH',
                        'records_failing': invalid_count,
                        'sample_failing': sample_invalid,
                        'message': f'Field {field_name} contains values not in enum: {sample_invalid}'
                    })
    
    def check_uuid_format(self):
        """Check UUID format using regex."""
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        schema = self.contract.get('schema', {})
        
        for field_name, field_schema in schema.items():
            if field_schema.get('format') == 'uuid' and field_name in self.df.columns:
                # Convert to string and check pattern
                invalid_mask = ~self.df[field_name].astype(str).str.match(uuid_pattern, na=False)
                invalid_count = invalid_mask.sum()
                if invalid_count > 0:
                    sample_invalid = self.df[field_name][invalid_mask].head(3).tolist()
                    self.results.append({
                        'check_id': f'{self.contract_id}.{field_name}.uuid_format',
                        'column_name': field_name,
                        'check_type': 'format',
                        'status': 'FAIL',
                        'actual_value': f'{invalid_count} invalid UUIDs',
                        'expected': 'Valid UUID v4 format',
                        'severity': 'HIGH',
                        'records_failing': invalid_count,
                        'sample_failing': sample_invalid,
                        'message': f'Field {field_name} contains invalid UUIDs: {sample_invalid}'
                    })
    
    def check_date_time_format(self):
        """Check ISO 8601 date-time format."""
        schema = self.contract.get('schema', {})
        
        for field_name, field_schema in schema.items():
            if field_schema.get('format') == 'date-time' and field_name in self.df.columns:
                def is_valid_datetime(val):
                    if pd.isna(val):
                        return True
                    try:
                        datetime.fromisoformat(str(val).replace('Z', '+00:00'))
                        return True
                    except:
                        return False
                
                invalid_mask = ~self.df[field_name].apply(is_valid_datetime)
                invalid_count = invalid_mask.sum()
                if invalid_count > 0:
                    sample_invalid = self.df[field_name][invalid_mask].head(3).tolist()
                    self.results.append({
                        'check_id': f'{self.contract_id}.{field_name}.datetime_format',
                        'column_name': field_name,
                        'check_type': 'format',
                        'status': 'FAIL',
                        'actual_value': f'{invalid_count} invalid timestamps',
                        'expected': 'ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)',
                        'severity': 'MEDIUM',
                        'records_failing': invalid_count,
                        'sample_failing': sample_invalid,
                        'message': f'Field {field_name} contains invalid timestamps: {sample_invalid}'
                    })
    
    def check_range(self):
        """Check numeric range constraints."""
        schema = self.contract.get('schema', {})
        
        # Check top-level numeric fields
        for field_name, field_schema in schema.items():
            min_val = field_schema.get('minimum')
            max_val = field_schema.get('maximum')
            
            if (min_val is not None or max_val is not None) and field_name in self.df.columns:
                if pd.api.types.is_numeric_dtype(self.df[field_name]):
                    actual_min = self.df[field_name].min()
                    actual_max = self.df[field_name].max()
                    
                    min_ok = True
                    max_ok = True
                    if min_val is not None:
                        min_ok = actual_min >= min_val
                    if max_val is not None:
                        max_ok = actual_max <= max_val
                    
                    if not (min_ok and max_ok):
                        failing_mask = pd.Series(False, index=self.df.index)
                        if min_val is not None:
                            failing_mask = failing_mask | (self.df[field_name] < min_val)
                        if max_val is not None:
                            failing_mask = failing_mask | (self.df[field_name] > max_val)
                        failing_count = failing_mask.sum()
                        
                        self.results.append({
                            'check_id': f'{self.contract_id}.{field_name}.range',
                            'column_name': field_name,
                            'check_type': 'range',
                            'status': 'FAIL',
                            'actual_value': f'min={actual_min:.3f}, max={actual_max:.3f}',
                            'expected': f'min>={min_val}, max<={max_val}' if min_val and max_val else f'range constraint',
                            'severity': 'CRITICAL',
                            'records_failing': int(failing_count),
                            'message': f'Field {field_name} outside range [{min_val}, {max_val}]'
                        })
        
        # Check nested confidence field (Week 3 special case)
        if 'extracted_facts' in schema:
            items = schema['extracted_facts'].get('items', {})
            if 'confidence' in items:
                conf_schema = items['confidence']
                if 'fact_confidence' in self.df.columns:
                    actual_min = self.df['fact_confidence'].min()
                    actual_max = self.df['fact_confidence'].max()
                    min_val = conf_schema.get('minimum', -float('inf'))
                    max_val = conf_schema.get('maximum', float('inf'))
                    
                    min_ok = actual_min >= min_val
                    max_ok = actual_max <= max_val
                    
                    if not (min_ok and max_ok):
                        failing_mask = self.df['fact_confidence'] < min_val
                        failing_mask = failing_mask | (self.df['fact_confidence'] > max_val)
                        failing_count = failing_mask.sum()
                        
                        self.results.append({
                            'check_id': f'{self.contract_id}.confidence.range',
                            'column_name': 'extracted_facts.confidence',
                            'check_type': 'range',
                            'status': 'FAIL',
                            'actual_value': f'min={actual_min:.3f}, max={actual_max:.3f}',
                            'expected': f'min>={min_val}, max<={max_val}',
                            'severity': 'CRITICAL',
                            'records_failing': int(failing_count),
                            'sample_failing': self.df[failing_mask]['fact_confidence'].head(3).tolist(),
                            'message': f'Confidence outside 0.0-1.0 range! Found max={actual_max:.3f}'
                        })
                    else:
                        self.results.append({
                            'check_id': f'{self.contract_id}.confidence.range',
                            'column_name': 'extracted_facts.confidence',
                            'check_type': 'range',
                            'status': 'PASS',
                            'actual_value': f'min={actual_min:.3f}, max={actual_max:.3f}',
                            'expected': f'min>={min_val}, max<={max_val}',
                            'severity': 'INFO',
                            'message': 'Confidence within expected range'
                        })
    
    def check_pattern(self):
        """Check regex pattern constraints."""
        schema = self.contract.get('schema', {})
        
        for field_name, field_schema in schema.items():
            pattern = field_schema.get('pattern')
            if pattern and field_name in self.df.columns:
                invalid_mask = ~self.df[field_name].astype(str).str.match(pattern, na=False)
                invalid_count = invalid_mask.sum()
                if invalid_count > 0:
                    sample_invalid = self.df[field_name][invalid_mask].head(3).tolist()
                    self.results.append({
                        'check_id': f'{self.contract_id}.{field_name}.pattern',
                        'column_name': field_name,
                        'check_type': 'pattern',
                        'status': 'FAIL',
                        'actual_value': f'{invalid_count} values don\'t match pattern',
                        'expected': f'Pattern: {pattern}',
                        'severity': 'HIGH',
                        'records_failing': invalid_count,
                        'sample_failing': sample_invalid,
                        'message': f'Field {field_name} has values not matching pattern: {sample_invalid}'
                    })
    
    def check_statistical_drift(self):
        """Check statistical drift using z-score."""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col in self.baselines:
                current_mean = self.df[col].mean()
                current_std = self.df[col].std()
                baseline = self.baselines[col]
                
                z_score = abs(current_mean - baseline['mean']) / max(baseline['stddev'], 1e-9)
                
                if z_score > 3:
                    self.results.append({
                        'check_id': f'{self.contract_id}.{col}.drift',
                        'column_name': col,
                        'check_type': 'statistical_drift',
                        'status': 'FAIL',
                        'actual_value': f'mean={current_mean:.3f}, z={z_score:.1f}',
                        'expected': f'within 3σ of baseline (mean={baseline["mean"]:.3f})',
                        'severity': 'HIGH',
                        'z_score': round(z_score, 2),
                        'message': f'Statistical drift detected: {col} mean shifted {z_score:.1f}σ from baseline'
                    })
                elif z_score > 2:
                    self.results.append({
                        'check_id': f'{self.contract_id}.{col}.drift',
                        'column_name': col,
                        'check_type': 'statistical_drift',
                        'status': 'WARN',
                        'actual_value': f'mean={current_mean:.3f}, z={z_score:.1f}',
                        'expected': f'within 2σ of baseline (mean={baseline["mean"]:.3f})',
                        'severity': 'MEDIUM',
                        'z_score': round(z_score, 2),
                        'message': f'Statistical drift warning: {col} mean shifted {z_score:.1f}σ from baseline'
                    })
    
    def run(self) -> Dict:
        """Execute all validation checks."""
        print(f"\n Validating {self.data_path} against {self.contract_path}")
        
        self.load_contract()
        self.load_data()
        self.load_baselines()
        
        print(" Running structural checks...")
        self.check_required_fields()
        self.check_unique_constraints()
        self.check_enum_constraints()
        self.check_uuid_format()
        self.check_date_time_format()
        self.check_pattern()
        self.check_range()
        
        print(" Running statistical checks...")
        self.check_statistical_drift()
        
        # Save baseline if this is first run with data
        if not self.baselines and len(self.df) > 0:
            print(" Saving baseline statistics...")
            self.save_baseline()
        
        # Generate report
        report = {
            'report_id': str(uuid.uuid4()),
            'contract_id': self.contract['id'],
            'snapshot_id': hashlib.sha256(open(self.data_path, 'rb').read()).hexdigest(),
            'run_timestamp': datetime.utcnow().isoformat(),
            'total_checks': len(self.results),
            'passed': sum(1 for r in self.results if r['status'] == 'PASS'),
            'failed': sum(1 for r in self.results if r['status'] == 'FAIL'),
            'warned': sum(1 for r in self.results if r['status'] == 'WARN'),
            'errored': sum(1 for r in self.results if r['status'] == 'ERROR'),
            'results': self.results
        }
        
        return report


def main():
    parser = argparse.ArgumentParser(description='Validate data against contract')
    parser.add_argument('--contract', required=True, help='Contract YAML file')
    parser.add_argument('--data', required=True, help='JSONL data file')
    parser.add_argument('--output', required=True, help='Output report path')
    
    args = parser.parse_args()
    
    runner = ValidationRunner(args.contract, args.data)
    report = runner.run()
    
    # Save report
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n✅ Validation report saved to: {output_path}")
    print(f"   Total checks: {report['total_checks']}")
    print(f"   Passed: {report['passed']}")
    print(f"   Failed: {report['failed']}")
    print(f"   Warnings: {report['warned']}")
    print(f"   Errors: {report['errored']}")


if __name__ == '__main__':
    main()
