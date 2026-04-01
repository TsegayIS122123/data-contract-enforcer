#!/usr/bin/env python3
"""
ContractGenerator - Auto-generates data contracts from JSONL outputs.
"""

import json
import yaml
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

class ContractGenerator:
    def __init__(self, source_path: str, contract_id: Optional[str] = None):
        self.source_path = Path(source_path)
        # Auto-generate contract_id from filename if not provided
        if contract_id:
            self.contract_id = contract_id
        else:
            # Extract from filename: extractions.jsonl -> week3_extractions
            name = self.source_path.stem
            if 'extractions' in name:
                self.contract_id = 'week3_extractions'
            elif 'events' in name:
                self.contract_id = 'week5_events'
            else:
                self.contract_id = name
        self.df = None
    
    def load_and_flatten(self) -> pd.DataFrame:
        """Load JSONL and flatten for analysis."""
        records = []
        with open(self.source_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        if not records:
            raise ValueError(f"No records found in {self.source_path}")
        
        # Check if this is Week 3 data (has extracted_facts)
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
            print(f"Loaded {len(records)} records, flattened to {len(self.df)} facts")
        else:
            self.df = pd.DataFrame(records)
            print(f"Loaded {len(records)} records")
        
        return self.df
    
    def detect_column_type(self, col_name: str, series: pd.Series) -> Dict:
        """Generate contract clause for column."""
        null_frac = series.isna().mean()
        
        # UUID detection
        if col_name.endswith('_id'):
            return {
                'type': 'string',
                'format': 'uuid',
                'required': null_frac == 0.0,
                'description': f"Unique identifier for {col_name.replace('_id', '')}"
            }
        
        # Timestamp detection
        if col_name.endswith('_at'):
            return {
                'type': 'string',
                'format': 'date-time',
                'required': null_frac == 0.0,
                'description': f"Timestamp for {col_name.replace('_at', '')}"
            }
        
        # Confidence detection
        if 'confidence' in col_name.lower():
            return {
                'type': 'number',
                'minimum': 0.0,
                'maximum': 1.0,
                'required': null_frac == 0.0,
                'description': "Confidence score. MUST remain 0.0-1.0 float."
            }
        
        # Numeric fields
        if pd.api.types.is_numeric_dtype(series):
            return {
                'type': 'number' if 'float' in str(series.dtype) else 'integer',
                'required': null_frac == 0.0,
                'minimum': float(series.min()),
                'maximum': float(series.max()),
                'description': "Numeric field"
            }
        
        # Default string field
        return {
            'type': 'string',
            'required': null_frac == 0.0,
            'description': "String field"
        }
    
    def generate_schema(self) -> Dict:
        """Generate schema section."""
        schema = {}
        
        for col in self.df.columns:
            if col.startswith('_'):
                continue
            clause = self.detect_column_type(col, self.df[col])
            if clause:
                schema[col] = clause
        
        # Handle nested extracted_facts structure
        if 'fact_confidence' in schema:
            schema = {
                'extracted_facts': {
                    'type': 'array',
                    'items': {
                        'confidence': schema.pop('fact_confidence', {}),
                        'fact_id': schema.pop('fact_id', {}),
                        'page_ref': schema.pop('fact_page_ref', {})
                    }
                },
                **{k: v for k, v in schema.items() if not k.startswith('fact_')}
            }
        
        return schema
    
    def generate_quality_checks(self) -> List[str]:
        """Generate Soda quality checks."""
        checks = []
        
        # Required fields check
        for col in self.df.columns:
            if self.df[col].isna().sum() == 0 and not col.startswith('fact_'):
                checks.append(f"missing_count({col}) = 0")
        
        # Confidence range check
        if 'fact_confidence' in self.df.columns:
            checks.append("max(fact_confidence) <= 1.0")
            checks.append("min(fact_confidence) >= 0.0")
        
        # Row count
        checks.append("row_count >= 1")
        
        return checks
    
    def build_contract(self) -> Dict:
        """Build complete Bitol contract."""
        return {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': self.contract_id,
            'info': {
                'title': self.contract_id.replace('_', ' ').title(),
                'version': '1.0.0',
                'owner': 'data-team',
                'description': f"Auto-generated contract for {self.source_path.name}"
            },
            'servers': {
                'local': {
                    'type': 'local',
                    'path': str(self.source_path),
                    'format': 'jsonl'
                }
            },
            'schema': self.generate_schema(),
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks for data': self.generate_quality_checks()
                }
            },
            'generated_at': datetime.utcnow().isoformat()
        }
    
    def generate_dbt_schema(self, contract: Dict) -> Dict:
        """Generate dbt schema.yml from contract."""
        dbt_schema = {
            'version': 2,
            'models': [
                {
                    'name': self.contract_id,
                    'description': contract['info']['description'],
                    'columns': []
                }
            ]
        }
        
        for col_name, col_schema in contract['schema'].items():
            column = {
                'name': col_name,
                'description': col_schema.get('description', '')
            }
            
            tests = []
            if col_schema.get('required'):
                tests.append('not_null')
            if col_schema.get('format') == 'uuid':
                tests.append('unique')
            if col_schema.get('minimum') == 0.0 and col_schema.get('maximum') == 1.0:
                tests.append({'accepted_values': {'values': ['range:0.0-1.0']}})
            
            if tests:
                column['tests'] = tests
            
            dbt_schema['models'][0]['columns'].append(column)
        
        return dbt_schema
    
    def run(self, output_dir: str) -> tuple:
        """Run the complete generation pipeline."""
        print(f"\nGenerating contract for {self.source_path}")
        
        self.load_and_flatten()
        print(f"Found {len(self.df.columns)} columns")
        
        contract = self.build_contract()
        dbt_schema = self.generate_dbt_schema(contract)
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        contract_file = output_path / f"{self.contract_id}.yaml"
        with open(contract_file, 'w', encoding='utf-8') as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)
        
        dbt_file = output_path / f"{self.contract_id}_dbt.yml"
        with open(dbt_file, 'w', encoding='utf-8') as f:
            yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False)
        
        print(f"Saved: {contract_file}")
        print(f"Saved: {dbt_file}")
        
        return str(contract_file), str(dbt_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, help='Path to JSONL source file')
    parser.add_argument('--contract-id', help='Contract identifier (optional)')
    parser.add_argument('--output', default='generated_contracts', help='Output directory')
    
    args = parser.parse_args()
    
    generator = ContractGenerator(args.source, args.contract_id)
    generator.run(args.output)


if __name__ == '__main__':
    main()
