#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ContractGenerator - Auto-generates data contracts from JSONL outputs.
Phase 1 of the Data Contract Enforcer.
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

# Optional import for LLM annotation
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class ContractGenerator:
    def __init__(self, source_path: str, contract_id: Optional[str] = None, lineage_path: Optional[str] = None):
        self.source_path = Path(source_path)
        if contract_id:
            self.contract_id = contract_id
        else:
            name = self.source_path.stem
            if 'extractions' in name:
                self.contract_id = 'week3_document_refinery'
            elif 'events' in name:
                self.contract_id = 'week5_event_sourcing'
            else:
                self.contract_id = name
        self.lineage_path = Path(lineage_path) if lineage_path else None
        self.df = None
        self.profiles = {}
        self.stats = {}
    
    def load_and_flatten(self) -> pd.DataFrame:
        records = []
        with open(self.source_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        if not records:
            raise ValueError(f"No records found in {self.source_path}")
        
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
            print(f"Loaded {len(records)} records, flattened to {len(self.df)} facts")
        else:
            self.df = pd.DataFrame(records)
            print(f"Loaded {len(records)} records")
        
        return self.df
    
    def structural_profiling(self):
        """Perform structural profiling on each column."""
        for col in self.df.columns:
            series = self.df[col]
            self.profiles[col] = {
                'name': col,
                'dtype': str(series.dtype),
                'null_fraction': float(series.isna().mean()),
                'cardinality': int(series.nunique()),
                'sample_values': [str(v) for v in series.dropna().unique()[:5]]
            }
        return self.profiles
    
    def statistical_profiling(self):
        """Perform statistical profiling on numeric columns."""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            series = self.df[col].dropna()
            if len(series) == 0:
                continue
            
            self.stats[col] = {
                'min': float(series.min()),
                'max': float(series.max()),
                'mean': float(series.mean()),
                'p25': float(series.quantile(0.25)),
                'p50': float(series.quantile(0.50)),
                'p75': float(series.quantile(0.75)),
                'p95': float(series.quantile(0.95)),
                'p99': float(series.quantile(0.99)),
                'stddev': float(series.std()),
                'count': len(series)
            }
            
            # Flag suspicious distributions (mean > 0.99 or mean < 0.01)
            if 'confidence' in col.lower():
                if self.stats[col]['mean'] > 0.99:
                    self.stats[col]['warning'] = "CRITICAL: Confidence mean > 0.99 - values may be clamped to 1.0"
                elif self.stats[col]['mean'] < 0.01:
                    self.stats[col]['warning'] = "CRITICAL: Confidence mean < 0.01 - extraction may be completely broken"
        
        return self.stats
    
    def save_baseline(self):
        """Save statistical baseline to schema_snapshots/baselines.json"""
        baseline = {
            'written_at': datetime.utcnow().isoformat(),
            'contract_id': self.contract_id,
            'columns': {}
        }
        
        for col, stat in self.stats.items():
            baseline['columns'][col] = {
                'mean': stat['mean'],
                'stddev': stat['stddev']
            }
        
        baseline_path = Path('schema_snapshots')
        baseline_path.mkdir(parents=True, exist_ok=True)
        
        with open(baseline_path / 'baselines.json', 'w') as f:
            json.dump(baseline, f, indent=2)
        
        print(f"Baseline saved to schema_snapshots/baselines.json")
    
    def llm_annotate_ambiguous_columns(self):
        """Use LLM to annotate columns with ambiguous meanings."""
        if not OPENAI_AVAILABLE:
            print("OpenAI not available - skipping LLM annotation")
            return {}
        
        annotations = {}
        ambiguous_patterns = ['score', 'value', 'status', 'flag', 'code', 'type']
        
        for col, profile in self.profiles.items():
            col_lower = col.lower()
            if any(p in col_lower for p in ambiguous_patterns):
                try:
                    client = OpenAI()
                    sample = profile['sample_values'][:3]
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[{
                            "role": "system",
                            "content": "You are a data contract annotator. Provide: (a) description, (b) business rule, (c) cross-column relationships."
                        }, {
                            "role": "user",
                            "content": f"Column '{col}' in table '{self.contract_id}'. Sample values: {sample}. Adjacent columns: {list(self.profiles.keys())[:5]}"
                        }],
                        max_tokens=200
                    )
                    annotations[col] = response.choices[0].message.content
                except Exception as e:
                    print(f"LLM annotation failed for {col}: {e}")
                    annotations[col] = f"Auto-detected {profile['dtype']} column"
        
        return annotations
    
    def inject_lineage_context(self) -> List[Dict]:
        """Inject downstream consumers from Week 4 lineage graph."""
        if not self.lineage_path or not self.lineage_path.exists():
            print("No lineage file - skipping lineage injection")
            return []
        
        with open(self.lineage_path, 'r') as f:
            snapshots = [json.loads(line) for line in f if line.strip()]
        
        if not snapshots:
            return []
        
        latest = snapshots[-1]
        consumers = []
        
        # Find edges where source contains our contract_id
        for edge in latest.get('edges', []):
            source = edge.get('source', '').lower()
            if self.contract_id.lower() in source or 'week3' in source and 'extractions' in self.contract_id:
                consumers.append({
                    'id': edge.get('target', 'unknown'),
                    'description': f"Consumes {self.contract_id} data via {edge.get('relationship', 'UNKNOWN')}",
                    'fields_consumed': self._get_fields_for_system(edge.get('target', '')),
                    'breaking_fields': ['extracted_facts.confidence', 'doc_id']
                })
        
        return consumers
    
    def _get_fields_for_system(self, target: str) -> List[str]:
        """Determine which fields a downstream system consumes."""
        target_lower = target.lower()
        if 'cartographer' in target_lower:
            return ['doc_id', 'extracted_facts', 'extraction_model']
        elif 'event' in target_lower:
            return ['event_id', 'event_type', 'payload']
        elif 'enforcer' in target_lower or 'week7' in target_lower:
            return ['extracted_facts.confidence', 'doc_id']
        return []
    
    def generate_schema(self) -> Dict:
        """Generate schema section with suspicious distribution warnings."""
        schema = {}
        
        for col, profile in self.profiles.items():
            if col.startswith('_'):
                continue
            
            if col.endswith('_id'):
                clause = {
                    'type': 'string', 'format': 'uuid', 'required': profile['null_fraction'] == 0.0,
                    'unique': profile['cardinality'] == len(self.df),
                    'description': f"Unique identifier"
                }
            elif col.endswith('_at'):
                clause = {
                    'type': 'string', 'format': 'date-time', 'required': profile['null_fraction'] == 0.0,
                    'description': "Timestamp"
                }
            elif 'confidence' in col.lower():
                clause = {
                    'type': 'number', 'minimum': 0.0, 'maximum': 1.0, 'required': profile['null_fraction'] == 0.0,
                    'description': "Confidence score. MUST remain 0.0-1.0 float.",
                    'x-warning': self.stats.get(col, {}).get('warning', None)
                }
            elif profile['dtype'] in ['float64', 'int64']:
                clause = {
                    'type': 'number' if 'float' in profile['dtype'] else 'integer',
                    'required': profile['null_fraction'] == 0.0,
                    'description': "Numeric field"
                }
                if col in self.stats:
                    clause['minimum'] = self.stats[col]['min']
                    clause['maximum'] = self.stats[col]['max']
            else:
                clause = {
                    'type': 'string', 'required': profile['null_fraction'] == 0.0,
                    'description': "String field"
                }
            
            schema[col] = clause
        
        # Handle nested extracted_facts
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
    
    def generate_dbt_schema(self, contract: Dict) -> Dict:
        """Generate dbt schema.yml with tests."""
        dbt_schema = {
            'version': 2,
            'models': [{
                'name': self.contract_id,
                'description': contract['info']['description'],
                'columns': []
            }]
        }
        
        for col_name, col_schema in contract.get('schema', {}).items():
            column = {'name': col_name, 'description': col_schema.get('description', '')}
            tests = []
            
            if col_schema.get('required'):
                tests.append('not_null')
            if col_schema.get('unique'):
                tests.append('unique')
            if col_schema.get('format') == 'uuid':
                tests.append('unique')
            if col_schema.get('minimum') == 0.0 and col_schema.get('maximum') == 1.0:
                tests.append({'accepted_values': {'values': ['range:0.0-1.0']}})
            
            if tests:
                column['tests'] = tests
            dbt_schema['models'][0]['columns'].append(column)
        
        return dbt_schema
    
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
                'local': {'type': 'local', 'path': str(self.source_path), 'format': 'jsonl'}
            },
            'schema': self.generate_schema(),
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks for data': [
                        f"missing_count({col}) = 0" for col in self.profiles if self.profiles[col]['null_fraction'] == 0.0 and not col.startswith('fact_')
                    ] + (["max(fact_confidence) <= 1.0", "min(fact_confidence) >= 0.0"] if 'fact_confidence' in self.profiles else []) + ["row_count >= 1"]
                }
            },
            'lineage': {
                'upstream': [],
                'downstream': self.inject_lineage_context()
            },
            'generated_at': datetime.utcnow().isoformat()
        }
    
    def run(self, output_dir: str):
        """Run complete generation pipeline."""
        print(f"\nGenerating contract for {self.source_path}")
        
        self.load_and_flatten()
        print(f"Structural profiling: {len(self.structural_profiling())} columns")
        print(f"Statistical profiling: {len(self.statistical_profiling())} numeric columns")
        
        # Save baseline
        self.save_baseline()
        
        # LLM annotation (optional)
        annotations = self.llm_annotate_ambiguous_columns()
        if annotations:
            print(f"LLM annotated {len(annotations)} columns")
        
        # Build contract
        contract = self.build_contract()
        dbt_schema = self.generate_dbt_schema(contract)
        
        # Save outputs
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        with open(output_path / f"{self.contract_id}.yaml", 'w') as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)
        
        with open(output_path / f"{self.contract_id}_dbt.yml", 'w') as f:
            yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False)
        
        print(f"Saved: {output_path / self.contract_id}.yaml")
        print(f"Saved: {output_path / self.contract_id}_dbt.yml")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--contract-id')
    parser.add_argument('--lineage')
    parser.add_argument('--output', default='generated_contracts')
    args = parser.parse_args()
    
    generator = ContractGenerator(args.source, args.contract_id, args.lineage)
    generator.run(args.output)


if __name__ == '__main__':
    main()
