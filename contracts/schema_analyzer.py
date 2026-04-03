#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SchemaEvolutionAnalyzer - Detects and classifies schema changes.
Phase 3 of the Data Contract Enforcer.
"""

import json
import yaml
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import uuid


class SchemaEvolutionAnalyzer:
    
    def __init__(self, contract_id: str, since_days: int = 7):
        self.contract_id = contract_id
        self.snapshots_dir = Path('schema_snapshots') / contract_id
        self.snapshots = []
    
    def load_snapshots(self) -> List[Dict]:
        if not self.snapshots_dir.exists():
            raise FileNotFoundError(f"No snapshots found for {self.contract_id}")
        
        snapshots = []
        for yaml_file in sorted(self.snapshots_dir.glob('*.yaml')):
            with open(yaml_file, 'r') as f:
                snapshots.append({
                    'timestamp': yaml_file.stem,
                    'path': str(yaml_file),
                    'schema': yaml.safe_load(f)
                })
        self.snapshots = snapshots
        return snapshots
    
    def extract_fields(self, schema: Dict) -> Dict:
        """Extract all field definitions with their constraints."""
        fields = {}
        
        def traverse(obj, prefix=''):
            if isinstance(obj, dict):
                # Check if this is a field definition
                if 'type' in obj and not prefix.endswith('items'):
                    fields[prefix] = obj
                # Handle nested extracted_facts
                if 'extracted_facts' in obj:
                    items = obj['extracted_facts'].get('items', {})
                    for k, v in items.items():
                        fields[f'extracted_facts.{k}'] = v
                # Continue traversal
                for key, value in obj.items():
                    if key not in ['description', 'examples']:
                        new_prefix = f"{prefix}.{key}" if prefix else key
                        traverse(value, new_prefix)
        
        traverse(schema.get('schema', {}))
        return fields
    
    def classify_change(self, field_name: str, old: Dict, new: Dict) -> Dict:
        """Classify change using Confluent taxonomy."""
        
        # NEW FIELD ADDED
        if old is None:
            return {
                'field': field_name,
                'type': 'ADD_NULLABLE_FIELD' if not new.get('required') else 'ADD_REQUIRED_FIELD',
                'compatible': not new.get('required', False),
                'severity': 'COMPATIBLE' if not new.get('required') else 'BREAKING',
                'message': f"Field '{field_name}' added ({'required' if new.get('required') else 'nullable'})"
            }
        
        # FIELD REMOVED
        if new is None:
            return {
                'field': field_name,
                'type': 'REMOVE_FIELD',
                'compatible': False,
                'severity': 'BREAKING',
                'message': f"Field '{field_name}' removed"
            }
        
        # TYPE CHANGE - This catches confidence float → int!
        old_type = old.get('type')
        new_type = new.get('type')
        if old_type != new_type:
            is_narrowing = (old_type == 'number' and new_type == 'integer') or (old_type == 'float' and new_type == 'int')
            return {
                'field': field_name,
                'type': 'NARROW_TYPE' if is_narrowing else 'TYPE_CHANGE',
                'compatible': False,
                'severity': 'CRITICAL' if is_narrowing else 'BREAKING',
                'message': f"Type changed from {old_type} to {new_type} - {'CRITICAL data loss risk!' if is_narrowing else 'breaking change'}"
            }
        
        # RANGE CHANGE - This catches confidence 0.0-1.0 → 0-100!
        old_min = old.get('minimum')
        old_max = old.get('maximum')
        new_min = new.get('minimum')
        new_max = new.get('maximum')
        
        if old_min != new_min or old_max != new_max:
            # Detect confidence scale change
            if old_min == 0.0 and old_max == 1.0 and new_min == 0 and new_max == 100:
                return {
                    'field': field_name,
                    'type': 'CONFIDENCE_SCALE_CHANGE',
                    'compatible': False,
                    'severity': 'CRITICAL',
                    'message': f"Confidence scale changed from [{old_min}, {old_max}] to [{new_min}, {new_max}] - SILENT CORRUPTION RISK!"
                }
            return {
                'field': field_name,
                'type': 'RANGE_CHANGE',
                'compatible': False,
                'severity': 'BREAKING',
                'message': f"Range changed from [{old_min}, {old_max}] to [{new_min}, {new_max}]"
            }
        
        # ENUM CHANGE
        old_enum = old.get('enum')
        new_enum = new.get('enum')
        if old_enum != new_enum:
            removed = set(old_enum or []) - set(new_enum or [])
            if removed:
                return {
                    'field': field_name,
                    'type': 'REMOVE_ENUM_VALUE',
                    'compatible': False,
                    'severity': 'BREAKING',
                    'message': f"Enum values removed: {removed}"
                }
        
        return {
            'field': field_name,
            'type': 'NO_CHANGE',
            'compatible': True,
            'severity': 'INFO',
            'message': 'No material change'
        }
    
    def compute_blast_radius(self, changes: List[Dict]) -> Dict:
        """Compute affected downstream systems."""
        registry_path = Path('contract_registry/subscriptions.yaml')
        affected = []
        
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                registry = yaml.safe_load(f)
            
            breaking_changes = [c for c in changes if not c.get('compatible', True)]
            if breaking_changes:
                for sub in registry.get('subscriptions', []):
                    if sub.get('contract_id') == self.contract_id:
                        affected.append({
                            'subscriber_id': sub.get('subscriber_id'),
                            'subscriber_team': sub.get('subscriber_team'),
                            'validation_mode': sub.get('validation_mode')
                        })
        
        return {'affected_subscribers': affected, 'total_affected': len(affected)}
    
    def run(self) -> Dict:
        print(f"\n🔍 Analyzing schema evolution for {self.contract_id}")
        
        snapshots = self.load_snapshots()
        if len(snapshots) < 2:
            raise ValueError(f"Need at least 2 snapshots, found {len(snapshots)}")
        
        old_snapshot = snapshots[-2]
        new_snapshot = snapshots[-1]
        
        print(f"   Comparing: {old_snapshot['timestamp']} → {new_snapshot['timestamp']}")
        
        old_fields = self.extract_fields(old_snapshot['schema'])
        new_fields = self.extract_fields(new_snapshot['schema'])
        
        changes = []
        all_fields = set(old_fields.keys()) | set(new_fields.keys())
        
        for field_name in all_fields:
            old_clause = old_fields.get(field_name)
            new_clause = new_fields.get(field_name)
            
            if old_clause != new_clause:
                classification = self.classify_change(field_name, old_clause, new_clause)
                changes.append(classification)
                print(f"   {classification['severity']}: {classification['message']}")
        
        blast = self.compute_blast_radius(changes)
        
        report = {
            'report_id': str(uuid.uuid4()),
            'contract_id': self.contract_id,
            'generated_at': datetime.utcnow().isoformat(),
            'old_snapshot': old_snapshot['timestamp'],
            'new_snapshot': new_snapshot['timestamp'],
            'changes': changes,
            'breaking_changes': [c for c in changes if not c.get('compatible', True)],
            'compatible_changes': [c for c in changes if c.get('compatible', True)],
            'blast_radius': blast,
            'overall_verdict': 'BREAKING' if any(not c.get('compatible', True) for c in changes) else 'COMPATIBLE'
        }
        
        output_dir = Path('validation_reports')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = output_dir / f"schema_evolution_{self.contract_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n✅ Report saved: {report_path}")
        print(f"   Breaking changes: {len(report['breaking_changes'])}")
        print(f"   Affected subscribers: {report['blast_radius']['total_affected']}")
        
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract-id', required=True)
    parser.add_argument('--since', default='7')
    parser.add_argument('--output')
    args = parser.parse_args()
    
    analyzer = SchemaEvolutionAnalyzer(args.contract_id, int(args.since))
    report = analyzer.run()


if __name__ == '__main__':
    main()
