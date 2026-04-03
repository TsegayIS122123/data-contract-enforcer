
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SchemaEvolutionAnalyzer - Detects schema changes with rollback planning.
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
        fields = {}
        def traverse(obj, prefix=''):
            if isinstance(obj, dict):
                if 'type' in obj and not prefix.endswith('items'):
                    fields[prefix] = obj
                if 'extracted_facts' in obj:
                    items = obj['extracted_facts'].get('items', {})
                    for k, v in items.items():
                        fields[f'extracted_facts.{k}'] = v
                for key, value in obj.items():
                    if key not in ['description', 'examples']:
                        traverse(value, f"{prefix}.{key}" if prefix else key)
        traverse(schema.get('schema', {}))
        return fields
    
    def classify_change(self, field_name: str, old: Dict, new: Dict) -> Dict:
        if old is None:
            return {
                'field': field_name, 'type': 'ADD_NULLABLE_FIELD' if not new.get('required') else 'ADD_REQUIRED_FIELD',
                'compatible': not new.get('required', False),
                'severity': 'COMPATIBLE' if not new.get('required') else 'BREAKING',
                'message': f"Field '{field_name}' added ({'required' if new.get('required') else 'nullable'})"
            }
        if new is None:
            return {
                'field': field_name, 'type': 'REMOVE_FIELD', 'compatible': False,
                'severity': 'BREAKING', 'message': f"Field '{field_name}' removed"
            }
        
        old_type = old.get('type')
        new_type = new.get('type')
        if old_type != new_type:
            is_narrowing = (old_type == 'number' and new_type == 'integer') or (old_type == 'float' and new_type == 'int')
            return {
                'field': field_name, 'type': 'NARROW_TYPE' if is_narrowing else 'TYPE_CHANGE',
                'compatible': False, 'severity': 'CRITICAL' if is_narrowing else 'BREAKING',
                'message': f"Type changed from {old_type} to {new_type} - {'CRITICAL data loss risk!' if is_narrowing else 'breaking change'}"
            }
        
        old_min, old_max = old.get('minimum'), old.get('maximum')
        new_min, new_max = new.get('minimum'), new.get('maximum')
        if old_min != new_min or old_max != new_max:
            if old_min == 0.0 and old_max == 1.0 and new_min == 0 and new_max == 100:
                return {
                    'field': field_name, 'type': 'CONFIDENCE_SCALE_CHANGE', 'compatible': False,
                    'severity': 'CRITICAL', 'message': f"Confidence scale changed! [{old_min},{old_max}] → [{new_min},{new_max}]"
                }
            return {
                'field': field_name, 'type': 'RANGE_CHANGE', 'compatible': False,
                'severity': 'BREAKING', 'message': f"Range changed from [{old_min},{old_max}] to [{new_min},{new_max}]"
            }
        
        return {'field': field_name, 'type': 'NO_CHANGE', 'compatible': True, 'severity': 'INFO', 'message': 'No change'}
    
    def compute_blast_radius(self, changes: List[Dict]) -> Dict:
        registry_path = Path('contract_registry/subscriptions.yaml')
        affected = []
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                registry = yaml.safe_load(f)
            for sub in registry.get('subscriptions', []):
                if sub.get('contract_id') == self.contract_id:
                    for change in changes:
                        if not change.get('compatible', True):
                            affected.append({
                                'subscriber_id': sub.get('subscriber_id'),
                                'subscriber_team': sub.get('subscriber_team'),
                                'validation_mode': sub.get('validation_mode'),
                                'contamination_depth': 1
                            })
                            break
        return {'affected_subscribers': affected, 'total_affected': len(affected)}
    
    def generate_rollback_plan(self, changes: List[Dict]) -> Dict:
        return {
            'steps': [
                "git log --oneline schema_snapshots/ to find last good snapshot",
                f"git checkout {self.snapshots[-2]['timestamp']} -- schema_snapshots/{self.contract_id}/",
                "python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/",
                "python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/rollback_verify.json",
                "Re-establish statistical baseline by running validation with --mode AUDIT for 24 hours"
            ],
            'estimated_time': '15 minutes',
            'data_loss_risk': 'Low - snapshots preserve previous schema state'
        }
    
    def per_consumer_failure_analysis(self, changes: List[Dict]) -> List[Dict]:
        analysis = []
        registry_path = Path('contract_registry/subscriptions.yaml')
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                registry = yaml.safe_load(f)
            for sub in registry.get('subscriptions', []):
                if sub.get('contract_id') == self.contract_id:
                    for change in changes:
                        if not change.get('compatible', True):
                            analysis.append({
                                'subscriber': sub.get('subscriber_id'),
                                'team': sub.get('subscriber_team'),
                                'failure_mode': f"Schema change {change['type']} will cause validation failure in {sub.get('validation_mode')} mode",
                                'required_action': f"Update code to handle {change['field']} change before deployment",
                                'deadline': 'Before next deployment cycle'
                            })
                            break
        return analysis
    
    def run(self) -> Dict:
        print(f"\nAnalyzing schema evolution for {self.contract_id}")
        snapshots = self.load_snapshots()
        if len(snapshots) < 2:
            raise ValueError(f"Need at least 2 snapshots, found {len(snapshots)}")
        
        old_snapshot = snapshots[-2]
        new_snapshot = snapshots[-1]
        print(f"Comparing: {old_snapshot['timestamp']} → {new_snapshot['timestamp']}")
        
        old_fields = self.extract_fields(old_snapshot['schema'])
        new_fields = self.extract_fields(new_snapshot['schema'])
        
        changes = []
        for field in set(old_fields.keys()) | set(new_fields.keys()):
            old_clause = old_fields.get(field)
            new_clause = new_fields.get(field)
            if old_clause != new_clause:
                classification = self.classify_change(field, old_clause, new_clause)
                changes.append(classification)
                print(f"   {classification['severity']}: {classification['message']}")
        
        blast = self.compute_blast_radius(changes)
        rollback = self.generate_rollback_plan(changes)
        consumer_analysis = self.per_consumer_failure_analysis(changes)
        
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
            'rollback_plan': rollback,
            'per_consumer_failure_analysis': consumer_analysis,
            'overall_verdict': 'BREAKING' if any(not c.get('compatible', True) for c in changes) else 'COMPATIBLE'
        }
        
        output_dir = Path('validation_reports')
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"schema_evolution_{self.contract_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nReport saved: {report_path}")
        print(f"Breaking changes: {len(report['breaking_changes'])}")
        print(f"Affected subscribers: {report['blast_radius']['total_affected']}")
        print(f"Rollback plan generated with {len(rollback['steps'])} steps")
        
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract-id', required=True)
    parser.add_argument('--since', default='7')
    args = parser.parse_args()
    analyzer = SchemaEvolutionAnalyzer(args.contract_id, int(args.since))
    analyzer.run()


if __name__ == '__main__':
    main()
