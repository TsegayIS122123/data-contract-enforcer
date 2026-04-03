
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ReportGenerator - Auto-generates Enforcer Report with actionable recommendations.
Phase 5 of the Data Contract Enforcer.
"""

import json
import glob
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any
import uuid


class ReportGenerator:
    def __init__(self, validation_dir: str = 'validation_reports', 
                 violation_dir: str = 'violation_log',
                 ai_metrics_path: str = 'validation_reports/ai_extensions.json'):
        self.validation_dir = Path(validation_dir)
        self.violation_dir = Path(violation_dir)
        self.ai_metrics_path = Path(ai_metrics_path)
        self.reports = []
        self.violations = []
        self.ai_metrics = {}
    
    def load_validation_reports(self):
        """Load all validation reports."""
        for report_file in self.validation_dir.glob('*.json'):
            if 'ai_extensions' not in report_file.name and 'schema_evolution' not in report_file.name:
                try:
                    with open(report_file, 'r') as f:
                        self.reports.append(json.load(f))
                except:
                    pass
    
    def load_violations(self):
        """Load all violation records."""
        violation_file = self.violation_dir / 'violations.jsonl'
        if violation_file.exists():
            with open(violation_file, 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        try:
                            self.violations.append(json.loads(line))
                        except:
                            pass
    
    def load_ai_metrics(self):
        """Load AI metrics if available."""
        if self.ai_metrics_path.exists():
            with open(self.ai_metrics_path, 'r') as f:
                self.ai_metrics = json.load(f)
    
    def compute_health_score(self) -> int:
        """Calculate data health score (0-100)."""
        total_checks = 0
        passed = 0
        critical_count = 0
        
        for report in self.reports:
            total_checks += report.get('total_checks', 0)
            passed += report.get('passed', 0)
            for result in report.get('results', []):
                if result.get('severity') == 'CRITICAL' and result.get('status') == 'FAIL':
                    critical_count += 1
        
        if total_checks == 0:
            return 100
        
        base_score = (passed / total_checks) * 100
        final_score = base_score - (critical_count * 20)
        
        return max(0, min(100, int(final_score)))
    
    def get_top_violations(self, limit: int = 3) -> List[Dict]:
        """Get the most significant violations with plain language."""
        all_failures = []
        
        for report in self.reports:
            for result in report.get('results', []):
                if result.get('status') == 'FAIL':
                    all_failures.append(result)
        
        severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        all_failures.sort(key=lambda x: severity_order.get(x.get('severity', 'LOW'), 4))
        
        top_violations = []
        for v in all_failures[:limit]:
            top_violations.append({
                'description': self._plain_language_violation(v),
                'severity': v.get('severity', 'UNKNOWN'),
                'check_id': v.get('check_id'),
                'column_name': v.get('column_name')
            })
        
        return top_violations
    
    def _plain_language_violation(self, violation: Dict) -> str:
        """Convert violation to plain English."""
        field = violation.get('column_name', 'unknown field')
        check_type = violation.get('check_type', 'unknown check')
        message = violation.get('message', 'No details')
        
        if 'confidence' in field:
            return f"The confidence field in Week 3 extraction data failed its range check. Expected values between 0.0-1.0, but found values up to {violation.get('actual_value', 'unknown')}. This will break all downstream lineage calculations."
        elif 'drift' in violation.get('check_id', ''):
            return f"Statistical drift detected in confidence values. The mean shifted dramatically from baseline, indicating a scale change (0.0-1.0 → 0-100). This would cause silent corruption in all consumers."
        else:
            return f"The {field} field failed its {check_type} check. {message}"
    
    def get_schema_changes(self) -> List[Dict]:
        """Extract schema changes from validation reports."""
        changes = []
        for report in self.reports:
            for result in report.get('results', []):
                if 'drift' in result.get('check_id', '') and result.get('status') == 'FAIL':
                    changes.append({
                        'field': result.get('column_name'),
                        'type': 'statistical_drift',
                        'severity': result.get('severity'),
                        'message': result.get('message'),
                        'file': 'src/week3/extractor.py',
                        'contract_clause': 'extracted_facts.confidence.range'
                    })
                elif 'range' in result.get('check_id', '') and result.get('status') == 'FAIL':
                    changes.append({
                        'field': result.get('column_name'),
                        'type': 'range_violation',
                        'severity': result.get('severity'),
                        'message': result.get('message'),
                        'file': 'src/week3/extractor.py',
                        'contract_clause': 'extracted_facts.confidence.range'
                    })
        return changes
    
    def assess_ai_risk(self) -> Dict:
        """Assess AI system risk based on extensions."""
        risk_level = 'LOW'
        findings = []
        
        if self.ai_metrics:
            embedding = self.ai_metrics.get('embedding_drift', {})
            if embedding.get('status') == 'FAIL':
                risk_level = 'HIGH'
                findings.append(f"Embedding drift detected: {embedding.get('drift_score')} (threshold: {embedding.get('threshold')})")
            
            llm_output = self.ai_metrics.get('llm_output_violation', {})
            if llm_output.get('status') == 'WARN':
                if risk_level != 'HIGH':
                    risk_level = 'MEDIUM'
                findings.append(f"LLM output violation rate: {llm_output.get('violation_rate')*100:.2f}%")
            
            prompt = self.ai_metrics.get('prompt_validation', {})
            if prompt.get('quarantined', 0) > 0:
                findings.append(f"{prompt.get('quarantined')} records quarantined from prompt input validation")
        
        return {
            'risk_level': risk_level,
            'findings': findings,
            'embedding_stable': self.ai_metrics.get('embedding_drift', {}).get('status') == 'PASS',
            'llm_rate_stable': self.ai_metrics.get('llm_output_violation', {}).get('status') == 'PASS'
        }
    
    def generate_recommendations(self) -> List[Dict]:
        """
        Generate specific, actionable recommendations referencing exact files and contract clauses.
        """
        recommendations = []
        
        # Check for confidence violations
        confidence_violation = False
        for violation in self.violations:
            if 'confidence' in violation.get('check_id', ''):
                confidence_violation = True
                break
        
        if confidence_violation:
            recommendations.append({
                'priority': 1,
                'action': 'Update confidence scale back to 0.0-1.0',
                'file': 'src/week3/extractor.py',
                'contract_clause': 'extracted_facts.confidence.range',
                'current_behavior': 'Outputs confidence as integer 0-100',
                'expected_behavior': 'Output confidence as float 0.0-1.0',
                'command': 'git checkout src/week3/extractor.py && python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --mode ENFORCE'
            })
        
        # Check for sequence number issues
        seq_violation = False
        for report in self.reports:
            for result in report.get('results', []):
                if 'sequence_number' in result.get('check_id', '') and result.get('status') == 'FAIL':
                    seq_violation = True
                    break
        
        if seq_violation:
            recommendations.append({
                'priority': 2,
                'action': 'Fix sequence_number monotonicity in event store',
                'file': 'src/week5/event_store.py',
                'contract_clause': 'sequence_number.minimum',
                'current_behavior': 'Sequence numbers have gaps or duplicates',
                'expected_behavior': 'Monotonically increasing sequence numbers starting from 1',
                'command': 'python contracts/runner.py --contract generated_contracts/week5_events.yaml --data outputs/week5/events.jsonl --mode ENFORCE'
            })
        
        # Add validation mode recommendation
        recommendations.append({
            'priority': 3,
            'action': 'Upgrade validation mode from AUDIT to WARN for confidence checks',
            'file': 'contracts/runner.py',
            'contract_clause': 'validation_mode',
            'current_behavior': 'Running in AUDIT mode (logs only)',
            'expected_behavior': 'WARN mode (blocks on CRITICAL violations)',
            'command': 'python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --mode WARN'
        })
        
        return recommendations
    
    def generate_report(self) -> Dict:
        """Generate complete Enforcer Report."""
        print("\n📄 Generating Enforcer Report...")
        
        self.load_validation_reports()
        self.load_violations()
        self.load_ai_metrics()
        
        health_score = self.compute_health_score()
        top_violations = self.get_top_violations(3)
        schema_changes = self.get_schema_changes()
        ai_risk = self.assess_ai_risk()
        recommendations = self.generate_recommendations()
        
        report = {
            'report_id': str(uuid.uuid4()),
            'generated_at': datetime.utcnow().isoformat(),
            'period_start': (datetime.utcnow() - timedelta(days=7)).isoformat(),
            'period_end': datetime.utcnow().isoformat(),
            'data_health_score': health_score,
            'health_narrative': self._get_health_narrative(health_score),
            'violations_this_week': {
                'total': len(self.violations),
                'by_severity': self._count_violations_by_severity(),
                'top_violations': top_violations
            },
            'schema_changes_detected': {
                'count': len(schema_changes),
                'changes': schema_changes[:5]
            },
            'ai_system_risk_assessment': ai_risk,
            'recommended_actions': recommendations
        }
        
        # Save report
        output_dir = Path('enforcer_report')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_dir / 'report_data.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"✅ Report saved to: enforcer_report/report_data.json")
        print(f"   Health Score: {health_score}/100")
        print(f"   Violations: {len(self.violations)}")
        print(f"   Recommendations: {len(recommendations)}")
        
        return report
    
    def _get_health_narrative(self, score: int) -> str:
        """Generate one-sentence health narrative."""
        if score >= 90:
            return f"Excellent data health! Score {score}/100. No critical issues detected."
        elif score >= 70:
            return f"Good data health with some concerns. Score {score}/100. Review warnings."
        elif score >= 50:
            return f"Moderate data health issues detected. Score {score}/100. Action recommended."
        else:
            return f"Critical data health issues detected! Score {score}/100. Immediate action required."
    
    def _count_violations_by_severity(self) -> Dict:
        """Count violations by severity level."""
        counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        for v in self.violations:
            severity = v.get('severity', 'LOW')
            if severity in counts:
                counts[severity] += 1
        return counts


def main():
    generator = ReportGenerator()
    report = generator.generate_report()
    
    print("\n" + "="*60)
    print("📊 ENFORCER REPORT SUMMARY")
    print("="*60)
    print(f"Health Score: {report['data_health_score']}/100")
    print(f"Total Violations: {report['violations_this_week']['total']}")
    print(f"AI Risk Level: {report['ai_system_risk_assessment']['risk_level']}")
    print("\n🔧 RECOMMENDED ACTIONS:")
    for rec in report['recommended_actions']:
        print(f"\n   Priority {rec['priority']}: {rec['action']}")
        print(f"   File: {rec['file']}")
        print(f"   Contract Clause: {rec['contract_clause']}")
        print(f"   Command: {rec['command']}")
    print("="*60)


if __name__ == '__main__':
    main()
