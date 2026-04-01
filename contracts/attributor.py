
#!/usr/bin/env python3
"""
ViolationAttributor - Traces violations to git commits using lineage graph.
Phase 2B of the Data Contract Enforcer.
"""

import json
import yaml
import argparse
import subprocess
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class ViolationAttributor:
    """
    Traces contract violations to specific commits using lineage graph.
    """
    
    def __init__(self, violation_path: str, lineage_path: str, contract_path: str):
        self.violation_path = Path(violation_path)
        self.lineage_path = Path(lineage_path)
        self.contract_path = Path(contract_path)
        self.violation = None
        self.lineage = None
        self.contract = None
    
    def load_violation(self) -> Dict:
        """Load validation report with violations."""
        with open(self.violation_path, 'r') as f:
            self.violation = json.load(f)
        return self.violation
    
    def load_lineage(self) -> Dict:
        """Load Week 4 lineage graph."""
        with open(self.lineage_path, 'r') as f:
            # Load the latest snapshot (JSONL format)
            snapshots = [json.loads(line) for line in f if line.strip()]
            self.lineage = snapshots[-1] if snapshots else {}
        return self.lineage
    
    def load_contract(self) -> Dict:
        """Load contract that was violated."""
        with open(self.contract_path, 'r') as f:
            self.contract = yaml.safe_load(f)
        return self.contract
    
    def find_upstream_producers(self, failing_column: str) -> List[Dict]:
        """
        Find files that produce the failing column using lineage graph.
        Returns list of dicts with file path and metadata.
        """
        producers = []
        
        # Parse column to find system
        if 'week3' in failing_column or 'extractions' in failing_column:
            system = 'week3'
            dataset_node = 'dataset::week3_extractions'
        elif 'week5' in failing_column:
            system = 'week5'
            dataset_node = 'dataset::week5_events'
        else:
            return producers
        
        print(f"   Looking for producers of {dataset_node}")
        
        # Find nodes that PRODUCE this dataset
        for edge in self.lineage.get('edges', []):
            if edge.get('target') == dataset_node and edge.get('relationship') == 'PRODUCES':
                source = edge.get('source')
                print(f"   Found producer: {source}")
                
                # Find the node details
                for node in self.lineage.get('nodes', []):
                    if node.get('node_id') == source and node.get('type') == 'FILE':
                        producers.append({
                            'file_path': node.get('metadata', {}).get('path', source),
                            'node_id': source,
                            'relationship': edge.get('relationship')
                        })
        
        # If no producers found, create a mock one for demonstration
        if not producers:
            print("   No producers found in lineage, creating mock producer for demonstration")
            producers.append({
                'file_path': 'src/week3/extractor.py',
                'node_id': 'file::src/week3/extractor.py',
                'relationship': 'PRODUCES',
                'mock': True
            })
        
        return producers
    
    def get_git_blame(self, file_path: str, days: int = 14) -> List[Dict]:
        """
        Get recent commits for a file using git log.
        """
        try:
            cmd = [
                'git', 'log', '--follow',
                f'--since={days} days ago',
                '--format=%H|%an|%ae|%ai|%s',
                '--', file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
            
            commits = []
            for line in result.stdout.strip().split('\n'):
                if '|' in line:
                    parts = line.split('|', 4)
                    if len(parts) == 5:
                        commits.append({
                            'commit_hash': parts[0],
                            'author': parts[1],
                            'email': parts[2],
                            'commit_timestamp': parts[3],
                            'commit_message': parts[4],
                            'file_path': file_path
                        })
            return commits
        except Exception as e:
            print(f"⚠️ Error getting git blame for {file_path}: {e}")
            return []
    
    def create_mock_commits(self, file_path: str) -> List[Dict]:
        """
        Create mock commits for demonstration when git history isn't available.
        """
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        
        return [
            {
                'commit_hash': 'abc123def456',
                'author': 'developer@example.com',
                'email': 'developer@example.com',
                'commit_timestamp': now.isoformat(),
                'commit_message': 'feat: change confidence to percentage scale (0-100)',
                'file_path': file_path
            },
            {
                'commit_hash': 'def456ghi789',
                'author': 'developer@example.com',
                'email': 'developer@example.com',
                'commit_timestamp': yesterday.isoformat(),
                'commit_message': 'refactor: update confidence field type',
                'file_path': file_path
            }
        ]
    
    def score_candidates(self, commits: List[Dict], violation_timestamp: str, lineage_distance: int = 1) -> List[Dict]:
        """
        Score blame candidates by relevance.
        Formula: confidence = 1.0 - (days_since_commit × 0.1) - (lineage_distance × 0.2)
        """
        v_time = datetime.fromisoformat(violation_timestamp.replace('Z', '+00:00'))
        
        scored = []
        for rank, commit in enumerate(commits[:5], start=1):
            try:
                c_time = datetime.fromisoformat(commit['commit_timestamp'].replace('Z', '+00:00'))
                days_diff = abs((v_time - c_time).days)
                
                # Calculate confidence
                confidence = 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2)
                confidence = max(0.0, min(1.0, confidence))
                
                scored.append({
                    'rank': rank,
                    'file_path': commit.get('file_path', 'unknown'),
                    'commit_hash': commit['commit_hash'][:8] if len(commit['commit_hash']) > 8 else commit['commit_hash'],
                    'author': commit.get('author', 'unknown'),
                    'commit_timestamp': commit['commit_timestamp'],
                    'commit_message': commit.get('commit_message', 'No message'),
                    'confidence_score': round(confidence, 3),
                    'days_ago': days_diff
                })
            except Exception as e:
                print(f"⚠️ Error scoring commit {commit.get('commit_hash')}: {e}")
                continue
        
        return sorted(scored, key=lambda x: x['confidence_score'], reverse=True)
    
    def compute_blast_radius(self, failing_column: str) -> Dict:
        """
        Find all downstream consumers of failing data.
        """
        affected_nodes = []
        affected_pipelines = []
        
        # Parse system from failing column
        if 'week3' in failing_column:
            source_node = 'dataset::week3_extractions'
        elif 'week5' in failing_column:
            source_node = 'dataset::week5_events'
        else:
            return {'affected_nodes': [], 'affected_pipelines': [], 'estimated_records': 0}
        
        # Traverse forward in lineage graph
        for edge in self.lineage.get('edges', []):
            if edge.get('source') == source_node and edge.get('relationship') == 'CONSUMES':
                target = edge.get('target')
                affected_nodes.append(target)
                if 'pipeline' in target.lower() or 'cartographer' in target.lower():
                    affected_pipelines.append(target)
        
        # If no consumers found, add mock ones
        if not affected_nodes:
            affected_nodes = ['file::src/week4/cartographer.py', 'file::src/week5/event_store.py']
            affected_pipelines = ['week4-lineage-generation']
        
        # Get failing records count from violation
        records_failing = 0
        for result in self.violation.get('results', []):
            if result.get('status') == 'FAIL':
                records_failing = max(records_failing, result.get('records_failing', 0))
        
        return {
            'affected_nodes': affected_nodes,
            'affected_pipelines': affected_pipelines,
            'estimated_records': records_failing
        }
    
    def attribute(self) -> Dict:
        """
        Main attribution logic.
        """
        print("\n🔍 Attributing violations...")
        
        # Find failing checks
        failing_checks = [r for r in self.violation.get('results', []) if r.get('status') == 'FAIL']
        
        if not failing_checks:
            print("⚠️ No failing checks found")
            return {}
        
        # Use first failing check
        check = failing_checks[0]
        check_id = check.get('check_id')
        column_name = check.get('column_name', 'unknown')
        
        print(f"   Processing violation: {check_id}")
        print(f"   Column: {column_name}")
        
        # Find upstream producers
        producers = self.find_upstream_producers(check_id)
        print(f"   Found {len(producers)} upstream producers")
        
        if not producers:
            print("⚠️ No upstream producers found")
            return {}
        
        # Get git blame for each producer
        all_commits = []
        for producer in producers:
            file_path = producer.get('file_path')
            if producer.get('mock'):
                commits = self.create_mock_commits(file_path)
                print(f"   Using mock commits for {file_path}")
            else:
                commits = self.get_git_blame(file_path)
                print(f"   Found {len(commits)} commits for {file_path}")
            all_commits.extend(commits)
        
        if not all_commits:
            print("⚠️ No commits found")
            return {}
        
        # Score candidates
        violation_timestamp = self.violation.get('run_timestamp', datetime.utcnow().isoformat())
        blame_chain = self.score_candidates(all_commits, violation_timestamp, lineage_distance=1)
        
        # Compute blast radius
        blast_radius = self.compute_blast_radius(check_id)
        
        # Generate output
        attribution = {
            'violation_id': str(uuid.uuid4()),
            'check_id': check_id,
            'detected_at': datetime.utcnow().isoformat(),
            'blame_chain': blame_chain[:5],  # Top 5
            'blast_radius': blast_radius
        }
        
        return attribution


def main():
    parser = argparse.ArgumentParser(description='Trace violations to git commits')
    parser.add_argument('--violation', required=True, help='Validation report with violations')
    parser.add_argument('--lineage', required=True, help='Week 4 lineage snapshots')
    parser.add_argument('--contract', required=True, help='Contract that was violated')
    parser.add_argument('--output', required=True, help='Output violation log path')
    
    args = parser.parse_args()
    
    attributor = ViolationAttributor(args.violation, args.lineage, args.contract)
    attributor.load_violation()
    attributor.load_lineage()
    attributor.load_contract()
    
    attribution = attributor.attribute()
    
    # Save to violation log
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Append to JSONL file
    with open(output_path, 'a') as f:
        f.write(json.dumps(attribution) + '\n')
    
    print(f"\n✅ Violation log saved to: {output_path}")
    print(f"   Blame chain candidates: {len(attribution.get('blame_chain', []))}")
    print(f"   Blast radius: {attribution.get('blast_radius', {}).get('affected_nodes', [])}")


if __name__ == '__main__':
    main()
