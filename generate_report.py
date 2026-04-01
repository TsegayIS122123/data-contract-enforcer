import json
from pathlib import Path
from datetime import datetime

print("=" * 70)
print("í³Š DATA CONTRACT ENFORCER - VIOLATION REPORT")
print("=" * 70)
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

violations_file = Path('violation_log/violations.jsonl')
if violations_file.exists():
    with open(violations_file) as f:
        violations = [json.loads(line) for line in f if line.strip() and line != '{}']
    
    print(f"í³ˆ TOTAL VIOLATIONS: {len(violations)}")
    
    for i, v in enumerate(violations, 1):
        print(f"\n{'â”€'*70}")
        print(f"Violation #{i}: {v.get('check_id', 'N/A')}")
        print(f"{'â”€'*70}")
        
        print(f"\ní¾¯ PRIMARY SUSPECT:")
        blame = v.get('blame_chain', [])
        if blame:
            top = blame[0]
            print(f"   Author: {top.get('author')}")
            print(f"   File: {top.get('file_path')}")
            print(f"   Commit: {top.get('commit_hash')}")
            print(f"   Message: {top.get('commit_message')}")
            print(f"   Confidence: {top.get('confidence_score') * 100:.0f}%")
        
        print(f"\ní²£ BLAST RADIUS:")
        blast = v.get('blast_radius', {})
        print(f"   {len(blast.get('affected_nodes', []))} downstream systems affected")
        print(f"   {blast.get('estimated_records', 0)} records impacted")
        
        if blast.get('affected_pipelines'):
            print(f"   Pipelines: {', '.join(blast.get('affected_pipelines', []))}")
    
    print(f"\n{'='*70}")
    print("âœ… RECOMMENDED ACTIONS:")
    print("   1. Revert the confidence scale change in src/week3/extractor.py")
    print("   2. Update all downstream consumers (cartographer, event store)")
    print("   3. Add contract validation to CI/CD pipeline")
    print("   4. Run full validation on all 50 affected records")
    print("="*70)
else:
    print("No violations found")

print()
