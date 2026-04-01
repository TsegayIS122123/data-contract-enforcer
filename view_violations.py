import json
from pathlib import Path

print("=" * 70)
print("VIOLATION LOG")
print("=" * 70)

violations_file = Path("violation_log/violations.jsonl")
if violations_file.exists():
    with open(violations_file) as f:
        lines = [line.strip() for line in f if line.strip()]
    print(f"\nFound {len(lines)} violation records\n")
    for i, line in enumerate(lines, 1):
        try:
            v = json.loads(line)
            print(f"\n--- Violation {i} ---")
            print(f"ID: {v.get(\"violation_id\", \"N/A\")}")
            print(f"Check: {v.get(\"check_id\", \"N/A\")}")
            print(f"Detected: {v.get(\"detected_at\", \"N/A\")}")
            print("\nBlame Chain:")
            for candidate in v.get("blame_chain", []):
                print(f"  Rank {candidate.get(\"rank\")}: {candidate.get(\"author\")}")
                print(f"    File: {candidate.get(\"file_path\")}")
                print(f"    Commit: {candidate.get(\"commit_hash\")}")
                print(f"    Confidence: {candidate.get(\"confidence_score\")}")
            blast = v.get("blast_radius", {})
            print(f"\nBlast Radius:")
            print(f"  Affected nodes: {blast.get(\"affected_nodes\", [])}")
            print(f"  Records: {blast.get(\"estimated_records\", 0)}")
        except Exception as e:
            print(f"Error: {e}")
else:
    print("No violations found")

print("\n" + "=" * 70)
