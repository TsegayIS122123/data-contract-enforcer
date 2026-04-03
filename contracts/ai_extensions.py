#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Contract Extensions - Embedding drift, prompt validation, LLM output enforcement.
Phase 4 of the Data Contract Enforcer.
"""

import json
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import uuid

# Optional imports with fallbacks
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("Warning: openai not installed. Embedding drift will use mock data.")

try:
    from jsonschema import validate, ValidationError
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False
    print("Warning: jsonschema not installed. Prompt validation will be basic.")


# ============================================
# Extension 1: Embedding Drift Detection
# ============================================

def embed_texts_mock(texts, n=200):
    """Mock embedding for testing without OpenAI."""
    return np.random.randn(min(len(texts), n), 1536)


def embed_texts_real(texts, n=200, model='text-embedding-3-small'):
    """Real embedding using OpenAI."""
    if not OPENAI_AVAILABLE:
        return embed_texts_mock(texts, n)
    
    client = OpenAI()
    sample = texts[:n] if len(texts) > n else texts
    response = client.embeddings.create(input=sample, model=model)
    return np.array([e.embedding for e in response.data])


def check_embedding_drift(extractions_path: str, baseline_path: str = 'schema_snapshots/embedding_baselines.npz', threshold: float = 0.15) -> Dict:
    """
    Detect semantic drift in extracted facts text.
    """
    print("\n🤖 Running Embedding Drift Detection...")
    
    # Extract text from extractions
    texts = []
    with open(extractions_path, 'r') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                for fact in record.get('extracted_facts', []):
                    if fact.get('text'):
                        texts.append(fact['text'])
    
    if not texts:
        return {'status': 'ERROR', 'message': 'No text found', 'drift_score': None}
    
    # Get embeddings
    use_real = OPENAI_AVAILABLE and Path('.env').exists()
    if use_real:
        current_vecs = embed_texts_real(texts)
    else:
        current_vecs = embed_texts_mock(texts)
    
    current_centroid = current_vecs.mean(axis=0)
    
    # Check baseline
    baseline_file = Path(baseline_path)
    if not baseline_file.exists():
        # Save baseline
        baseline_file.parent.mkdir(parents=True, exist_ok=True)
        np.savez(baseline_file, centroid=current_centroid)
        return {
            'status': 'BASELINE_SET',
            'drift_score': 0.0,
            'message': 'Baseline established from current data',
            'texts_analyzed': len(texts)
        }
    
    # Load baseline and compute drift
    baseline = np.load(baseline_file)
    baseline_centroid = baseline['centroid']
    
    # Cosine similarity
    dot = np.dot(current_centroid, baseline_centroid)
    norm = np.linalg.norm(current_centroid) * np.linalg.norm(baseline_centroid)
    cosine_sim = dot / (norm + 1e-9)
    drift = 1 - cosine_sim
    
    return {
        'status': 'FAIL' if drift > threshold else 'PASS',
        'drift_score': round(float(drift), 4),
        'threshold': threshold,
        'texts_analyzed': len(texts),
        'message': f"Drift: {drift:.4f} (threshold: {threshold})"
    }


# ============================================
# Extension 2: Prompt Input Schema Validation
# ============================================

PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000}
    },
    "additionalProperties": False
}


def validate_prompt_inputs(extractions_path: str, quarantine_path: str = 'outputs/quarantine/') -> Dict:
    """
    Validate prompt inputs before sending to LLM.
    """
    print("\n🔒 Running Prompt Input Validation...")
    
    valid = []
    quarantined = []
    
    with open(extractions_path, 'r') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                # Create prompt input structure
                prompt_input = {
                    'doc_id': record.get('doc_id'),
                    'source_path': record.get('source_path'),
                    'content_preview': record.get('source_path', '')[:100]
                }
                
                try:
                    if JSONSCHEMA_AVAILABLE:
                        validate(instance=prompt_input, schema=PROMPT_INPUT_SCHEMA)
                    valid.append(prompt_input)
                except Exception as e:
                    quarantined.append({
                        'record': prompt_input,
                        'error': str(e),
                        'timestamp': datetime.utcnow().isoformat()
                    })
    
    # Save quarantined records
    if quarantined:
        quarantine_dir = Path(quarantine_path)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_file = quarantine_dir / f"quarantine_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
        with open(quarantine_file, 'a') as f:
            for q in quarantined:
                f.write(json.dumps(q) + '\n')
    
    return {
        'total': len(valid) + len(quarantined),
        'valid': len(valid),
        'quarantined': len(quarantined),
        'quarantine_rate': len(quarantined) / max(len(valid) + len(quarantined), 1),
        'quarantine_path': str(quarantine_file) if quarantined else None
    }


# ============================================
# Extension 3: LLM Output Schema Violation Rate
# ============================================

def check_output_schema_violation_rate(verdicts_path: str, threshold: float = 0.02) -> Dict:
    """
    Track LLM output schema violation rate.
    """
    print("\n📊 Running LLM Output Schema Validation...")
    
    violations = 0
    total = 0
    
    with open(verdicts_path, 'r') as f:
        for line in f:
            if line.strip():
                total += 1
                record = json.loads(line)
                
                # Check required fields
                if 'overall_verdict' not in record:
                    violations += 1
                elif record.get('overall_verdict') not in ['PASS', 'FAIL', 'WARN']:
                    violations += 1
                
                if 'scores' in record:
                    for score in record['scores'].values():
                        if isinstance(score, dict):
                            s = score.get('score', 0)
                            if not isinstance(s, int) or s < 1 or s > 5:
                                violations += 1
    
    rate = violations / max(total, 1)
    
    # Load previous baseline for trend
    baseline_file = Path('schema_snapshots/llm_baseline.json')
    baseline_rate = None
    trend = 'stable'
    
    if baseline_file.exists():
        with open(baseline_file, 'r') as f:
            baseline_data = json.load(f)
            baseline_rate = baseline_data.get('violation_rate', 0)
            trend = 'rising' if rate > baseline_rate * 1.5 else 'falling' if rate < baseline_rate * 0.5 else 'stable'
    
    # Save new baseline
    baseline_file.parent.mkdir(parents=True, exist_ok=True)
    with open(baseline_file, 'w') as f:
        json.dump({
            'violation_rate': rate,
            'total_outputs': total,
            'timestamp': datetime.utcnow().isoformat()
        }, f)
    
    result = {
        'total_outputs': total,
        'schema_violations': violations,
        'violation_rate': round(rate, 4),
        'trend': trend,
        'baseline_rate': baseline_rate,
        'status': 'WARN' if rate > threshold else 'PASS',
        'threshold': threshold
    }
    
    # Write to violation log if threshold breached
    if rate > threshold:
        violation_entry = {
            'violation_id': str(uuid.uuid4()),
            'check_id': 'llm_output_schema.violation_rate',
            'detected_at': datetime.utcnow().isoformat(),
            'severity': 'WARN',
            'message': f"LLM output schema violation rate {rate:.2%} exceeds threshold {threshold:.2%}",
            'details': result
        }
        
        violation_log = Path('violation_log/ai_violations.jsonl')
        violation_log.parent.mkdir(parents=True, exist_ok=True)
        with open(violation_log, 'a') as f:
            f.write(json.dumps(violation_entry) + '\n')
    
    return result


# ============================================
# Single Entry Point
# ============================================

def run_all_extractions(extractions_path: str, verdicts_path: str) -> Dict:
    """
    Run all three AI extensions.
    """
    print("\n" + "="*60)
    print("🤖 AI CONTRACT EXTENSIONS")
    print("="*60)
    
    results = {
        'run_timestamp': datetime.utcnow().isoformat(),
        'embedding_drift': check_embedding_drift(extractions_path),
        'prompt_validation': validate_prompt_inputs(extractions_path),
        'llm_output_violation': check_output_schema_violation_rate(verdicts_path)
    }
    
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['all', 'embedding', 'prompt', 'output'], default='all')
    parser.add_argument('--extractions', default='outputs/week3/extractions.jsonl')
    parser.add_argument('--verdicts', default='outputs/week2/verdicts.jsonl')
    parser.add_argument('--output', default='validation_reports/ai_extensions.json')
    
    args = parser.parse_args()
    
    if args.mode == 'all':
        results = run_all_extractions(args.extractions, args.verdicts)
    elif args.mode == 'embedding':
        results = check_embedding_drift(args.extractions)
    elif args.mode == 'prompt':
        results = validate_prompt_inputs(args.extractions)
    elif args.mode == 'output':
        results = check_output_schema_violation_rate(args.verdicts)
    
    # Save results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Results saved to: {output_path}")


if __name__ == '__main__':
    main()
