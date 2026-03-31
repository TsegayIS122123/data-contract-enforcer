# Phase 0: DOMAIN_NOTES.md 

## Question 1: Backward-Compatible vs Breaking Schema Changes

### Definition
A **backward-compatible** change means new consumers can read old data. A **breaking** change means old consumers cannot read new data, or data loss occurs.

### From My Week 1: Intent-Code Correlator (`intent_records.jsonl`)

**Example Schema:**
```json
{
  "intent_id": "uuid-v4",
  "description": "string",
  "code_refs": [{"file": "path", "line_start": 42, "line_end": 67}],
  "confidence": 0.87,
  "governance_tags": ["auth", "pii"],
  "created_at": "2025-01-15T14:23:00Z"
}
```

#### ✅ Backward-Compatible Examples (3)

| Change | Why It's Safe | Consumer Impact |
|--------|---------------|-----------------|
| **1. Add nullable `priority` field** | Old consumers ignore new field | Existing `intent_records` without priority still valid |
| **2. Add new value to `governance_tags` enum** (e.g., "compliance") | Old consumers only check for tags they know | They ignore unknown tags, no breakage |
| **3. Widen `confidence` from float32 to float64** | Same range, more precision | Old consumers read as float, no precision loss |

#### ❌ Breaking Examples (3)

| Change | Why It's Breaking | What Breaks |
|--------|-------------------|-------------|
| **1. Rename `confidence` → `confidence_score`** | Old consumers look for `confidence` | `KeyError` in all downstream code |
| **2. Change `confidence` from float to int** | Type mismatch | `TypeError` when comparing (float vs int) |
| **3. Make `code_refs` required (was nullable)** | Existing records without code_refs fail | Validation errors on historical data |

---

### From My Week 3: Document Refinery (`extractions.jsonl`)

**Example Schema:**
```json
{
  "doc_id": "uuid-v4",
  "extracted_facts": [
    {"fact_id": "uuid", "confidence": 0.93, "page_ref": 4}
  ],
  "extraction_model": "claude-3-5-sonnet"
}
```

#### ✅ Backward-Compatible Examples (3)

| Change | Why It's Safe |
|--------|---------------|
| **1. Add nullable `processing_time_ms`** | New field, old consumers ignore |
| **2. Add new model to `extraction_model` pattern** (e.g., "gpt-4") | Old consumers only check for specific models they support |
| **3. Add new entity type to `entities[].type` enum** (e.g., "ORGANIZATION") | Additive enum change, old consumers ignore unknown types |

#### ❌ Breaking Examples (3)

| Change | Why It's Breaking |
|--------|-------------------|
| **1. Change `confidence` from float 0.0-1.0 to int 0-100** | Range violation, type mismatch |
| **2. Remove `page_ref` field** | Consumers expecting page_ref for provenance break |
| **3. Change `fact_id` from UUID to integer** | Format mismatch, uniqueness assumptions break |

---

### From My Week 5: Event Sourcing Platform (`events.jsonl`)

**Example Schema:**
```json
{
  "event_id": "uuid-v4",
  "event_type": "DocumentProcessed",
  "sequence_number": 42,
  "payload": {},
  "schema_version": "1.0"
}
```

#### ✅ Backward-Compatible Examples (3)

| Change | Why It's Safe |
|--------|---------------|
| **1. Add nullable `correlation_id`** | New field, optional |
| **2. Add new event type to enum** | Old consumers ignore unknown types |
| **3. Widen `sequence_number` from int32 to int64** | More capacity, old consumers still read |

#### ❌ Breaking Examples (3)

| Change | Why It's Breaking |
|--------|-------------------|
| **1. Remove `schema_version` field** | Version detection breaks |
| **2. Change `event_type` from string to enum** | String consumers expecting string fail |
| **3. Make `payload` required with additionalProperties: false** | Existing events with extra fields fail validation |

---

## Question 2: The Confidence Scale Failure

### The Failure Trace

#### Step 1: The Breaking Change
A developer updates the Week 3 Document Refinery to output confidence as integer 0-100 instead of float 0.0-1.0:

```python
# BEFORE (Week 3 - Working)
confidence = 0.93  # float, 0.0-1.0

# AFTER (Breaking Change)
confidence = 93    # int, 0-100
```

#### Step 2: Week 4 Cartographer Ingests Data
The Cartographer reads Week 3 outputs to build lineage and metadata:

```python
# Week 4 Cartographer - expects float 0.0-1.0
def extract_metadata(extraction):
    confidence = extraction['extracted_facts'][0]['confidence']
    
    # THIS NOW BREAKS:
    if confidence < 0.5:  # TypeError: '<' not supported between int and float
        risk_level = "HIGH"
```

**Actual Failure:** `TypeError: '<' not supported between instances of 'int' and 'float'`

#### Step 3: Cascading Failures
1. **Lineage graph building fails** - Cartographer crashes on first record
2. **Blast radius analysis unavailable** - No lineage means no attribution
3. **Downstream consumers (Week 7 Enforcer)** receive incomplete lineage data
4. **All consumers of Week 3 data** now have wrong confidence values (93 instead of 0.93)

#### Step 4: The Contract Clause That Catches This

```yaml
# generated_contracts/week3_extractions.yaml
kind: DataContract
apiVersion: v3.0.0
id: week3-document-refinery-extractions
info:
  title: Week 3 Document Refinery - Extraction Records
  version: 1.0.0
  owner: week3-team
  description: "Contract for extracted facts from documents"

servers:
  local:
    type: local
    path: outputs/week3/extractions.jsonl
    format: jsonl

schema:
  extracted_facts:
    type: array
    description: "List of facts extracted from the document"
    items:
      type: object
      properties:
        confidence:
          type: number
          description: "Confidence score of the extraction"
          minimum: 0.0
          maximum: 1.0
          exclusiveMinimum: false
          exclusiveMaximum: false
          examples: [0.87, 0.93, 0.45]
          # CRITICAL: This clause catches the scale change
          # If confidence becomes int 0-100, both minimum and maximum fail
        fact_id:
          type: string
          format: uuid
        page_ref:
          type: integer
          nullable: true
          minimum: 1
      required: [confidence, fact_id]

quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - missing_count(confidence) = 0
      - max(confidence) <= 1.0    # CATCHES 0-100 SCALE
      - min(confidence) >= 0.0    # CATCHES NEGATIVE VALUES
      - invalid_percent(confidence, valid_min=0.0, valid_max=1.0) = 0

lineage:
  upstream: []
  downstream:
    - id: week4-cartographer
      description: "Consumes confidence for metadata and risk analysis"
      fields_consumed: [extracted_facts.confidence, doc_id]
      breaking_if_changed:
        - extracted_facts.confidence  # Any change to confidence schema is breaking
        - extracted_facts.confidence.type
        - extracted_facts.confidence.range
```

### Why This Works

| Check | What It Detects | Example Violation |
|-------|-----------------|-------------------|
| `type: number` | Type mismatch | int 93 instead of float 0.93 |
| `minimum: 0.0` | Range floor violation | confidence = -0.1 |
| `maximum: 1.0` | Range ceiling violation | confidence = 93 |
| `max(confidence) <= 1.0` | Statistical max | max = 93 → FAIL |
| `min(confidence) >= 0.0` | Statistical min | min = -0.1 → FAIL |

---

## Question 3: Lineage-Based Attribution - Step by Step

### The Lineage Graph from Week 4

From your Week 4 Cartographer output (`lineage_snapshots.jsonl`):

```json
{
  "snapshot_id": "snap-001",
  "nodes": [
    {"node_id": "file::src/week3/extractor.py", "type": "FILE"},
    {"node_id": "file::src/week4/cartographer.py", "type": "FILE"},
    {"node_id": "file::src/week5/event_store.py", "type": "FILE"},
    {"node_id": "dataset::week3_extractions", "type": "DATASET"},
    {"node_id": "function::extract_facts", "type": "FUNCTION"}
  ],
  "edges": [
    {"source": "file::src/week3/extractor.py", "target": "dataset::week3_extractions", "relationship": "PRODUCES"},
    {"source": "dataset::week3_extractions", "target": "file::src/week4/cartographer.py", "relationship": "CONSUMES"},
    {"source": "dataset::week3_extractions", "target": "file::src/week5/event_store.py", "relationship": "CONSUMES"},
    {"source": "function::extract_facts", "target": "dataset::week3_extractions", "relationship": "WRITES"}
  ]
}
```

### Step-by-Step Blame Chain Generation

#### Step 1: Violation Detection
ValidationRunner detects a confidence range violation:

```json
{
  "check_id": "week3.extracted_facts.confidence.range",
  "status": "FAIL",
  "actual_value": "max=93, mean=87.2",
  "expected": "max<=1.0, min>=0.0",
  "severity": "CRITICAL"
}
```

#### Step 2: Identify Failing Field and System
From the check_id:
- **System:** `week3` (Document Refinery)
- **Field:** `extracted_facts.confidence`

#### Step 3: Lineage Traversal - Find Producers

```python
def find_producers(lineage_graph, target_dataset, field_name):
    """
    Traverse edges backward to find who produces this data.
    
    Graph traversal logic:
    1. Start at the dataset node that contains the failing field
    2. Follow PRODUCES edges backward to find source files/functions
    3. Return all unique file paths
    """
    
    # Step 3a: Find dataset node for week3_extractions
    dataset_node = find_node_by_type(lineage_graph, "dataset::week3_extractions")
    
    # Step 3b: Find all incoming PRODUCES edges
    producers = []
    for edge in lineage_graph.edges:
        if edge['target'] == dataset_node['node_id']:
            if edge['relationship'] in ['PRODUCES', 'WRITES']:
                # Step 3c: Get the source node
                source_node = get_node(lineage_graph, edge['source'])
                if source_node['type'] == 'FILE':
                    producers.append(source_node['metadata']['path'])
                elif source_node['type'] == 'FUNCTION':
                    # Step 3d: Find file containing this function
                    function_file = find_function_file(lineage_graph, source_node['node_id'])
                    producers.append(function_file)
    
    # Step 3e: Result
    return producers  # ['src/week3/extractor.py']
```

**Result of traversal:**
```python
upstream_files = ['src/week3/extractor.py']
lineage_distance = 1  # One hop from dataset to producer
```

#### Step 4: Git Blame Integration

```python
def get_commit_blame(file_path, violation_timestamp, lineage_distance):
    """
    Find commits that modified the confidence field.
    """
    # Step 4a: Get git log for the file
    cmd = f'git log --follow --since="30 days ago" --format="%H|%an|%ae|%ai|%s" -- {file_path}'
    commits = subprocess.run(cmd, capture_output=True, text=True)
    
    # Step 4b: Parse commits
    blame_candidates = []
    for line in commits.stdout.strip().split('\n'):
        if '|' in line:
            hash_, author, email, timestamp, message = line.split('|', 4)
            
            # Step 4c: Find commits that modified confidence field
            if 'confidence' in message.lower() or 'scale' in message.lower():
                blame_candidates.append({
                    'commit_hash': hash_,
                    'author': author,
                    'commit_timestamp': timestamp,
                    'commit_message': message
                })
    
    # Step 4d: Score candidates
    v_time = datetime.fromisoformat(violation_timestamp)
    scored = []
    for rank, commit in enumerate(blame_candidates[:5], 1):
        c_time = datetime.fromisoformat(commit['commit_timestamp'])
        days_diff = abs((v_time - c_time).days)
        
        # Confidence formula: base - (days*0.1) - (lineage_distance*0.2)
        confidence = 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2)
        
        scored.append({
            **commit,
            'rank': rank,
            'confidence_score': max(0.0, round(confidence, 3))
        })
    
    return scored
```

#### Step 5: Compute Blast Radius

```python
def compute_blast_radius(lineage_graph, dataset_node):
    """
    Find all downstream consumers of the failing dataset.
    """
    # Step 5a: BFS traversal from dataset
    visited = set()
    queue = [dataset_node['node_id']]
    blast_radius = []
    
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        
        # Step 5b: Find all outgoing CONSUMES edges
        for edge in lineage_graph.edges:
            if edge['source'] == current and edge['relationship'] == 'CONSUMES':
                target_node = get_node(lineage_graph, edge['target'])
                blast_radius.append(target_node['metadata']['path'])
                queue.append(edge['target'])
    
    return blast_radius
```

**Result:**
```python
blast_radius = [
    'src/week4/cartographer.py',
    'src/week5/event_store.py'
]
```

#### Step 6: Generate Final Blame Chain

```json
{
  "violation_id": "viol-001",
  "check_id": "week3.extracted_facts.confidence.range",
  "detected_at": "2026-03-31T14:23:00Z",
  "blame_chain": [
    {
      "rank": 1,
      "file_path": "src/week3/extractor.py",
      "commit_hash": "a1b2c3d4e5f6g7h8i9j0",
      "author": "developer@example.com",
      "commit_timestamp": "2026-03-30T09:15:00Z",
      "commit_message": "feat: change confidence to percentage scale (0-100)",
      "confidence_score": 0.94
    },
    {
      "rank": 2,
      "file_path": "src/week3/models.py",
      "commit_hash": "b2c3d4e5f6g7h8i9j0k1",
      "author": "developer@example.com",
      "commit_timestamp": "2026-03-29T16:30:00Z",
      "commit_message": "refactor: update confidence field type",
      "confidence_score": 0.82
    }
  ],
  "blast_radius": {
    "affected_nodes": [
      "src/week4/cartographer.py",
      "src/week5/event_store.py"
    ],
    "affected_pipelines": [
      "week4-lineage-generation",
      "week5-event-ingestion"
    ],
    "estimated_records": 847
  }
}
```

---

## Question 4: LangSmith Trace Contract

### LangSmith Trace Schema (from Project Document)

```json
{
  "id": "uuid-v4",
  "name": "chain or LLM name",
  "run_type": "llm",
  "inputs": {},
  "outputs": {},
  "error": null,
  "start_time": "2025-01-15T14:23:00Z",
  "end_time": "2025-01-15T14:23:02Z",
  "total_tokens": 5090,
  "prompt_tokens": 4200,
  "completion_tokens": 890,
  "total_cost": 0.0153,
  "tags": ["week3", "extraction"],
  "parent_run_id": null,
  "session_id": "uuid-v4"
}
```

### Complete Bitol Contract

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Export Contract
  version: 1.0.0
  owner: week3-team
  description: |
    Contract for LangSmith trace exports used by the Document Refinery.
    Every LLM call must produce a trace with complete token usage and cost.

servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl

schema:
  # STRUCTURAL CLAUSES
  id:
    type: string
    format: uuid
    required: true
    description: "Unique trace identifier"
  
  name:
    type: string
    required: true
    minLength: 1
    description: "Name of the chain or LLM call"
  
  run_type:
    type: string
    required: true
    enum: ["llm", "chain", "tool", "retriever", "embedding"]
    description: "Type of run - must be one of the allowed values"
  
  start_time:
    type: string
    format: date-time
    required: true
    description: "ISO 8601 timestamp of run start"
  
  end_time:
    type: string
    format: date-time
    required: true
    description: "ISO 8601 timestamp of run end"
  
  total_tokens:
    type: integer
    required: true
    minimum: 0
    description: "Total tokens consumed (prompt + completion)"
  
  prompt_tokens:
    type: integer
    required: true
    minimum: 0
    description: "Input tokens sent to the model"
  
  completion_tokens:
    type: integer
    required: true
    minimum: 0
    description: "Output tokens generated by the model"
  
  total_cost:
    type: number
    required: true
    minimum: 0.0
    description: "Estimated cost in USD"
  
  # AI-SPECIFIC CLAUSE: Model version tracking
  model_version:
    type: string
    required: true
    pattern: "^(claude|gpt|gemini)-[0-9]+-[a-z0-9-]+$"
    description: "Model version used for this trace"

quality:
  type: SodaChecks
  specification:
    checks for traces:
      # STATISTICAL CLAUSE: Token sum integrity
      - valid: total_tokens = prompt_tokens + completion_tokens
        name: "token_sum_integrity"
        description: "Total tokens must equal sum of prompt and completion tokens"
      
      # STATISTICAL CLAUSE: Duration constraint
      - valid: end_time > start_time
        name: "duration_positive"
        description: "End time must be after start time"
      
      # STATISTICAL CLAUSE: Cost sanity
      - valid: total_cost > 0 OR error IS NOT NULL
        name: "cost_or_error"
        description: "Successful runs must have cost > 0; failed runs can have zero cost"
      
      # AI-SPECIFIC CLAUSE: Embedding drift (for runs with run_type=embedding)
      - metric: embedding_drift_score
        threshold: 0.15
        description: "Embedding centroid drift should not exceed 15%"
      
      # AI-SPECIFIC CLAUSE: Output schema violation rate
      - metric: output_schema_violation_rate
        threshold: 0.02
        description: "Less than 2% of LLM outputs should violate expected schema"
      
      # STATISTICAL CLAUSE: Token range sanity
      - min(prompt_tokens) >= 0
      - max(completion_tokens) <= 32000
        description: "Completion tokens should not exceed model context window"

# AI-SPECIFIC EXTENSIONS
ai_extensions:
  embedding_drift:
    enabled: true
    check: "embedding_drift_score"
    threshold: 0.15
    description: "Detects when semantic meaning of text has shifted"
    action: "WARN"
  
  prompt_input_schema:
    enabled: true
    schema_path: "schemas/prompt_input_schema.json"
    description: "Validates all prompt inputs before LLM call"
    quarantine_path: "outputs/quarantine/"
  
  structured_output:
    enabled: true
    schema_path: "schemas/llm_output_schema.json"
    description: "Validates LLM outputs against expected structure"
    violation_rate_threshold: 0.02

lineage:
  upstream:
    - id: week3-document-refinery
      description: "Traces generated during document extraction"
  downstream:
    - id: week7-ai-extensions
      description: "Consumed by AI Contract Extensions for drift detection"
      fields_consumed: [id, run_type, total_tokens, total_cost]
```

### Why This Contract Matters

| Clause Type | What It Catches | Real-World Impact |
|-------------|-----------------|-------------------|
| **Structural** | Missing `id`, invalid `run_type` | Prevents broken traces from entering analytics |
| **Statistical** | `total_tokens != prompt_tokens + completion_tokens` | Catches token counting bugs |
| **Statistical** | `end_time <= start_time` | Detects clock skew or corrupted data |
| **AI-Specific** | Embedding drift > 15% | Alerts when LLM outputs change meaning |
| **AI-Specific** | Output violation rate > 2% | Signals prompt degradation or model drift |

---

## Question 5: Contract Staleness - The Common Failure Mode

### The Most Common Failure Mode

**"Contract Rot"** - Contracts become stale because they are:
1. **Written once and never updated**
2. **Disconnected from actual data evolution**
3. **Not enforced at the right point in the pipeline**

### Why Contracts Get Stale

| Reason | Example from Your Systems | Consequence |
|--------|--------------------------|-------------|
| **Manual Maintenance** | Someone wrote `week3_contract.yaml` by hand, then forgot it | Contract describes old schema, new changes go undetected |
| **No Enforcement in CI** | Contract validation is a manual step before deployment | Engineers bypass it to "ship faster" |
| **No Schema Evolution Tracking** | No snapshots, so nobody knows when the schema changed | Breaking changes go unnoticed until production fails |
| **No Blast Radius Visibility** | Engineers don't know who consumes their data | They change schemas without understanding impact |
| **Statistical Drift Ignored** | Only structural checks enforced | Scale changes (0.0-1.0 → 0-100) pass validation |

### System Architecture
```bash
graph TB
    subgraph INPUTS["📥 INPUT SOURCES"]
        direction LR
        W1[Week 1: Intent Records<br/>intent_records.jsonl]
        W2[Week 2: Verdict Records<br/>verdicts.jsonl]
        W3[Week 3: Extraction Records<br/>extractions.jsonl]
        W4[Week 4: Lineage Graph<br/>lineage_snapshots.jsonl]
        W5[Week 5: Event Records<br/>events.jsonl]
        LS[LangSmith Traces<br/>runs.jsonl]
    end

    subgraph CORE["⚙️ CORE ENGINE"]
        direction TB
        
        subgraph GEN["Contract Generator"]
            CG[ContractGenerator]
        end
        
        subgraph VAL["Validation Engine"]
            VR[ValidationRunner]
            SE[SchemaEvolutionAnalyzer]
        end
        
        subgraph ATT["Attribution Engine"]
            VA[ViolationAttributor]
        end
        
        subgraph AI["AI Extensions"]
            AIX[AI Contract Extensions]
        end
        
        subgraph REP["Reporting Engine"]
            RG[ReportGenerator]
        end
    end

    subgraph OUTPUTS["📤 OUTPUT ARTIFACTS"]
        direction LR
        Y1[Contract YAML Files<br/>Bitol Format]
        Y2[dbt schema.yml<br/>Tests]
        Y3[Validation Reports<br/>JSON]
        Y4[Violation Log<br/>JSONL]
        Y5[Blame Chain<br/>JSON]
        Y6[Schema Snapshots<br/>YAML]
        Y7[Migration Reports<br/>JSON]
        Y8[AI Metrics<br/>JSON]
        Y9[Enforcer Report<br/>JSON + PDF]
    end

    subgraph EXTERNAL["🔗 EXTERNAL SYSTEMS"]
        direction LR
        GIT[Git Repository]
        LINEAGE[Week 4 Lineage Graph]
        LANG[LangSmith]
    end

    W1 --> CG
    W2 --> AIX
    W3 --> CG
    W4 --> CG
    W4 --> VA
    W5 --> CG
    LS --> AIX
    
    CG --> Y1
    CG --> Y2
    Y1 --> VR
    Y2 --> VR
    
    W3 --> VR
    W5 --> VR
    VR --> Y3
    VR --> SE
    
    Y3 --> VA
    VA --> Y4
    VA --> Y5
    
    SE --> Y6
    SE --> Y7
    
    W3 --> AIX
    W2 --> AIX
    LS --> AIX
    AIX --> Y8
    
    Y3 --> RG
    Y4 --> RG
    Y8 --> RG
    RG --> Y9
    
    GIT -.-> VA
    LINEAGE -.-> CG
    LINEAGE -.-> VA
    LANG -.-> AIX

    classDef input fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef core fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef output fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    classDef external fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    
    class W1,W2,W3,W4,W5,LS input
    class CG,VR,SE,VA,AIX,RG core
    class Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9 output
    class GIT,LINEAGE,LANG external

```
# How Architecture Prevents This
#### 1. Automatic Contract Generation (No Manual Maintenance)

```python
# Your ContractGenerator runs on every data update
python contracts/generator.py --source outputs/week3/extractions.jsonl

# Produces contracts that ALWAYS match current data structure
# No human to forget to update them
```

#### 2. Continuous Validation in CI/CD

```yaml
# .github/workflows/ci.yml
- name: Validate Contracts
  run: |
    python contracts/runner.py \
      --contract generated_contracts/week3_extractions.yaml \
      --data outputs/week3/extractions.jsonl
  # FAILS THE BUILD if contracts are violated
```

#### 3. Schema Snapshots with Evolution Tracking

```
schema_snapshots/week3-document-refinery-extractions/
├── 20260330_090000.yaml  # Before change
├── 20260331_090000.yaml  # After change (confidence scale)
└── diff_20260330_20260331.yaml  # Auto-generated diff
```

The SchemaEvolutionAnalyzer detects ANY change:

```json
{
  "change_type": "BREAKING",
  "field": "extracted_facts.confidence",
  "old": {"type": "number", "minimum": 0.0, "maximum": 1.0},
  "new": {"type": "integer", "minimum": 0, "maximum": 100},
  "impact": "All consumers expecting float 0.0-1.0 will break",
  "required_action": "Update all downstream systems before deployment"
}
```

#### 4. Blast Radius Analysis in Pre-Deploy

```python
# Before deploying schema change, engineers run:
python contracts/schema_analyzer.py --blast-radius

# Output:
# WARNING: Changing extracted_facts.confidence will affect:
#   - week4-cartographer (lineage generation)
#   - week5-event-store (event ingestion)
#   - week7-ai-extensions (embedding drift detection)
#   - 847 records in production
```

#### 5. Statistical Drift Detection (The Silent Killer)

```python
# Your ValidationRunner catches what structural checks miss
if current_mean > 1.0:  # Confidence is now in 0-100 scale
    return FAIL("Statistical drift detected: mean = 87.2 (expected < 1.0)")
```

#### 6. The Living Contract Pattern

Your architecture creates a **virtuous cycle**:

```
Data Changes → ContractGenerator Updates Contract → 
ValidationRunner Enforces → ViolationAttributor Traces → 
ReportGenerator Reports → Engineers Fix → Cycle Repeats
```

Instead of:
```
Data Changes → (No one updates contract) → 
Silent Failure → Production Incident → 
Emergency Fix → (Contract still stale)
```

### The Key Innovation: Contracts as Code

 system treats contracts as **executable specifications**:

```python
# Not just documentation
class Contract:
    def validate(self, data):
        # ACTUALLY CHECKS the data
        pass

    def detect_drift(self, baseline):
        # ACTUALLY DETECTS statistical changes
        pass

    def trace_violation(self, lineage):
        # ACTUALLY FINDS who broke it
        pass
```

Contracts aren't documents - they're **automated enforcement** that evolves with your system.

---

