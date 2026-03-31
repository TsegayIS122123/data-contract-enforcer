# Data Contract Enforcer

<div align="center">

![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-production_ready-brightgreen.svg)

**Enterprise-Grade Data Contract Enforcement System**

Automatically generate, validate, and enforce data contracts across microservices with statistical drift detection, lineage-based attribution, and AI-specific contract extensions.

</div>

---

## 📋 Overview

The Data Contract Enforcer solves the critical problem of silent data failures in production systems. When data producers change schemas without notifying consumers, systems continue running but produce wrong results. This system:

- **Automatically generates** contracts from existing data
- **Validates every record** against defined contracts
- **Detects structural and statistical violations** including hidden drift
- **Traces violations** to specific commits using lineage graphs
- **Reports blast radius** showing all affected downstream systems
- **Supports AI-specific contracts** for embeddings, prompts, and LLM outputs

### Key Features

| Feature | Description |
|---------|-------------|
| 🔍 **Auto-Contract Generation** | Generate Bitol-compatible YAML contracts from any JSONL dataset with 70%+ accuracy |
| 📊 **Statistical Drift Detection** | Catch silent corruption with z-score based drift detection (2σ warning, 3σ failure) |
| 🔗 **Lineage Attribution** | Trace violations to specific commits using Week 4 lineage graphs with confidence scoring |
| 🔄 **Schema Evolution Analysis** | Classify changes as backward/forward compatible with migration impact reports |
| 🤖 **AI Contract Extensions** | Embedding drift detection, prompt validation, and structured output enforcement |
| 📈 **Enforcer Report** | Auto-generated stakeholder reports with data health scores and plain-language recommendations |

---

## 🏗️ System Architecture

```mermaid
graph TD
    subgraph INPUTS["📥 INPUTS"]
        W1[Week 1<br/>Intent]
        W2[Week 2<br/>Verdict]
        W3[Week 3<br/>Extractions]
        W4[Week 4<br/>Lineage]
        W5[Week 5<br/>Events]
        LS[LangSmith<br/>Traces]
    end

    subgraph ENGINE["⚙️ ENGINE"]
        CG[Generator]
        VR[Runner]
        VA[Attributor]
        SEA[Analyzer]
        AI[AI Extensions]
        RG[Report Gen]
    end

    subgraph OUTPUTS["📤 OUTPUTS"]
        C[Contracts]
        V[Violations]
        R[Reports]
    end

    W1 --> CG
    W2 --> AI
    W3 --> CG
    W4 --> CG
    W5 --> CG
    LS --> AI
    
    CG --> C
    C --> VR
    W3 --> VR
    VR --> V
    V --> VA
    W4 --> VA
    VA --> R
    
    W3 --> AI
    W2 --> AI
    AI --> R
    
    VR --> RG
    V --> RG
    RG --> R

    style INPUTS fill:#e3f2fd
    style ENGINE fill:#fff3e0
    style OUTPUTS fill:#e8f5e9
```

# 📂 Project Structure
```bash
data-contract-enforcer/
├── contracts/                    # Core contract modules
│   ├── generator.py             # Auto-generates contracts from data
│   ├── runner.py                # Executes contract validation
│   ├── attributor.py            # Traces violations to commits
│   ├── schema_analyzer.py       # Analyzes schema evolution
│   ├── ai_extensions.py         # AI-specific contract checks
│   └── report_generator.py      # Generates stakeholder reports
│
├── generated_contracts/          # OUTPUT: Contract YAML files
│   ├── week3_extractions.yaml
│   ├── week5_events.yaml
│   └── langsmith_traces.yaml
│
├── validation_reports/           # OUTPUT: Validation results
│   ├── baseline.json
│   └── violated_run.json
│
├── violation_log/                # OUTPUT: Violation records
│   └── violations.jsonl
│
├── schema_snapshots/             # OUTPUT: Timestamped schemas
│   └── week3-document-refinery-extractions/
│       ├── 20250115_143000.yaml
│       └── 20250115_150000.yaml
│
├── enforcer_report/              # OUTPUT: Auto-generated reports
│   ├── report_data.json
│   └── report_20250115.pdf
│
├── outputs/                      # INPUT: Your week 1-5 data
│   ├── week1/intent_records.jsonl
│   ├── week2/verdicts.jsonl
│   ├── week3/extractions.jsonl
│   ├── week4/lineage_snapshots.jsonl
│   ├── week5/events.jsonl
│   ├── traces/runs.jsonl
│   └── quarantine/               # Quarantined invalid records
│
├── tests/                        # Unit and integration tests
│   ├── unit/
│   │   ├── test_generator.py
│   │   ├── test_runner.py
│   │   └── test_attributor.py
│   └── integration/
│       └── test_pipeline.py
│
├── config/                       # Configuration files
│   ├── contracts.yaml           # Default contract templates
│   └── settings.yaml            # System configuration
│
├── scripts/                      # Utility scripts
│   ├── setup.sh                 # Environment setup
│   ├── run_all.sh               # Run complete pipeline
│   └── inject_violation.py      # Inject test violations
│
├── docs/                         # Documentation
│   ├── api.md                   # API reference
│   ├── architecture.md          # System architecture
│   └── troubleshooting.md       # Common issues
│
├── .github/workflows/            # CI/CD pipelines
│   ├── test.yml                 # Run tests on PR
│   └── deploy.yml               # Deploy to production
│
├── .env.example                  # Environment variables template
├── .gitignore                    # Git ignore rules
├── requirements.txt              # Python dependencies
├── setup.py                      # Package installation
├── DOMAIN_NOTES.md               # Domain knowledge documentation
└── README.md                     # This file
```
---
## 📚 Phase 0 Complete: Domain Reconnaissance

Phase 0 established the foundational understanding required to build the Data Contract Enforcer. I've developed a complete mental model of data contracts, schema evolution, lineage attribution, and AI-specific contract requirements.

### ✅ What I Accomplished in Phase 0

#### 1. DOMAIN_NOTES.md - Complete Domain Analysis

Created a comprehensive 800+ word document answering all 5 critical questions with evidence from my actual Week 1-5 systems:

| Question | Answer Coverage |
|----------|-----------------|
| **Schema Evolution Taxonomy** | 3 backward-compatible + 3 breaking examples from my Week 1, 3, 5 schemas |
| **Confidence Scale Failure** | Full failure trace from Week 3 → Week 4 + Bitol contract clause |
| **Lineage Attribution** | Step-by-step graph traversal logic with actual code |
| **LangSmith Contract** | Complete Bitol YAML with structural, statistical, AI-specific clauses |
| **Contract Staleness** | Root cause analysis + how my architecture prevents it |

**Key Insights Documented:**
- How Week 3's `confidence` field change would break Week 4's Cartographer
- The exact graph traversal algorithm for finding blame chain
- Why statistical drift (z-score > 3) catches what structural checks miss
- How my architecture uses automatic generation + CI enforcement to prevent stale contracts

#### 2. System Architecture Diagrams

Created 9 complete Mermaid diagrams visualizing the entire system:

| Diagram | Purpose |
|---------|---------|
| **Complete System Architecture** | All components, inputs, outputs, and external dependencies |
| **Component Flow Diagram** | 5-phase pipeline from contract generation to reporting |
| **Component Interaction Sequence** | Time-based flow of all interactions |
| **Data Flow Architecture** | How data moves through processing layers |
| **Violation Attribution Flow** | Detailed step-by-step of finding the guilty commit |
| **Statistical Drift Detection** | How z-score catches scale changes |
| **AI Contract Extensions** | Embedding drift, prompt validation, output enforcement |
| **Report Generator Flow** | How health score and recommendations are computed |
| **Schema Evolution Analysis** | How breaking changes are detected and classified |
#### 3. Data Flow Analysis

Mapped all inter-system data flows between my Week 1-5 systems:

#### 4. Contract Coverage Strategy

Identified priority contracts based on risk:

| Priority | Contract | Why |
|----------|----------|-----|
| **Critical** | Week 3 `confidence` range | Scale change would cause silent corruption |
| **Critical** | Week 5 `sequence_number` monotonic | Concurrency issues in event store |
| **High** | Week 2 `overall_verdict` enum | Invalid verdicts break downstream decisions |
| **High** | LangSmith `total_tokens = prompt+completion` | Token counting bugs affect cost tracking |
| **Medium** | Week 4 `node_id` reference integrity | Broken lineage graph breaks attribution |

---

## 🏗️ System Architecture (Phase 0 Design)

```mermaid
graph TB
    subgraph INPUTS["📥 Week 1-5 Outputs"]
        W1[Intent Records]
        W2[Verdict Records]
        W3[Extraction Records]
        W4[Lineage Graph]
        W5[Event Records]
        LS[LangSmith Traces]
    end

    subgraph CORE["⚙️ Core Engine"]
        CG[ContractGenerator]
        VR[ValidationRunner]
        VA[ViolationAttributor]
        SE[SchemaEvolutionAnalyzer]
        AI[AI Contract Extensions]
        RG[ReportGenerator]
    end

    subgraph OUTPUTS["📤 Artifacts"]
        Y1[Contract YAML]
        Y2[Validation Reports]
        Y3[Violation Log]
        Y4[Blame Chain]
        Y5[Migration Reports]
        Y6[AI Metrics]
        Y7[Enforcer Report]
    end

    W1 --> CG
    W2 --> AI
    W3 --> CG
    W4 --> CG
    W4 --> VA
    W5 --> CG
    LS --> AI
    
    CG --> Y1
    Y1 --> VR
    W3 --> VR
    VR --> Y2
    VR --> VA
    VA --> Y3
    VA --> Y4
    
    Y2 --> SE
    SE --> Y5
    
    W3 --> AI
    LS --> AI
    AI --> Y6
    
    Y2 --> RG
    Y3 --> RG
    Y6 --> RG
    RG --> Y7

    classDef input fill:#e3f2fd,stroke:#1976d2
    classDef core fill:#fff3e0,stroke:#f57c00
    classDef output fill:#e8f5e9,stroke:#388e3c
    
    class W1,W2,W3,W4,W5,LS input
    class CG,VR,VA,SE,AI,RG core
    class Y1,Y2,Y3,Y4,Y5,Y6,Y7 output
```
## 🚀 Quick Start

### Prerequisites

```bash
# Python 3.11+
python --version

# Install required packages
pip install pandas numpy scikit-learn pyyaml jsonschema openai anthropic langsmith gitpython ydata-profiling soda-core

# Set up environment variables
cp .env.example .env
# Edit .env with your API keys
```
# 🙏 Acknowledgments
- Bitol Open Data Contract Standard
- Confluent Schema Registry
- dbt for contract testing patterns
- LangSmith for LLM tracing

# 📞 Contact
Tsegay - tsegayassefa27@gmail.com

Project Link: https://github.com/TsegayIS122123/data-contract-enforcer