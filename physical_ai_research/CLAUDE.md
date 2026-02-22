# CLAUDE.md: Physical AI Strategic Intelligence System (PASIS)

## 1. Project Overview
- Purpose: LG Uplus Portfolio Strategy Team's Physical AI research automation system.
- Objective: Collect and analyze global market signals (disclosures, papers, news) to derive strategic implications.
- Core Values: Primary source authority, traceability, and actionable insights.

## 2. Technical Standards & Constraints
### General & Python
- Type Hinting: Mandatory for all functions and methods.
- Error Handling: Specific exception handling with detailed logging.
- Metadata: All records must include {source_url, published_at, scraped_at, confidence_score}.

### SQL (BigQuery/SQLX)
- Standard: No SELECT *. Explicit column names only.
- Logic: Use QUALIFY for deduplication. Use SAFE_ functions.
- Partitioning: Mandatory explicit partitioning.

### Airflow & Orchestration
- Structure: Strict separation of orchestration logic from business logic.
- Idempotency: No now(). Use logical_date for partitioning.

## 3. Research Scope & Taxonomy
### Data Scopes
| Scope | Target Data | Key Companies |
| :--- | :--- | :--- |
| Market Signal | 10-K, 8-K, IR Reports, M&A | Tesla, NVIDIA, Amazon |
| Tech Frontier | arXiv, ICRA, IROS, CVPR | Figure AI, OpenAI, Boston Dynamics |
| Real-world Case | PoC, Partnerships, Keynotes | Agility Robotics, Gatik |
| Policy/Standard | EU AI Act, NIST, IFR | Global Regulators |

### Strategic Keywords
- Core Tech: Embodied AI, World Models, VLA Models, Foundation Models for Robotics
- Hardware: Humanoid, Actuator Control, End-to-End Robotics, Edge AI Hardware
- Business: Strategic Investment, M&A, PoC, Commercial Deployment
- Ops: Sim-to-Real, Digital Twins, Robot Fleet Management

## 4. Output Schema
All processed data must follow this JSON structure:
```json
{
  "event_id": "uuid",
  "scope": "Market|Tech|Case|Policy",
  "category": "Taxonomy match",
  "title": "string",
  "summary": "Conclusion-first summary",
  "strategic_implication": "LGU+ relevance",
  "source_metadata": {
    "url": "url",
    "publisher": "string",
    "published_at": "timestamp",
    "confidence_score": "0.0-1.0"
  }
}
```

## 5. Communication Style
- Tone: Professional, dry, and concise.
- Logic: Pyramid Principle (Conclusion -> Supporting Data -> Next Steps).
- Language: Business Korean (Keep technical terms in English where appropriate).