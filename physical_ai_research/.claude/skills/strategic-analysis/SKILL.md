# Strategic Analysis

**Purpose**: Extract strategic implications from collected Physical AI signals for LG Uplus portfolio strategy.

# Instructions
- **Analysis Standard**: 피라미드 원칙(Conclusion → Evidence → Actions)에 따라 결론 우선으로 작성하십시오.
- **LGU+ Relevance Filter**: 모든 분석에서 LG유플러스 관점의 시사점(Direct Impact, Future Opportunity, Competitive Threat, Partnership Potential)을 명시하십시오.
- **Confidence Scoring**: 신뢰도 점수(0.0-1.0)를 산출하되, 1차 자료(SEC, arXiv) > 2차 자료(뉴스)로 가중치를 부여하십시오.
- **Frameworks**: 분석 유형별 프레임워크(Competitive/Tech Trend/Investment/Regulatory)를 명시적으로 적용하십시오.
- **Target Companies**: Tesla, Figure AI, Boston Dynamics, NVIDIA, Amazon 등 핵심 기업에 집중하십시오.

## Skill Overview
Analyzes raw data from `physical-ai-scout` to derive actionable insights:
- Competitive landscape mapping
- Technology trend identification
- Investment/M&A signal detection
- Regulatory impact assessment

## Input Parameters
```json
{
  "signals": [
    {
      "event_id": "uuid",
      "scope": "Market|Tech|Case|Policy",
      "raw_content": "...",
      "source_metadata": {...}
    }
  ],
  "analysis_type": "competitive|tech_trend|investment|regulatory",
  "focus_companies": ["Tesla", "Figure AI", "..."]
}
```

## Output Schema
Enhanced PASIS output with strategic layer:
```json
{
  "event_id": "uuid-v4",
  "scope": "Market|Tech|Case|Policy",
  "category": "Taxonomy classification",
  "title": "Concise strategic title",
  "summary": "Conclusion-first summary (3-5 sentences)",
  "strategic_implication": "LGU+ relevance and recommended actions",
  "key_insights": [
    "Insight 1: Technology maturity assessment",
    "Insight 2: Market timing window",
    "Insight 3: Partnership opportunities"
  ],
  "confidence_score": 0.85,
  "source_metadata": {...}
}
```

## Analysis Frameworks

### 1. Competitive Intelligence
```python
def analyze_competitive_landscape(
    signals: list[dict],
    target_companies: list[str]
) -> dict:
    """
    Extract competitive positioning insights.

    Returns:
    - Market share trends
    - Technology differentiation points
    - Strategic partnerships
    - Investment rounds & valuations
    """
```

**Example Output:**
- "Figure AI raised $675M (Feb 2024) → Valuation $2.6B → Humanoid robotics market heating up"
- "Tesla FSD v12 uses end-to-end neural nets → Shift from rule-based to foundation model approach"

### 2. Technology Trend Detection
```python
def identify_tech_trends(
    papers: list[dict],
    window_months: int = 6
) -> dict:
    """
    Detect emerging technology patterns from research papers.

    Returns:
    - Trending keywords (with citation velocity)
    - Novel architectures (VLA, World Models)
    - Benchmark improvements
    """
```

**Example Output:**
- "VLA (Vision-Language-Action) models: 15 papers in Q1 2024 vs 3 in Q4 2023 → 5x growth"
- "Humanoid locomotion: Sim-to-real transfer now <10% performance gap (ICRA 2024)"

### 3. Investment Signal Extraction
```python
def extract_investment_signals(
    sec_filings: list[dict],
    news: list[dict]
) -> dict:
    """
    Identify M&A, funding, partnerships.

    Returns:
    - Deal size & structure
    - Strategic rationale (from filings)
    - Market reaction (if public)
    """
```

**Example Output:**
- "Amazon acquires iRobot ($1.7B, 8-K filed Aug 2022) → Expanding home robotics"
- "NVIDIA invests in Sanctuary AI (Series A, $30M) → Bet on general-purpose robots"

### 4. Regulatory Impact Assessment
```python
def assess_regulatory_impact(
    policy_docs: list[dict],
    regions: list[str] = ["EU", "US", "KR"]
) -> dict:
    """
    Analyze policy implications for Physical AI deployment.

    Returns:
    - Compliance requirements
    - Timeline & deadlines
    - Business impact (cost, restrictions)
    """
```

**Example Output:**
- "EU AI Act (June 2024 final): Physical AI = High-risk → Mandatory conformity assessment"
- "NIST AI RMF: Voluntary framework → Early adoption = competitive advantage"

## Strategic Implication Guidelines

### LGU+ Relevance Framework
For each signal, assess:
1. **Direct Impact**: Does this affect LGU+ current business?
2. **Future Opportunity**: New product/service possibilities?
3. **Competitive Threat**: Telcos/partners moving into this space?
4. **Partnership Potential**: Collaboration opportunities?

### Output Format (Pyramid Principle)
```
1. Conclusion (1 sentence)
   - What happened and why it matters to LGU+

2. Supporting Evidence (2-3 bullets)
   - Key data points from source
   - Market context

3. Recommended Actions (1-2 bullets)
   - Immediate next steps
   - Further research needed
```

**Example:**
```
Conclusion: Figure AI's BMW partnership (Jan 2024) validates commercial readiness
of humanoid robots in manufacturing → LGU+ should explore robotics-as-a-service
models for enterprise 5G use cases.

Evidence:
- Figure 01 deployed at BMW Spartanburg plant for automotive assembly tasks
- 5G connectivity required for real-time robot fleet coordination
- Market size: $38B humanoid robotics by 2035 (Goldman Sachs)

Actions:
- Immediate: Contact Figure AI Korea team for partnership discussion
- Research: Study telco+robotics convergence (AT&T, Verizon models)
```

## Quality Checks
- ✅ Summary starts with conclusion (not background)
- ✅ Strategic implication specific to LGU+ (not generic)
- ✅ Confidence score justified with source quality
- ✅ Actionable next steps included

## Example Usage
```bash
claude-skill strategic-analysis \
  --input signals.json \
  --analysis-type "investment" \
  --focus-companies "Figure AI,Tesla,Boston Dynamics" \
  --output strategic-brief.json
```

## Integration Points
- Consumes from: `physical-ai-scout` (raw signals)
- Feeds into: `data-archivist` (for PostgreSQL storage)
- Requires: Claude API (for LLM-based insight extraction)