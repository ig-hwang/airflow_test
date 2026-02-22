# Physical AI Scout

**Purpose**
- Automated data collection from primary sources for Physical AI market intelligence.
- 해외 공시(SEC), 논문(arXiv), 뉴스룸에서 피지컬 AI 데이터를 수집합니다.

# Instructions
- **Data Acquisition Standard**: 수집 시 반드시 source_url, publisher, published_at을 포함한 메타데이터를 확보하십시오. [cite: 3, 5]
- **Target Domains**: search 도구 사용 시 아래 도메인을 우선순위로 설정하십시오.
    - 기업 공시: site:sec.gov [cite: 10]
    - 학술 논문: site:arxiv.org, site:semanticscholar.org [cite: 11]
    - 산업 뉴스: site:techcrunch.com, site:reuters.com
- **Keyword Strategy**: Embodied AI, VLA Models, Humanoid Robotics 등 전략 키워드를 조합하십시오. [cite: 13]
- **Storage**: 수집된 로우 데이터는 data/raw/ 디렉토리에 {YYYYMMDD}_{Source}.json 형식으로 저장하십시오.

## Skill Overview
Monitors and collects strategic signals from multiple sources:
- SEC EDGAR filings (10-K, 8-K, S-1)
- arXiv preprints (cs.RO, cs.AI, cs.CV)
- Corporate announcements (IR, press releases)
- Conference proceedings (ICRA, IROS, CVPR, RSS)

## Input Parameters
```json
{
  "scope": "Market|Tech|Case|Policy",
  "keywords": ["list", "of", "keywords"],
  "companies": ["Tesla", "Figure AI", "..."],
  "date_range": {
    "start": "YYYY-MM-DD",
    "end": "YYYY-MM-DD"
  },
  "sources": ["SEC", "arXiv", "News"]
}
```

## Output Schema
Follows PASIS standard output format:
```json
{
  "event_id": "uuid-v4",
  "scope": "Market|Tech|Case|Policy",
  "category": "Taxonomy classification",
  "title": "Original title from source",
  "raw_content": "Full text or excerpt",
  "source_metadata": {
    "url": "Direct link to primary source",
    "publisher": "SEC/arXiv/Publisher name",
    "published_at": "ISO-8601 timestamp",
    "scraped_at": "ISO-8601 timestamp",
    "confidence_score": 0.95
  }
}
```

## Key Functions

### 1. SEC Filing Monitor
```python
def fetch_sec_filings(
    companies: list[str],
    form_types: list[str] = ["10-K", "8-K"],
    start_date: str,
    end_date: str
) -> list[dict]:
    """
    Fetch SEC filings using EDGAR API.
    Returns: List of filing metadata with direct URLs.
    """
```

### 2. arXiv Paper Tracker
```python
def fetch_arxiv_papers(
    categories: list[str] = ["cs.RO", "cs.AI"],
    keywords: list[str],
    max_results: int = 50
) -> list[dict]:
    """
    Query arXiv API for recent papers matching keywords.
    Returns: Parsed paper metadata with abstracts.
    """
```

### 3. News Aggregator
```python
def fetch_news_articles(
    companies: list[str],
    keywords: list[str],
    sources: list[str] = ["reuters", "techcrunch"]
) -> list[dict]:
    """
    Aggregate news from multiple sources.
    Returns: Deduplicated articles with metadata.
    """
```

## Technical Requirements
- **Rate Limiting**: Respect API limits (SEC: 10 req/sec, arXiv: 1 req/3sec)
- **Error Handling**: Retry with exponential backoff
- **Deduplication**: Hash-based content matching
- **Metadata**: Always include source URL, timestamps, confidence score

## Quality Checks
- ✅ Valid source URL (not 404)
- ✅ Published date within search range
- ✅ Contains at least one target keyword
- ✅ Metadata completeness score > 0.8

## Example Usage
```bash
claude-skill physical-ai-scout \
  --scope "Tech" \
  --keywords "humanoid,embodied-ai,world-model" \
  --sources "arXiv,SEC" \
  --output signals.json
```

## Integration Points
- Feeds into: `strategic-analysis` (for insight extraction)
- Feeds into: `data-archivist` (for storage)
- Requires: API keys (SEC EDGAR, news APIs)