# Physical AI Research Skills

This directory contains specialized Claude Code skills for the Physical AI Strategic Intelligence System (PASIS).

# Analysis Protocol

## 1. Input Validation
- 제공된 데이터에 source_url과 published_at이 포함되어 있는지 확인하십시오. [cite: 5]
- 정보의 선행 지표 여부(Leading Indicator)를 판단하여 우선순위를 지정하십시오.

## 2. Technical Taxonomy Mapping
- **Core Tech**: VLA, Foundation Models, Embodied AI [cite: 13]
- **Hardware**: Humanoid, Actuators, Edge Computing [cite: 13]
- **Business**: M&A, Investment, Strategic Partnership [cite: 10, 13]

## 3. LGU+ Strategic Filtering
- **Network & Infra**: 5G/6G 인프라 연계성 및 MEC 요구사항 분석.
- **B2B Service**: 산업용 로봇 관제 및 자동화 솔루션 연계성.
- **Portfolio Strategy**: 기술 획득(Build/Buy/Partner) 관점의 시사점 도출.

## 4. Reporting Structure (Pyramid Principle)
- **Executive Summary**: 한 문장으로 정의된 핵심 시사점. [cite: 14]
- **Strategic Implication**: LG유플러스에 주는 영향 및 대응 방안.
- **Next Steps**: 추가 리서치 또는 모니터링이 필요한 항목.


## Purpose
Custom skills designed for automated research tasks specific to Physical AI market intelligence:
- SEC filing analysis (10-K, 8-K)
- arXiv paper monitoring
- Industry news aggregation
- Strategic signal extraction

## Skill Development Guidelines
- Follow CLAUDE.md taxonomy and output schema
- Ensure all skills output standardized JSON with metadata
- Include confidence scoring for all extracted insights
- Maintain source traceability (URL, timestamp)

## Planned Skills
- `sec-filing-analyzer`: Parse and analyze SEC filings for Physical AI signals
- `arxiv-monitor`: Track latest robotics/embodied AI papers
- `market-signal-extractor`: Extract strategic implications from news/announcements
- `taxonomy-classifier`: Auto-classify signals into PASIS taxonomy
