# Physical AI Strategic Intelligence System (PASIS)

LG Uplus 포트폴리오 전략팀의 Physical AI 리서치 자동화 시스템

## 📁 프로젝트 구조

```
physical_ai_research/
├── CLAUDE.md              # 프로젝트 가이드라인 (Taxonomy, 기술 표준)
├── .claude/
│   └── skills/           # Physical AI 전용 스킬들
│       └── README.md     # 스킬 개발 가이드
└── README.md             # 이 파일
```

## 🎯 프로젝트 목표

글로벌 Physical AI 시장의 전략적 신호를 수집/분석하여 실행 가능한 인사이트 도출

### 리서치 범위
1. **Market Signal**: SEC 공시(10-K, 8-K), IR 보고서, M&A
   - Target: Tesla, NVIDIA, Amazon, Figure AI
2. **Tech Frontier**: arXiv, 학회 논문(ICRA, IROS, CVPR)
   - Target: OpenAI, Boston Dynamics, Embodied AI Labs
3. **Real-world Case**: PoC, 파트너십, 상용화 사례
   - Target: Agility Robotics, Gatik, 산업 현장 도입
4. **Policy/Standard**: 규제, 표준, 가이드라인
   - Target: EU AI Act, NIST, IFR

## 🔑 핵심 키워드

**Core Tech**: Embodied AI, World Models, VLA Models, Foundation Models for Robotics
**Hardware**: Humanoid, Actuator Control, End-to-End Robotics, Edge AI Hardware
**Business**: Strategic Investment, M&A, PoC, Commercial Deployment
**Ops**: Sim-to-Real, Digital Twins, Robot Fleet Management

## 📊 데이터 스키마

모든 수집 데이터는 아래 JSON 구조를 따름:

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

## 🛠️ 기술 스택 (예정)

- **Data Collection**: SEC EDGAR API, arXiv API, Web Scraping
- **Storage**: BigQuery (partitioned by date)
- **Orchestration**: Airflow
- **Analysis**: Claude API (structured extraction)
- **Reporting**: Automated summaries, strategic briefs

## 📖 사용 가이드

모든 개발은 `CLAUDE.md`의 기술 표준을 준수:
- Type hinting 필수
- 명시적 예외 처리
- 멱등성 보장
- 소스 메타데이터 필수 포함

## 🚀 다음 단계

- [ ] SEC EDGAR API 연동 스크립트
- [ ] arXiv 논문 모니터링 파이프라인
- [ ] BigQuery 스키마 설계
- [ ] Taxonomy 기반 자동 분류 로직
- [ ] 주간/월간 리포트 자동 생성
