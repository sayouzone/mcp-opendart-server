# DART 오픈API

패키지 구조

```
opendart/
├── __init__.py          # 공개 API 정의
├── client.py            # OpenDART HTTP 클라이언트
├── models.py            # 데이터 클래스 (DTO)
├── utils.py             # 유틸리티 함수 & 상수
├── crawler.py           # 통합 인터페이스 (Facade)
├── examples.py          # 사용 예시
└── parsers/
    ├── __init__.py
    ├── document.py        # 문서 API 파서
    ├── document_viewer.py # 문서 뷰어 API 파서
    ├── disclosure.py      # 공시정보 API 파서
    ├── finance.py         # 정기보고서 재무정보 API 파서
    ├── material_facts.py  # 주요사항보고서 주요정보 API 파서
    ├── ownership.py       # 지분공시 종합정보 API 파서
    ├── registration.py    # 증권신고서 주요정보 API 파서
    └── reports.py         # 정기보고서 주요정보 API 파서
```

```
OpenDartCrawler
├── DartDocumentViewer (DART 문서 뷰어 클래스)
│   ├── fetch (공시 정보로부터 문서 정보 가져오기)
│   └── ...
├── DartDocumentParser (DART 문서 파싱 클래스)
│   ├── fetch (공시 번호로부터 첨부파일 목록을 가져오기)
│   ├── download (현재 종목의 시가총액을 스크래핑하여 숫자로 반환)
│   └── parse (...)
├── DartDisclosureConnector (OpenDART 공시정보 API 수집 클래스)
│   ├── list (공시검색, 공시 유형별, 회사별, 날짜별 등 여러가지 조건으로 공시보고서 검색기능을 제공합니다.)
│   ├── company (기업개황, 전반적인 상황, DART에 등록되어있는 기업의 개황정보를 제공합니다.)
│   ├── document (공시서류원본파일, 공시보고서 원본파일을 제공합니다.)
│   └── corp_code (고유번호, DART에 등록되어있는 공시대상회사의 고유번호,회사명,종목코드, 최근변경일자를 파일로 제공합니다.)
├── DartFinanceConnector (OpenDART 정기보고서 재무정보 API 수집 클래스)
│   ├── finance ()
│   └── finance_file ()
...
├── DartAPIParser (OpenDART API 파싱 클래스)
│   ├── list (공시검색, 공시 유형별, 회사별, 날짜별 등 여러가지 조건으로 공시보고서 검색기능을 제공합니다.)
│   ├── company (기업개황, 전반적인 상황, DART에 등록되어있는 기업의 개황정보를 제공합니다.)
│   ├── document (공시서류원본파일, 공시보고서 원본파일을 제공합니다.)
│   ├── corp_code (고유번호, DART에 등록되어있는 공시대상회사의 고유번호,회사명,종목코드, 최근변경일자를 파일로 제공합니다.)
├── ...
├── check_gcp (GCP에서 Caching 정보 확인)
└── save_gcp (GCP에서 Caching 정보 저장)
```

- 공시정보: Public Disclosure, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
- 정기보고서 주요정보: Key Information in Periodic Reports, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002
- 정기보고서 재무정보: Financial Information in Periodic Reports, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS003
- 지분공시 종합정보: Comprehensive Share Ownership Information, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS004
- 주요사항보고서 주요정보: Key Information in Reports on Material Facts, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005
- 증권신고서 주요정보: Key Information in Registration Statements, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS006


주요 개선 사항
| 개선 항목 | 설명 |
|---------|------|
| 모듈 분리 | 파일링 타입별로 파서를 별도 모듈로 분리 |
| Facade 패턴 | `EDGARCrawler`가 모든 파서를 통합 관리 |
| 데이터 모델 | `@dataclass`를 별도 모듈로 분리 |
| 유틸리티 | 공통 함수/상수를 `utils.py`로 추출 |
| HTTP 클라이언트 | API 호출 로직을 `EDGARClient`로 분리 |
| 레거시 호환 | 기존 메서드명 유지 (하위 호환성) |


```python
crawler = EDGARCrawler(user_agent="Sayouzone sjkim@sayouzone.com")
cik = crawler.fetch_cik_by_ticker("AAPL")
filings = crawler.fetch_filings(cik, doc_type="10-K", count=1)
data = crawler.extract_10k(cik, filings[0].document_url, filings[0].accession_number)
```