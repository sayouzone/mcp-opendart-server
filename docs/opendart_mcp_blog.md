# OpenDart MCP Server 구축

`sayou-stock`을 이용하여 OpenDart API를 호출하는 MCP 서버를 구축합니다.<br/>
`OpenDart`에서 종목 기본 정보 (`공시정보`, `정기보고서 주요정보`, `정기보고서 재무정보`, `지분공시 종합정보`, `주요사항보고서 주요정보`, `증권신고서 주요정보`)를 가져옵니다.

MCP 서버는 LLM에 외부 도구 및 서비스에 대한 액세스 권한을 제공합니다.<br/>
FastMCP를 사용하여 MCP 서버와 클라이언트를 빌드하는 빠르고 Pythonic한 방법을 제공합니다.<br/>

- Gemini 2.5 Pro
- Gemini 2.5 Flash

**OpenDart API**
- [공시정보](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001)
- [정기보고서 주요정보](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002)
- [정기보고서 재무정보](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS003)
- [지분공시 종합정보](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS004)
- [주요사항보고서 주요정보](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005)
- [증권신고서 주요정보](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS006)

![OpenDart MCP Server Build Process](https://storage.googleapis.com/sayouzone-homepage/blog/opendart_mcp_sesrver_build_process.jpg)

## 기본 요구사항

터미널에서 `gcloud` 명령어를 사용하여 Google Cloud Platform의 서비스를 활성화하고 서비스 계정을 생성할 수 있어야 합니다.

#### 서비스 활성화 및 서비스 계정 생성

**서비스 활성화**

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

**서비스 계정 생성**

```bash
gcloud iam service-accounts create mcp-server-sa --display-name="MCP Server Service Account"
```

#### 소스 코드 설치

```bash
git clone https://github.com/sayouzone/mcp-opendart-server.git
```

```bash
cd mcp-opendart-server
```

`pyproject.toml` 파일을 확인합니다.

**pyproject.toml**

```
[project]
name = "opendart-mcp"
version = "0.1.0"
description = "Deploying an OpenDart MCP server on Cloud Run"
requires-python = ">=3.11"
dependencies = [
    "fastmcp==2.12.4",
    "beautifulsoup4>=4.14.0",
    "pandas==2.3.3",
    "xmltodict==1.0.2",
    "sayou-stock>=0.1.0",
    "google-cloud-secret-manager==2.25.0",
    "lxml==6.0.2",
]
```

`opendarts.py` 파일에서 라이브러리 import를 확인합니다. 
또한 `logger`를 설정합니다.

```python
import json
import logging
import os
import pandas as pd

from datetime import datetime
from fastmcp import FastMCP
from pathlib import Path
from typing import Optional

from google.cloud import secretmanager

from sayou.stock.opendart import OpenDartCrawler

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(levelname)s]: %(message)s", level=logging.INFO)
```

Google Secret Manager를 사용하여 DART API Key를 가져옵니다.

```python
sm_client = secretmanager.SecretManagerServiceClient()
name = "projects/1037372895180/secrets/DART_API_KEY/versions/latest"
response = sm_client.access_secret_version(name=name)
dart_api_key = response.payload.data.decode("UTF-8")
os.environ["DART_API_KEY"] = dart_api_key
```

OpenDart에서 기업 코드에 대한 JSON 파일명입니다.

OpenDartCrawler를 초기화하고, 
OpenDart에서 API로 기업코드를 가져옵니다.
기업코드를 이용하여 회사명 또는 주식 코드로 corp_code를 가져옵니다.

```python
corpcode_filename = "corpcode.json"

# OpenDartCrawler를 초기화
crawler = OpenDartCrawler(api_key=dart_api_key)
corp_data = crawler.corp_data
crawler.save_corp_data(corpcode_filename)
```

```python
@mcp.tool(
    name="find_opendart_finance",
    description="""OpenDART에서 한국 주식 재무제표 수집 (yfinance와 동일한 스키마).
    사용 대상:
    - 6자리 숫자 티커: 005930, 000660
    - .KS/.KQ 접미사: 005930.KS, 035720.KQ
    - 한국 기업명: 삼성전자, SK하이닉스

    반환: {
        "ticker": str,
        "country": "KR",
        "balance_sheet": str | None,      # JSON 문자열
        "income_statement": str | None,   # JSON 문자열
        "cash_flow": str | None           # JSON 문자열
    }

    참고: 캐시를 우선 사용하여 빠른 응답을 제공합니다.
    크롤링은 최대 60초 이상 소요될 수 있으므로 가능한 캐시를 활용합니다.
    """,
    tags={"opendart", "fundamentals", "korea", "standardized", "cached"}
)
def find_opendart_finance(stock: str, year: Optional[int] = None, quarter: Optional[int] = None):
    """
    OpenDART에서 한국 주식 재무제표 3종을 수집합니다.

    yfinance와 동일한 스키마를 반환하여 LLM 에이전트가
    한국 주식과 해외 주식을 동일한 방식으로 처리할 수 있습니다.

    Args:
        stock: 종목 코드 (예: "005930", "삼성전자")

    Returns:
        dict: 재무제표 3종 (yfinance와 동일한 스키마)

    Note:
        - use_cache=True (기본값): GCS에서 캐시된 데이터를 먼저 확인 (빠름)
        - use_cache=False: 항상 새로 크롤링 (느림, 30초+ 소요)
    """
    logger.info(f">>> 🛠️ Tool: 'find_opendart_finance' called for '{stock}'")
```

OpenDart에서 API으로 가져올 연도 및 분기를 확인합니다.

```python
    # 년도와 분기 정보가 있는지 확인하고, 
    # 없으면 오늘 기준으로 이전 분기를 설정합니다.
    is_date = year is not None and quarter is not None
    year, quarter = _year_quarter(year, quarter)
```

OpenDart에서 일부 API는 주식 코드를 통해서 정보를 조회할 수 있지만,
대부분의 API는 corp_code를 통해서 정보를 조회할 수 있습니다.

```python
    # OpenDartCrawler를 초기화
    crawler = OpenDartCrawler(api_key=dart_api_key)
    corp_data = crawler.corp_data
    crawler.save_corp_data(corpcode_filename)

    corp_code = crawler.fetch_corp_code(stock)
```

`@mcp.prompt()`으로 커스텀 명렬어를 자동으로 생성합니다.</br>

```python
@mcp.prompt()
def dividend(stock: str, year: Optional[int] = None, quarter: Optional[int] = None):
    """Find dividend information of a company."""

    return (
        f"{stock}의 {year}년 {quarter}분기 배당 정보를 찾았습니다."
        f"문서번호, 기업코드, 기업명, 당기순이익(백만원), 현금배당수익률(%), 주당순이익(원), 주당 현금배당금(원), 현금배당성향(%), 현금배당금총액(백만원)을 응답합니다."
    )
```

`/dividend --stock=삼성전자 --year=2025` 또는 `/dividend 삼성전자 --year=2025` 명령어를 실행합니다.

출력 결과물은 `문서번호`, `기업코드`, `기업명`, `당기순이익(백만원)`, `현금배당수익률(%)`, `주당순이익(원)`, `주당 현금배당금(원)`, `현금배당성향(%)`, `현금배당금총액(백만원)` 포함해서 출력합니다.

**출력 예시**

```bash
✦ 삼성전자의 2025년 배당 정보입니다.

   * 문서번호: 20251114002447
   * 기업코드: 00126380
   * 기업명: 삼성전자
   * 당기순이익(백만원): 24,968,902
   * 현금배당수익률(%): 1.30
   * 주당순이익(원): 3,724
   * 주당 현금배당금(원): 1,102
   * 현금배당성향(%): 29.50
   * 현금배당금총액(백만원): 7,354,422
```

## MCP 서버 배포 (Deploy MCP Server on Cloud Run)

```bash
MCP_SERVER_NAME=opendart-mcp-server
export GOOGLE_CLOUD_PROJECT=sayouzone-ai
```

**서비스 계정에 권한 부여**

```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=serviceAccount:mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --role="roles/secretmanager.secretAccessor"
```

#### 배포

**패키지 빌드 및 배포**

`--no-allow-unauthenticated` 옵션을 사용하여 인증을 요구합니다. 

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --service-account=mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp
```

**패키지 소스 및 접근권한 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp
```

## Tests

MCP 서버를 배포했다면 이제 Gemini CLI를 사용하여 MCP 서버를 테스트할 수 있습니다.

#### Gemini CLI 설치

npx로 Gemini CLI를 설치하지 않고 실행합니다.

```bash
# Using npx (no installation required)
npx https://github.com/google-gemini/gemini-cli
```

Windows에서 npm을 사용하여 설치합니다.

```cmd
npm install -g @google/gemini-cli
```

MacOS 또는 Linux에서 Homebrew를 사용하여 설치합니다.

```bash
brew install gemini-cli
```

사용자 계정으로 MCP 서버에 대한 접근 권한을 추가합니다.

```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=user:$(gcloud config get-value account) \
    --role='roles/run.invoker'
```

**참조 문서**
- [Gemini CLI](https://github.com/google-gemini/gemini-cli)
- [Gemini CLI Custom Slash Commands](https://cloud.google.com/blog/topics/developers-practitioners/gemini-cli-custom-slash-commands?e=48754805)

#### Gemini CLI 테스트

Gemini 설정 파일에서 사용할 수 있도록 Google Cloud 사용자 인증 정보와 프로젝트 번호를 환경 변수로 설정합니다.

```bash
export PROJECT_NUMBER=$(gcloud projects describe $GOOGLE_CLOUD_PROJECT --format="value(projectNumber)")
export ID_TOKEN=$(gcloud auth print-identity-token)
```

settings.json

```json
{
    "ide": {
        "hasSeenNudge": true
    },
    "mcpServers": {
        "opendart-remote": {
            "httpUrl": "https://opendart-mcp-server-$PROJECT_NUMBER.us-central1.run.app/mcp",
            "headers": {
                "Authorization": "Bearer $ID_TOKEN"
            }
        }
    },
    "security": {
        "auth": {
            "selectedType": "cloud-shell"
        }
    }
}
```

`settings.json` 파일을 `~/.gemini/` directory으로 복사합니다. (MacOS 기준)

```bash
cp settings.json ~/.gemini/
```

Gemini CLI를 실행합니다.

```bash
# https://aistudio.google.com/apikey에서 생성한 API Key를 사용합니다.
export GEMINI_API_KEY="YOUR_API_KEY"
```

```bash
gemini
```

```bash
Loaded cached credentials.

 ███            █████████  ██████████ ██████   ██████ █████ ██████   █████ █████
░░░███         ███░░░░░███░░███░░░░░█░░██████ ██████ ░░███ ░░██████ ░░███ ░░███
  ░░░███      ███     ░░░  ░███  █ ░  ░███░█████░███  ░███  ░███░███ ░███  ░███
    ░░░███   ░███          ░██████    ░███░░███ ░███  ░███  ░███░░███░███  ░███
     ███░    ░███    █████ ░███░░█    ░███ ░░░  ░███  ░███  ░███ ░░██████  ░███
   ███░      ░░███  ░░███  ░███ ░   █ ░███      ░███  ░███  ░███  ░░█████  ░███
 ███░         ░░█████████  ██████████ █████     █████ █████ █████  ░░█████ █████
░░░            ░░░░░░░░░  ░░░░░░░░░░ ░░░░░     ░░░░░ ░░░░░ ░░░░░    ░░░░░ ░░░░░

Tips for getting started:
1. Ask questions, edit files, or run commands.
2. Be specific for the best results.
3. Create GEMINI.md files to customize your interactions with Gemini.
4. /help for more information.

╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ Gemini CLI update available! 0.14.0 → 0.15.0                                                                            │
│ Installed via Homebrew. Please update with "brew upgrade".                                                              │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯


⠋ Connecting to MCP servers... (1/2)

 Using: 1 MCP servers
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ >   Type your message or @path/to/file                                                                                  │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
 ~/.../src/sayou/mcp/stocks_mcp (main*)                                      no sandbox (see /docs)                                       auto
```

MCP 서버가 정상적으로 실행되면 Gemini CLI를 사용하여 MCP 서버를 테스트할 수 있습니다.<br/>
아래 화면은 `삼성전자 배당에 대해 알려줘`를 입력한 후의 결과입니다.

```bash
   ░░░            ░░░░░░░░░  ░░░░░░░░░░ ░░░░░░   ░░░░░░ ░░░░░ ░░░░░░   ░░░░░ ░░░░░
     ░░░         ░░░     ░░░ ░░░        ░░░░░░   ░░░░░░  ░░░  ░░░░░░   ░░░░░  ░░░
       ░░░      ░░░          ░░░        ░░░ ░░░ ░░░ ░░░  ░░░  ░░░ ░░░  ░░░    ░░░
 ███     ░░░    █████████░░██████████ ██████ ░░██████░█████░██████ ░░█████ █████░
   ███ ░░░     ███░    ███░███░░      ██████  ░██████░░███░░██████  ░█████  ███░░
     ███      ███░░░     ░░███░░      ███░███ ███ ███░░███░░███░███  ███░░  ███░░
   ░░░ ███    ███ ░░░█████░██████░░░░░███░░█████  ███░░███░░███░░███ ███░░░ ███░░░
     ███      ███      ███ ███        ███   ███   ███  ███  ███   ██████    ███
   ███         ███     ███ ███        ███         ███  ███  ███    █████    ███
 ███            █████████  ██████████ ███         ███ █████ ███     █████  █████

Tips for getting started:
1. Ask questions, edit files, or run commands.
2. Be specific for the best results.
3. /help for more information.

> 삼성전자 배당에 대해 알려줘

╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ ✓  find_opendart_dividend (opendart-remote MCP Server) {"stock":"삼성전자"}                                                                                                   │
│                                                                                                                                                                               │
│ [{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"주당액면가액(원)","stock_knd":null,"thstrm":"100","frmtrm":"100","lwfr":"100 │
│ ","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"(연결)당기순이익(백만원)","stock_knd":null,"thstrm" │
│ :"24,968,902","frmtrm":"33,621,363","lwfr":"14,473,401","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se │
│ ":"(별도)당기순이익(백만원)","stock_knd":null,"thstrm":"19,807,790","frmtrm":"23,582,565","lwfr":"25,397,099","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code │
│ ":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"(연결)주당순이익(원)","stock_knd":null,"thstrm":"3,724","frmtrm":"4,950","lwfr":"2,131","stlm_dt":"2025-09-30"},{"rc │
│ ept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"현금배당금총액(백만원)","stock_knd":null,"thstrm":"7,354,422","frmtrm":"9,810,767 │
│ ","lwfr":"9,809,438","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"주식배당금총액(백만원)","stock_k │
│ nd":null,"thstrm":"-","frmtrm":"-","lwfr":"-","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"(연결)  │
│ 현금배당성향(%)","stock_knd":null,"thstrm":"29.50","frmtrm":"29.20","lwfr":"67.80","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y" │
│ ,"corp_name":"삼성전자","se":"현금배당수익률(%)","stock_knd":"보통주","thstrm":"1.30","frmtrm":"2.70","lwfr":"1.90","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","cor │
│ p_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"현금배당수익률(%)","stock_knd":"우선주","thstrm":"1.70","frmtrm":"3.30","lwfr":"2.40","stlm_dt":"2025-09-30"}, │
│ {"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"주식배당수익률(%)","stock_knd":"보통주","thstrm":"-","frmtrm":"-","lwfr":"-", │
│ "stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"주식배당수익률(%)","stock_knd":"우선주","thstrm":"-", │
│ "frmtrm":"-","lwfr":"-","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name":"삼성전자","se":"주당                          │
│ 현금배당금(원)","stock_knd":"보통주","thstrm":"1,102","frmtrm":"1,446","lwfr":"1,444","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls": │
│ "Y","corp_name":"삼성전자","se":"주당                                                                                                                                         │
│ 현금배당금(원)","stock_knd":"우선주","thstrm":"1,102","frmtrm":"1,447","lwfr":"1,445","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls": │
│ "Y","corp_name":"삼성전자","se":"주당                                                                                                                                         │
│ 주식배당(주)","stock_knd":"보통주","thstrm":"-","frmtrm":"-","lwfr":"-","stlm_dt":"2025-09-30"},{"rcept_no":"20251114002447","corp_code":"00126380","corp_cls":"Y","corp_name │
│ ":"삼성전자","se":"주당 주식배당(주)","stock_knd":"우선주","thstrm":"-","frmtrm":"-","lwfr":"-","stlm_dt":"2025-09-30"}]                                                      │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
✦ 삼성전자 배당 정보 (2025년 9월 30일 기준):

   * 주당 현금배당금(원)
       * 보통주: 1,102원
       * 우선주: 1,102원
   * 현금배당수익률(%)
       * 보통주: 1.30%
       * 우선주: 1.70%
   * 현금배당금총액(백만원): 7,354,422 백만원
   * (연결)현금배당성향(%): 29.50%

ℹ Gemini CLI update available! 0.17.1 → 0.22.5
  Installed via Homebrew. Please update with "brew upgrade".

 Using: 1 GEMINI.md file | 1 MCP server
╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ >   Type your message or @path/to/file                                                                                                                                        │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
 ~/Development/sayouzone/mcp-opendart-server (main*)                                                 no sandbox (see /docs)                                                 auto
```

![Gemini CLI](https://storage.googleapis.com/sayouzone-homepage/blog/opendart_gemini_cli.png)

**MCP 서버에 대한 요청 질문(예제)**

- 삼성전자
- 삼성전자 재무제표 보여줘
- 삼성전자 재무 상태를 보여줘
- 삼성전자 재무제표 보여줘
- 2024년 삼성전자 재무제표 보여줘
- 2025년 3분기 삼성전자 재무제표 보여줘
- 삼성전자 배당 정보를 보여줘
- 삼성전자 배당에 대해 알려줘
- 삼성전자 배당이 어떻게 되지?
- 2025년 삼성전자 배당이 어떻게 되지?
- 삼성전자 최근 배당 성향에 대해 알려줘
- 삼성전자가 지급하는 보상에 대해 알려줘


## References

- [Cloud Run에 보안 MCP 서버를 배포하는 방법](https://codelabs.developers.google.com/codelabs/cloud-run/how-to-deploy-a-secure-mcp-server-on-cloud-run?hl=ko)
- [Cloud Run에서 MCP 서버를 사용하는 ADK 에이전트 빌드 및 배포](https://codelabs.developers.google.com/codelabs/cloud-run/use-mcp-server-on-cloud-run-with-an-adk-agent?hl=ko)