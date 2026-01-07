# 🚀 Gemini와 OpenDart로 나만의 주식 분석 AI 비서 만들기 (MCP Server 구축 가이드)

LLM(Large Language Model)이 실시간 주식 정보를 읽어와서 분석해 준다면 어떨까요?
이번 포스트에서는 **Google Cloud Run**과 **FastMCP**를 사용하여 한국 기업의 공시 정보(OpenDart)를 조회하는 **MCP(Model Context Protocol) 서버**를 구축하는 방법을 소개합니다.

이 서버를 구축하면 Gemini와 같은 LLM이 `삼성전자 재무제표 보여줘`와 같은 자연어 질문에 대해 정확한 최신 펀더멘탈 데이터를 가져와 답변할 수 있게 됩니다.

---

## 🛠️ 아키텍처 및 기술 스택

* **Platform**: Google Cloud Run (Serverless)
* **Language**: Python 3.11+
* **Framework**: FastMCP (MCP 서버 구축용)
* **Data Source**: OpenDart API (공시, 재무정보)
* **Client**: Gemini CLI (테스트 및 인터랙션)
* **Library**: `sayou-stock` (OpenDart 래퍼)

---

## 1. 사전 준비 (GCP 환경 설정)

먼저 Google Cloud Platform(GCP)에서 프로젝트를 준비하고 필요한 서비스를 활성화해야 합니다. 터미널에서 `gcloud` 명령어를 사용합니다.

### 1.1 서비스 활성화 및 계정 생성

Cloud Run과 아티팩트 레지스트리 등을 사용하기 위해 API를 활성화하고, MCP 서버가 사용할 서비스 계정(SA)을 생성합니다.

```bash
# 서비스 활성화
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com

# 서비스 계정 생성
gcloud iam service-accounts create mcp-server-sa --display-name="MCP Server Service Account"

```

---

## 2. MCP 서버 개발 (Python)

이제 실제 코드를 작성해 봅시다. `FastMCP`를 사용하면 데코레이터 패턴으로 아주 쉽게 도구(Tool)를 정의할 수 있습니다.

### 2.1 프로젝트 설정 (`pyproject.toml`)

필요한 의존성을 정의합니다. `fastmcp`와 주식 데이터 수집을 위한 `sayou-stock`, 그리고 보안을 위한 `google-cloud-secret-manager`가 핵심입니다.

```toml
[project]
name = "opendart-mcp"
version = "0.1.0"
dependencies = [
    "fastmcp==2.12.4",
    "sayou-stock>=0.1.0",
    "google-cloud-secret-manager==2.25.0",
    "pandas==2.3.3",
    # ... 기타 의존성
]

```

### 2.2 서버 코드 구현 (`opendarts.py`)

주요 로직은 다음과 같습니다.

1. **Secret Manager**에서 OpenDart API Key를 안전하게 로드합니다.
2. `OpenDartCrawler`를 초기화하여 기업 코드 정보를 캐싱합니다.
3. `@mcp.tool` 데코레이터를 사용하여 LLM이 호출할 함수를 정의합니다.

```python
import os
import logging
from fastmcp import FastMCP
from google.cloud import secretmanager
from sayou.stock.opendart import OpenDartCrawler

# 1. Secret Manager에서 API Key 로드
sm_client = secretmanager.SecretManagerServiceClient()
name = "projects/1037372895180/secrets/DART_API_KEY/versions/latest"
response = sm_client.access_secret_version(name=name)
os.environ["DART_API_KEY"] = response.payload.data.decode("UTF-8")

# 2. OpenDartCrawler 초기화 (기업 코드 캐싱)
crawler = OpenDartCrawler(api_key=os.environ["DART_API_KEY"])
crawler.save_corp_data("corpcode.json")

mcp = FastMCP("opendart-server")

# 3. 도구(Tool) 정의
@mcp.tool(
    name="find_opendart_finance",
    description="OpenDART에서 한국 주식 재무제표(BS, IS, CF) 수집...",
    tags={"opendart", "fundamentals"}
)
async def find_opendart_finance(stock: str, year: Optional[int] = None, quarter: Optional[int] = None):
    """
    종목 코드나 기업명(예: '삼성전자')을 받아 재무제표를 반환합니다.
    캐시를 우선 사용하여 응답 속도를 높입니다.
    """
    logger.info(f">>> 🛠️ Tool: 'find_opendart_finance' called for '{stock}'")
    
    # 기업 코드로 변환 및 데이터 조회 로직 수행
    corp_code = crawler.fetch_corp_code(stock)
    # ... (데이터 조회 로직)
    return finance_data

```

---

## 3. 클라우드 배포 (Cloud Run)

로컬에서 개발이 완료되었다면 Cloud Run에 배포하여 언제든 접근 가능한 서버로 만듭니다.

### 3.1 권한 부여 및 배포

서비스 계정에 Secret Manager 접근 권한을 부여한 후 배포를 진행합니다.

```bash
export GOOGLE_CLOUD_PROJECT=sayouzone-ai
export MCP_SERVER_NAME=opendart-mcp-server

# Secret Manager 접근 권한 부여
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=serviceAccount:mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --role="roles/secretmanager.secretAccessor"

# Cloud Run 배포
gcloud run deploy $MCP_SERVER_NAME \
    --service-account=mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp

```

---

## 4. Gemini CLI로 테스트하기

배포된 MCP 서버를 Gemini CLI와 연결하여 실제 대화형 인터페이스로 테스트해 봅시다.

### 4.1 Gemini CLI 설정 (`settings.json`)

Cloud Run URL과 인증 토큰을 설정 파일에 추가합니다.

```json
{
    "mcpServers": {
        "opendart-remote": {
            "httpUrl": "https://opendart-mcp-server-xyz.us-central1.run.app/mcp",
            "headers": {
                "Authorization": "Bearer YOUR_ID_TOKEN"
            }
        }
    }
}

```

### 4.2 실행 및 질의

터미널에서 `gemini` 명령어를 실행하고 다음과 같이 물어보세요.

> **User:** "삼성전자 2024년 재무제표 보여줘"
> **Gemini:** (MCP 서버를 호출하여 OpenDart 데이터를 가져온 후 분석 결과 출력)
> *"삼성전자의 2024년 재무상태는 다음과 같습니다..."*

**💡 활용 가능한 질문 예시:**

* "SK하이닉스 최근 배당 성향에 대해 알려줘"
* "2025년 3분기 삼성전자 영업이익이 어떻게 돼?"
* "삼성전자가 지급하는 보상(배당) 정보를 요약해 줘"

---

## 📝 마치며

이제 여러분의 AI 에이전트는 한국 주식 시장의 공식 데이터(Dart)에 접근할 수 있는 강력한 도구를 갖게 되었습니다. 이를 응용하면 재무 분석 자동화, 투자 리포트 생성 등 다양한 핀테크 서비스를 구현할 수 있습니다.

**참고 링크:**

* [OpenDart API 가이드](https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001)
* [Google Cloud Run MCP 배포 가이드](https://codelabs.developers.google.com/codelabs/cloud-run/how-to-deploy-a-secure-mcp-server-on-cloud-run?hl=ko)
