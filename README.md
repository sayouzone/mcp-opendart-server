# OpenDart MCP Server

OpenDart Crawling and Caching MCP Server<br/>

주식 정보를 `OpenDart`에서 종목 기본 정보 (펀더멘탈, fundamental)를 가져온다.

모델 컨텍스트 프로토콜 (Model Context Protocol, MCP) 서버를 빌도하고 배포<br/>
MCP 서버는 LLM에 외부 도구 및 서비스에 대한 액세스 권한을 제공<br/>
FastMCP를 사용, MCP 서버와 클라이언트를 빌드하는 빠르고 Pythonic한 방법을 제공<br/>

- Gemini 3.0 Pro
- Gemini 3.0 Flash

#### 참조 문서
- [Cloud Run에 보안 MCP 서버를 배포하는 방법](https://codelabs.developers.google.com/codelabs/cloud-run/how-to-deploy-a-secure-mcp-server-on-cloud-run?hl=ko)
- [Gemini CLI: Custom slash commands](https://cloud.google.com/blog/topics/developers-practitioners/gemini-cli-custom-slash-commands?e=48754805)

## 패키지 구조

```
├── opendart/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│       ├── __init__.py
│       ├── document.py        # 문서 API 파서
│       ├── document_viewer.py # 문서 뷰어 API 파서
│       ├── disclosure.py      # 공시정보 API 파서
│       ├── finance.py         # 정기보고서 재무정보 API 파서
│       ├── material_facts.py  # 주요사항보고서 주요정보 API 파서
│       ├── ownership.py       # 지분공시 종합정보 API 파서
│       ├── registration.py    # 증권신고서 주요정보 API 파서
│       └── reports.py         # 정기보고서 주요정보 API 파서
├── tests/
│   ├── test_opendart.py       # OpenDART 테스트 (로컬 소스)
│   └── test_opendart_.py      # OpenDART 테스트 (sayou-stock)
├── __init__.py
├── .gitignore
├── Dockerfile
├── LICENSE
├── opendarts.py
├── pyproject.toml
├── README.md
├── requirements.txt
└── server.py
```

## 배포 (Cloud Run)

```bash
MCP_SERVER_NAME=opendart-mcp-server
export GOOGLE_CLOUD_PROJECT=sayouzone-ai
```

#### GCP 설정 (1회만)

서비스 활성화

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

서비스 계정 생성

```bash
gcloud iam service-accounts create mcp-server-sa --display-name="MCP Server Service Account"
```

```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=user:$(gcloud config get-value account) \
    --role='roles/run.invoker'
```

```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=serviceAccount:mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --role="roles/secretmanager.secretAccessor"
```

#### 배포

**패키지 소스로 테스트**

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

**sayou-stock 설치 및 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --service-account=mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp
```

**sayou-stock 설치 및 접근권한 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp
```

## Tests

#### Gemini 테스트

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
            "selectedType": "gemini-api-key"
        }
    }
}
```

Copy settings.json file to ~/.gemini/ directory.

```bash
cp settings.json ~/.gemini/
```

```bash
gemini
```

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

 Using: 2 MCP servers
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ >   Type your message or @path/to/file                                                                                  │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
 ~/.../src/sayou/mcp/stocks_mcp (main*)                                      no sandbox (see /docs)                                       auto
```

## Deploy sayou-stock

```bash
git push origin main
git tag sayou-stock-v0.1.1
git push origin sayou-stock-v0.1.1 
```

## Errors

```bash
> /mcp

Configured MCP servers:

🟢 stocks-remote - Ready (4 tools)
  Tools:
  - find_fnguide_data
  - find_yahoofinance_data
  - get_yahoofinance_fundamentals
  - save_fundamentals_data_to_gcs

🟢 zoo-remote - Ready (2 tools, 1 prompt)
  Tools:
  - get_animal_details
  - get_animals_by_species
  Prompts:
  - find


ℹ Gemini CLI update available! 0.14.0 → 0.15.0
  Installed via Homebrew. Please update with "brew upgrade".

 Using: 2 MCP servers
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ >   Type your message or @path/to/file                                                                                  │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
 ~/.../src/sayou/mcp/stocks_mcp (main*)                     no sandbox (see /docs)                                     auto
```

```bash
> 삼성전자

ℹ Gemini CLI update available! 0.14.0 → 0.15.0
  Installed via Homebrew. Please update with "brew upgrade".
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ x  find_fnguide_data (stocks-remote MCP Server) {"stock":"삼성전자"}                                                      │
│                                                                                                                         │
│    MCP tool 'find_fnguide_data' reported tool error for function call: {"name":"find_fnguide_data","args":{"stock":"삼성전자"}} with  │
│    response: [{"functionResponse":{"name":"find_fnguide_data","response":{"error":{"content":[{"type":"text","text":"Error calling tool │
│    'find_fnguide_data': BrowserType.launch: Executable doesn't exist at                                                                     │
│    /root/.cache/ms-playwright/chromium_headless_shell-1194/chrome-linux/headless_shell\n╔══════════════════════════════════════════════════ │
│    ══════════╗\n║ Looks like Playwright was just installed or updated.       ║\n║ Please run the following command to download new          │
│    browsers: ║\n║                                                            ║\n║     playwright install                                    │
│    ║\n║                                                            ║\n║ <3 Playwright Team                                                  │
│    ║\n╚════════════════════════════════════════════════════════════╝"}],"isError":true}}}}]                                                 │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ -  Shell playwright install [current working directory /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/mcp/stocks_mcp] … │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## References

- [Cloud Run에서 MCP 서버를 사용하는 ADK 에이전트 빌드 및 배포](https://codelabs.developers.google.com/codelabs/cloud-run/use-mcp-server-on-cloud-run-with-an-adk-agent?hl=ko)