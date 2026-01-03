import json
import logging
import os
import pandas as pd

from datetime import datetime
from fastmcp import FastMCP
from pathlib import Path
from typing import Optional

from utils.gcpmanager import GCSManager

env_type = os.getenv("ENV_TYPE", "local")

if env_type == "local":
    from opendart import OpenDartCrawler
else:
    from sayou.stock.opendart import OpenDartCrawler

from google.cloud import secretmanager

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(levelname)s]: %(message)s", level=logging.INFO)

sm_client = secretmanager.SecretManagerServiceClient()
name = "projects/1037372895180/secrets/DART_API_KEY/versions/latest"
response = sm_client.access_secret_version(name=name)
dart_api_key = response.payload.data.decode("UTF-8")
print(f"DART API Key: {dart_api_key}")
os.environ["DART_API_KEY"] = dart_api_key

corpcode_filename = "corpcode.json"

mcp = FastMCP("OpenDart MCP Server")

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
async def find_opendart_finance(stock: str, year: Optional[int] = None, quarter: Optional[int] = None):
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
    logger.info(f">>> 🛠️ Tool: 'find_opendart_data' called for '{stock}'")

    is_date = year is not None and quarter is not None

    year, quarter = _year_quarter(year, quarter)

    crawler = OpenDartCrawler(api_key=dart_api_key)
    corp_data = crawler.corp_data
    crawler.save_corp_data(corpcode_filename)

    #api_type = "단일회사 주요계정"
    api_type = "단일회사 전체 재무제표"
    corp_code = crawler.fetch_corp_code(stock)

    count = 1
    while True:
        logger.info(f"fetching finance data: {year}Q{quarter}")
        data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
        if is_date or len(data) > 0 or count > 4:
            break
        quarter = quarter - 1 if quarter > 1 else 4
        year = year - 1 if quarter == 4 else year
        count += 1

    outputs = []
    for item in data:
        outputs.append(item.to_dict())

    return outputs


@mcp.tool(
    name="find_opendart_dividend",
    description="""OpenDART에서 한국 주식 배당 정보 수집.
    사용 대상:
    - 6자리 숫자 티커: 005930, 000660
    - .KS/.KQ 접미사: 005930.KS, 035720.KQ
    - 한국 기업명: 삼성전자, SK하이닉스

    반환: {
        "ticker": str,
        "country": "KR",
        "dividend": json,
    }

    참고: 캐시를 우선 사용하여 빠른 응답을 제공합니다.
    크롤링은 최대 60초 이상 소요될 수 있으므로 가능한 캐시를 활용합니다.
    """,
    tags={"opendart", "dividend", "korea", "standardized", "cached"}
)
async def find_opendart_dividend(stock: str, year: Optional[int] = None, quarter: Optional[int] = None):
    """
    OpenDART에서 한국 주식 배당 정보를 수집합니다.

    yfinance와 동일한 스키마를 반환하여 LLM 에이전트가
    한국 주식과 해외 주식을 동일한 방식으로 처리할 수 있습니다.

    Args:
        stock: 종목 코드 (예: "005930", "삼성전자")

    Returns:
        dict: 배당 정보 (yfinance와 동일한 스키마)

    Note:
        - use_cache=True (기본값): GCS에서 캐시된 데이터를 먼저 확인 (빠름)
        - use_cache=False: 항상 새로 크롤링 (느림, 30초+ 소요)
    """
    logger.info(f">>> 🛠️ Tool: 'find_opendart_dividend' called for '{stock}'")

    is_date = year is not None and quarter is not None

    year, quarter = _year_quarter(year, quarter)

    crawler = OpenDartCrawler(api_key=dart_api_key)
    corp_data = crawler.corp_data
    crawler.save_corp_data(corpcode_filename)

    api_type = "배당에 관한 사항"
    corp_code = crawler.fetch_corp_code(stock)

    count = 1
    while True:
        logger.info(f"fetching finance data: {year}Q{quarter}")
        data = crawler.reports(corp_code, year=year, quarter=quarter, api_type=api_type)
        if is_date or len(data) > 0 or count > 4:
            break
        quarter = quarter - 1 if quarter > 1 else 4
        year = year - 1 if quarter == 4 else year
        count += 1

    outputs = []
    for item in data:
        outputs.append(item.to_dict())

    return outputs

@mcp.tool(
    name="find_opendart_compensation",
    description="""OpenDART에서 한국 기업의 이사 및 감사 보수 정보 수집.
    사용 대상:
    - 6자리 숫자 티커: 005930, 000660
    - .KS/.KQ 접미사: 005930.KS, 035720.KQ
    - 한국 기업명: 삼성전자, SK하이닉스

    반환: {
        "ticker": str,
        "country": "KR",
        "compensation": json,
    }

    참고: 캐시를 우선 사용하여 빠른 응답을 제공합니다.
    크롤링은 최대 60초 이상 소요될 수 있으므로 가능한 캐시를 활용합니다.
    """,
    tags={"opendart", "dividend", "korea", "standardized", "cached"}
)
async def find_opendart_compensation(stock: str, year: Optional[int] = None, quarter: Optional[int] = None):
    """
    OpenDART에서 한국 기업의 이사 및 감사 보수 정보를 수집합니다.

    yfinance와 동일한 스키마를 반환하여 LLM 에이전트가
    한국 주식과 해외 주식을 동일한 방식으로 처리할 수 있습니다.

    Args:
        stock: 종목 코드 (예: "005930", "삼성전자")

    Returns:
        dict: 배당 정보 (yfinance와 동일한 스키마)

    Note:
        - use_cache=True (기본값): GCS에서 캐시된 데이터를 먼저 확인 (빠름)
        - use_cache=False: 항상 새로 크롤링 (느림, 30초+ 소요)
    """
    logger.info(f">>> 🛠️ Tool: 'find_opendart_compensation' called for '{stock}'")

    is_date = year is not None and quarter is not None

    year, quarter = _year_quarter(year, quarter)

    crawler = OpenDartCrawler(api_key=dart_api_key)
    corp_data = crawler.corp_data
    crawler.save_corp_data(corpcode_filename)

    corp_code = crawler.fetch_corp_code(stock)

    outputs = []

    api_type = "이사·감사의 개인별 보수현황(5억원 이상)"

    count = 1
    while True:
        logger.info(f"fetching finance data: {year}Q{quarter}")
        data = crawler.reports(corp_code, year=year, quarter=quarter, api_type=api_type)
        if is_date or len(data) > 0 or count > 4:
            break
        quarter = quarter - 1 if quarter > 1 else 4
        year = year - 1 if quarter == 4 else year
        count += 1

    for item in data:
        outputs.append(item.to_dict())

    api_type = "이사·감사 전체의 보수현황(보수지급금액 - 이사·감사 전체)"

    count = 1
    while True:
        logger.info(f"fetching finance data: {year}Q{quarter}")
        data = crawler.reports(corp_code, year=year, quarter=quarter, api_type=api_type)
        if is_date or len(data) > 0 or count > 4:
            break
        quarter = quarter - 1 if quarter > 1 else 4
        year = year - 1 if quarter == 4 else year
        count += 1

    for item in data:
        outputs.append(item.to_dict())

    api_type = "개인별 보수지급 금액(5억이상 상위5인)"

    count = 1
    while True:
        logger.info(f"fetching finance data: {year}Q{quarter}")
        data = crawler.reports(corp_code, year=year, quarter=quarter, api_type=api_type)
        if is_date or len(data) > 0 or count > 4:
            break
        quarter = quarter - 1 if quarter > 1 else 4
        year = year - 1 if quarter == 4 else year
        count += 1

    for item in data:
        outputs.append(item.to_dict())

    return outputs

def _year_quarter(year, quarter):
    """Year and Quarter """
    now = datetime.now()
    q = (now.month - 1) // 3
    default_year, default_quarter = (now.year - 1, 4) if q == 0 else (now.year, q)
    
    year = year or default_year
    quarter = quarter or (4 if year < now.year else default_quarter)

    return year, quarter

def _to_json(data):
    if isinstance(data, pd.DataFrame):
        return json.loads(data.to_json(orient="records", date_format="iso"))
    if isinstance(data, pd.Series):
        return data.to_dict()
    if isinstance(data, dict):
        return data
