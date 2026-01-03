"""
OpenDart Crawler
===========================

OpenDart에서 주요 데이터를 추출하는 Python 패키지

Installation:
    pip install requests beautifulsoup4 lxml

Quick Start:
    >>> from opendart import OpenDartCrawler
    >>> 
    >>> crawler = OpenDartCrawler(api_key=dart_api_key)
    >>> 
    >>> # DART의 기업코드을 조회
    >>> filings = crawler.fetch_corp_code(code)
    >>> print(f"기업코드: {corp_code}")
    >>> 
    >>> # 정기보고서 재무정보 | 단일회사 주요계정 조회
    >>> api_type = "단일회사 주요계정"
    >>> corp_name = crawler.fetch_corp_name(corp_code)
    >>> data = crawler.finance(corp_code, last_year, api_type=api_type)
    >>> list = data.get("list", [])
    >>> print(pd.DataFrame(list))
    >>> 
    >>> # 정기보고서 주요정보 | 증자(감자) 현황
    >>> year = "2024"
    >>> quarter = 4
    >>> api_no = 0 # 증자(감자) 현황
    >>> api_key, data = crawler.reports(corp_code, year=year, quarter=quarter, api_no=api_no)
    >>> list = data.get("list", [])
    >>> print(pd.DataFrame(list))
    >>> 
    >>> # 지분공시 종합정보 | 대량보유 상황보고 현황
    >>> api_no = 0 # 대량보유 상황보고 현황
    >>> api_key, data = crawler.ownership(corp_code, api_no=api_no)
    >>> list = data.get("list", [])
    >>> print(pd.DataFrame(list))
    >>> 
    >>> # 주요사항보고서 주요정보 | 자산양수도(기타), 풋백옵션 현황
    >>> corp_code = "00409681"
    >>> api_no = 0 # 자산양수도(기타), 풋백옵션 현황
    >>> api_key, data = crawler.material_facts(corp_code, start_date="20190101", end_date="20251231", api_no=api_no)
    >>> list = data.get("list", [])
    >>> print(pd.DataFrame(list))
    >>> 
    >>> # 증권신고서 주요정보 | 증자(감자) 현황
    >>> corp_code = "00106395"
    >>> api_no = 0 # 증자(감자) 현황
    >>> api_key, data = crawler.registration(corp_code, start_date="20190101", end_date="20251231", api_no=api_no)
    >>> list = data.get("list", [])
    >>> print(pd.DataFrame(list))

Supported Filings:
    - DART의 기업코드을 조회
    - 정기보고서 재무정보
    - 정기보고서 주요정보
    - 지분공시 종합정보
    - 주요사항보고서 주요정보
    - 증권신고서 주요정보

Note:
    OpenDart에서 API Key를 사용하세요.
"""

__version__ = "0.1.0"
__author__ = "SeongJung Kim"

# Copyright (c) 2025, Sayouzone
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
from .crawler import OpenDartCrawler
from .client import OpenDartClient
from .models import (
    # 공시정보
    DisclosureData,
    # 정기보고서 주요정보
    StockIssuanceData,
    DividendsData,
    TreasuryStockData,
    MajorShareholderData,
    MajorShareholderChangeData,
    MinorShareholderData,
    ExecutiveData,
    EmployeeData,
    DirectorCompensationData,
    TotalDirectorCompensationData,
    IntercorporateInvestmentData,
    OutstandingSharesData,
    DebtSecuritiesIssuanceData,
    CPOutstandingData,
    ShortTermBondsOutstandingData,
    CorporateBondsOutstandingData,
    HybridSecuritiesOutstandingData,
    CoCoBondsOutstandingData,
    AuditOpinionsData,
    AuditServiceContractsData,
    NonAuditServiceContractsData,
    OutsideDirectorChangesData,
    UnregisteredExecutiveCompensationData,
    ApprovedDirectorCompensationData,
    CompensationCategoryData,
    ProceedsUseData,
    PrivateEquityFundsUseData,
    # 정기보고서 재무정보
    SingleCompanyMainAccountsData,
    MultiCompanyMainAccountsData,
    XBRLTaxonomyFinancialStatementsData,
    ConsolidatedFinancialStatementsData,
    SingleCompanyKeyFinancialMetricsData,
    MultiCompanyKeyFinancialMetricsData,
    # 지분공시 종합정보
    MajorShoreholdingsData,
    InsiderOwnershipData,
    # 주요사항보고서 주요정보
    # 증권신고서 주요정보
    EquitySecuritiesData,
    RegistrationStatementData,
    DepositoryReceiptData,
    CompanyMergerData,
    ShareExchangeData,
    CompanySpinoffData,
)
from .parsers import (
    OpenDartDisclosureParser,
    OpenDartDocumentParser,
    OpenDartDocumentViewer,
    OpenDartFinanceParser,
    OpenDartMaterialFactsParser,
    OpenDartOwnershipParser,
    OpenDartRegistrationParser,
    OpenDartReportsParser,
)

__all__ = [
    # 메인 클래스
    "OpenDartCrawler",
    "OpenDartClient",
    
    # 데이터 모델
    
    # 파서
    "OpenDartDisclosureParser",
    "OpenDartDocumentParser",
    "OpenDartDocumentViewer",
    "OpenDartFinanceParser",
    "OpenDartMaterialFactsParser",
    "OpenDartOwnershipParser",
    "OpenDartRegistrationParser",
    "OpenDartReportsParser",
]
