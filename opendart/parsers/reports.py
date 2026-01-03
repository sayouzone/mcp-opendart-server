import io
import zipfile
import re
import pandas as pd
from datetime import datetime
from urllib.parse import unquote

from ..client import OpenDartClient
from ..models import (
    OpenDartRequest,
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
)
from ..utils import (
    decode_euc_kr,
    REPORTS_URLS,
    quarters,
    REPORTS_COLUMNS
)

class OpenDartReportsParser:
    """
    OpenDART 정기보고서 주요정보 API 파싱 클래스
    
    정기보고서 주요정보 (Key Information in Periodic Reports):
    - https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002
    """

    def __init__(self, client: OpenDartClient):
        self.client = client

    def fetch(self, corp_code: str, year: str, quarter: int, api_no: int = -1, api_type: str = None):
        url = None

        api_key = list(REPORTS_URLS.keys())[api_no] if api_no > -1 else api_type        
        url = REPORTS_URLS.get(api_key)

        if not url:
            return

        report_code = quarters.get(str(quarter), "4")
        
        request = OpenDartRequest(
            crtfc_key=self.client.api_key,
            corp_code=corp_code,
            bsns_year=year,
            reprt_code=report_code,
        )

        response = self.client._get(url, params=request.to_params())

        json_data = response.json()
        #print(json_data)
            
        status = json_data.get("status")

        # 에러 체크
        if status != "000":
            print(f"Error: {json_data.get('message')}")
            return []

        self.corp_code = json_data.get("corp_code")
        self.corp_name = json_data.get("corp_name")
        self.stock_code = json_data.get("stock_code")

        del json_data["status"]
        del json_data["message"]
        
        print(f"api_key: {api_key}")
        data_list = json_data.get("list", [])
        if api_key == "증자(감자) 현황":
            return [StockIssuanceData(**data) for data in data_list]
        elif api_key == "배당에 관한 사항":
            return [DividendsData(**data) for data in data_list]
        elif api_key == "자기주식 취득 및 처분 현황":
            return [TreasuryStockData(**data) for data in data_list]
        elif api_key == "최대주주 현황":
            return [MajorShareholderData(**data) for data in data_list]
        elif api_key == "최대주주 변동현황":
            return [MajorShareholderChangeData(**data) for data in data_list]
        elif api_key == "소액주주 현황":
            return [MinorShareholderData(**data) for data in data_list]
        elif api_key == "임원 현황":
            return [ExecutiveData(**data) for data in data_list]
        elif api_key == "직원 현황":
            return [EmployeeData(**data) for data in data_list]
        elif api_key == "이사·감사의 개인별 보수현황(5억원 이상)":
            return [DirectorCompensationData(**data) for data in data_list]
        elif api_key == "이사·감사 전체의 보수현황(보수지급금액 - 이사·감사 전체)":
            return [TotalDirectorCompensationData(**data) for data in data_list]
        elif api_key == "개인별 보수지급 금액(5억이상 상위5인)":
            return [DirectorCompensationData(**data) for data in data_list]
        elif api_key == "타법인 출자현황":
            return [IntercorporateInvestmentData(**data) for data in data_list]
        elif api_key == "주식의 총수 현황":
            return [OutstandingSharesData(**data) for data in data_list]
        elif api_key == "채무증권 발행실적":
            return [DebtSecuritiesIssuanceData(**data) for data in data_list]
        elif api_key == "기업어음증권 미상환 잔액":
            return [CPOutstandingData(**data) for data in data_list]
        elif api_key == "단기사채 미상환 잔액":
            return [ShortTermBondsOutstandingData(**data) for data in data_list]
        elif api_key == "회사채 미상환 잔액 ":
            return [CorporateBondsOutstandingData(**data) for data in data_list]
        elif api_key == "신종자본증권 미상환 잔액":
            return [HybridSecuritiesOutstandingData(**data) for data in data_list]
        elif api_key == "조건부 자본증권 미상환 잔액":
            return [CoCoBondsOutstandingData(**data) for data in data_list]
        elif api_key == "회계감사인의 명칭 및 감사의견":
            return [AuditOpinionsData(**data) for data in data_list]
        elif api_key == "감사용역체결현황":
            return [AuditServiceContractsData(**data) for data in data_list]
        elif api_key == "회계감사인과의 비감사용역 계약체결 현황":
            return [NonAuditServiceContractsData(**data) for data in data_list]
        elif api_key == "사외이사 및 그 변동현황":
            return [OutsideDirectorChangesData(**data) for data in data_list]
        elif api_key == "미등기임원 보수현황":
            return [UnregisteredExecutiveCompensationData(**data) for data in data_list]
        elif api_key == "이사·감사 전체의 보수현황(주주총회 승인금액)":
            return [ApprovedDirectorCompensationData(**data) for data in data_list]
        elif api_key == "이사·감사 전체의 보수현황(보수지급금액 - 유형별)":
            return [CompensationCategoryData(**data) for data in data_list]
        elif api_key == "공모자금의 사용내역":
            return [ProceedsUseData(**data) for data in data_list]
        elif api_key == "사모자금의 사용내역":
            return [PrivateEquityFundsUseData(**data) for data in data_list]

        return []
