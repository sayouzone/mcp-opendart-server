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
 
import io
import zipfile
import re
import pandas as pd
from datetime import datetime
from urllib.parse import unquote

from ..client import OpenDartClient
from ..models import (
    OpenDartRequest,
    PutOptionData,
    BankruptcyData,
    SuspensionData,
    RehabilitationData,
    DissolutionData,
    RightsIssueData,
    BonusIssueData,
    IssueIncreaseData,
    CapitalReductionData,
    ManagementProcedureData,
    LegalProceedingsData,
    ListingDecisionData,
    DelistingDecisionData,
    ListingData,
    DelistingData,
    CBIssuanceDecisionData,
    BWIssuanceDecisionData,
    EBIssuanceDecisionData,
    CreditorBankManagementProcessSuspensionData,
    CoCoBondIssuanceDecisionData,
    ShareBuybackDecisionData,
    TreasuryStockDisposalDecisionData,
    TrustAgreementAcquisitionDecisionData,
    TrustAgreementResolutionDecisionData,
    BusinessAcquisitionDecisionData,
    BusinessTransferDecisionData,
    AssetTransferDecisionData,
    OtherShareTransferDecisionData,
    EquityLinkedBondsTransferDecisionData,
    CompanyMergerDecisionData,
    CompanySpinoffDecisionData,
    CompanySpinoffMergerDecisionData,
    ShareExchangeDecisionData,
)
from ..utils import (
    decode_euc_kr,
    MATERIAL_FACTS_URLS,
    quarters,
    MATERIAL_FACTS_COLUMNS
)

class OpenDartMaterialFactsParser:
    """
    OpenDART 주요사항보고서 주요정보 API 파싱 클래스
    
    주요사항보고서 주요정보: Key Information in Reports on Material Facts, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005
    """

    def __init__(self, client: OpenDartClient):
        self.client = client

    def fetch(
        self, 
        corp_code: str, 
        start_date: str, 
        end_date: str, 
        api_no: int = -1, 
        api_type: str = None):
        url = None

        api_key = list(MATERIAL_FACTS_URLS.keys())[api_no] if api_no > -1 else api_type        
        url = MATERIAL_FACTS_URLS.get(api_key)

        if not url:
            return

        request = OpenDartRequest(
            crtfc_key=self.client.api_key,
            corp_code=corp_code,
            bgn_de=start_date,
            end_de=end_date,
        )

        response = self.client._get(url, params=request.to_params())

        json_data = response.json()
        #print(json_data)
            
        status = json_data.get("status")

        self.corp_code = json_data.get("corp_code")
        self.corp_name = json_data.get("corp_name")
        self.stock_code = json_data.get("stock_code")

        # 에러 체크
        if status != "000":
            print(f"Error: {json_data.get('message')}")
            return json_data

        del json_data["status"]
        del json_data["message"]
        
        print(f"api_key: {api_key}")
        data_list = json_data.get("list", [])
        if api_key == "자산양수도(기타), 풋백옵션":
            return [PutOptionData(**data) for data in data_list]
        elif api_key == "부도발생":
            return [BankruptcyData(**data) for data in data_list]
        elif api_key == "영업정지":
            return [SuspensionData(**data) for data in data_list]
        elif api_key == "회생절차 개시신청":
            return [RehabilitationData(**data) for data in data_list]
        elif api_key == "해산사유 발생":
            return [DissolutionData(**data) for data in data_list]
        elif api_key == "유상증자 결정":
            return [RightsIssueData(**data) for data in data_list]
        elif api_key == "무상증자 결정":
            return [BonusIssueData(**data) for data in data_list]
        elif api_key == "유무상증자 결정":
            return [IssueIncreaseData(**data) for data in data_list]
        elif api_key == "감자 결정":
            return [CapitalReductionData(**data) for data in data_list]
        elif api_key == "채권은행 등의 관리절차 개시":
            return [ManagementProcedureData(**data) for data in data_list]
        elif api_key == "소송 등의 제기":
            return [LegalProceedingsData(**data) for data in data_list]
        elif api_key == "해외 증권시장 주권등 상장 결정":
            return [ListingDecisionData(**data) for data in data_list]
        elif api_key == "해외 증권시장 주권등 상장폐지 결정":
            return [DelistingDecisionData(**data) for data in data_list]
        elif api_key == "해외 증권시장 주권등 상장":
            return [ListingData(**data) for data in data_list]
        elif api_key == "해외 증권시장 주권등 상장폐지":
            return [DelistingData(**data) for data in data_list]
        elif api_key == "전환사채권 발행결정":
            return [CBIssuanceDecisionData(**data) for data in data_list]
        elif api_key == "신주인수권부사채권 발행결정":
            return [BWIssuanceDecisionData(**data) for data in data_list]
        elif api_key == "교환사채권 발행결정":
            return [EBIssuanceDecisionData(**data) for data in data_list]
        elif api_key == "채권은행 등의 관리절차 중단":
            return [CreditorBankManagementProcessSuspensionData(**data) for data in data_list]
        elif api_key == "상각형 조건부자본증권 발행결정":
            return [CoCoBondIssuanceDecisionData(**data) for data in data_list]
        elif api_key == "자기주식 취득 결정":
            return [ShareBuybackDecisionData(**data) for data in data_list]
        elif api_key == "자기주식 처분 결정":
            return [TreasuryStockDisposalDecisionData(**data) for data in data_list]
        elif api_key == "자기주식취득 신탁계약 체결 결정":
            return [TrustAgreementAcquisitionDecisionData(**data) for data in data_list]
        elif api_key == "자기주식취득 신탁계약 해지 결정":
            return [TrustAgreementResolutionDecisionData(**data) for data in data_list]
        elif api_key == "영업양수 결정":
            return [BusinessAcquisitionDecisionData(**data) for data in data_list]
        elif api_key == "영업양도 결정":
            return [BusinessTransferDecisionData(**data) for data in data_list]
        elif api_key == "유형자산 양수 결정":
            return [AssetTransferDecisionData(**data) for data in data_list]
        elif api_key == "유형자산 양도 결정":
            return [AssetTransferDecisionData(**data) for data in data_list]
        elif api_key == "타법인 주식 및 출자증권 양수결정":
            return [OtherShareTransferDecisionData(**data) for data in data_list]
        elif api_key == "타법인 주식 및 출자증권 양도결정":
            return [OtherShareTransferDecisionData(**data) for data in data_list]
        elif api_key == "주권 관련 사채권 양수 결정":
            return [EquityLinkedBondsTransferDecisionData(**data) for data in data_list]
        elif api_key == "주권 관련 사채권 양도 결정":
            return [EquityLinkedBondsTransferDecisionData(**data) for data in data_list]
        elif api_key == "회사합병 결정":
            return [CompanyMergerDecisionData(**data) for data in data_list]
        elif api_key == "회사분할 결정":
            return [CompanySpinoffDecisionData(**data) for data in data_list]
        elif api_key == "회사분할합병 결정":
            return [CompanySpinoffMergerDecisionData(**data) for data in data_list]
        elif api_key == "주식교환·이전 결정":
            return [ShareExchangeDecisionData(**data) for data in data_list]

        return json_data