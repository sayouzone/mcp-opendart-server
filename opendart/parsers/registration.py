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

import logging 
import io
import pandas as pd
import re
import zipfile
from datetime import datetime
from urllib.parse import unquote

from ..client import OpenDartClient
from ..models import (
    OpenDartRequest,
    EquitySecuritiesData,
    RegistrationStatementData,
    DepositoryReceiptData,
    CompanyMergerData,
    ShareExchangeData,
    CompanySpinoffData,
)
from ..utils import (
    decode_euc_kr,
    REGISTRATION_URLS,
    quarters,
    duplicate_keys,
    REGISTRATION_COLUMNS
)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OpenDartRegistrationParser:
    """
    OpenDART 증권신고서 주요정보 API 파싱 클래스
    
    증권신고서 주요정보 (Key Information in Registration Statements):
    - https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS006
    """

    def __init__(self, client: OpenDartClient):
        self.client = client

    def fetch(self, corp_code: str, start_date: str, end_date: str, api_no: int = -1, api_type: str = None):
        url = None

        api_key = list(REGISTRATION_URLS.keys())[api_no] if api_no > -1 else api_type        
        url = REGISTRATION_URLS.get(api_key)

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

        # 에러 체크
        if status != "000":
            logger.error(f"Error: {json_data.get('message')}")
            #print(f"Error: {json_data.get('message')}")
            return None

        self.corp_code = json_data.get("corp_code")
        self.corp_name = json_data.get("corp_name")
        self.stock_code = json_data.get("stock_code")

        del json_data["status"]
        del json_data["message"]
        
        print(f"api_key: {api_key}")
        if api_key == "지분증권":
            return EquitySecuritiesData.from_raw_data(json_data)
        elif api_key == "채무증권":
            return RegistrationStatementData.from_raw_data(json_data)
        elif api_key == "증권예탁증권":
            return DepositoryReceiptData.from_raw_data(json_data)
        elif api_key == "합병":
            return CompanyMergerData.from_raw_data(json_data)
        elif api_key == "주식의포괄적교환·이전":
            return ShareExchangeData.from_raw_data(json_data)
        elif api_key == "분할":
            return CompanySpinoffData.from_raw_data(json_data)
        
        return None

if __name__ == "__main__":
    registration = DartRegistrationParser(None)
    duplicate_keys(registration.registration_columns)