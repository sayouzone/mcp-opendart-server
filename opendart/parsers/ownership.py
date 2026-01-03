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
    MajorShoreholdingsData,
    InsiderOwnershipData,
)
from ..utils import (
    decode_euc_kr,
    OWNERSHIP_URLS,
    quarters,
    OWNERSHIP_COLUMNS
)

class OpenDartOwnershipParser:
    """
    OpenDART 지분공시 종합정보 API 파싱 클래스
    
    지분공시 종합정보: Comprehensive Share Ownership Information, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS004
    """
	
    def __init__(self, client: OpenDartClient):
        self.client = client

        self.params = {
            "crtfc_key": self.client.api_key,
            "corp_code": None,
        }

    def fetch(self, corp_code: str, api_no: int = -1, api_type: str = None):
        url = None

        api_key = list(OWNERSHIP_URLS.keys())[api_no] if api_no > -1 else api_type        
        url = OWNERSHIP_URLS.get(api_key)

        if not url:
            return

        request = OpenDartRequest(
            crtfc_key=self.client.api_key,
            corp_code=corp_code,
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

        data_list = json_data.get("list", [])
        if api_key == "대량보유 상황보고":
            return [MajorShoreholdingsData(**data) for data in data_list]
        elif api_key == "임원ㆍ주요주주 소유보고":
            return [InsiderOwnershipData(**data) for data in data_list]

        return []