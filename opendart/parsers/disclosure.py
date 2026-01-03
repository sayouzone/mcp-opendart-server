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
from ..models import DisclosureRequest, DisclosureData
from ..utils import (
    decode_euc_kr,
    DISCLOSURE_URLS,
    quarters,
    DISCLOSURE_COLUMNS,
    save_zip_path,
    parse_xml,
    parse_unzip_xml,
)

class OpenDartDisclosureParser:
    """
    OpenDART 공시정보 API 파싱 클래스
    
    공시정보: Public Disclosure, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
    """

    def __init__(self, client: OpenDartClient):
        self.client = client

        self.params = {
            "crtfc_key": self.client.api_key,
        }

    def list(self, code, start: str | None = None, end: str | None = None):
        """
        OpenDart 공시정보 - 공시검색 

        Args:
            code (str): 기업 고유번호 (주식 코드(stock_code) 및 DART 고유번호 모두 가능(corp_code))
            start (str): 시작일 (YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, YYYYMMDD)
            end (end): 종료일 (YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, YYYYMMDD)
        Returns:
            pd.DataFrame: 공시 목록
        """
        start = datetime.now().strftime("%Y%m%d") if not start else self._dateformat(start)
        end = datetime.now().strftime("%Y%m%d") if not end else self._dateformat(end)

        request = DisclosureRequest(
            crtfc_key=self.client.api_key,
            corp_code=code,
            bgn_de=start,
            end_de=end,
            corp_cls="Y",
            page_no=1,
            page_count=100
        )

        all_data = []  # 전체 데이터 저장
        all_disclosures = []

        page = 1
        total_page = 0
        total_count = 0
        while True:
            request.page_no = page

            url = DISCLOSURE_URLS.get("공시검색")
            
            response = self.client._get(url, params=request.to_params())
            json_data = response.json()
            
            status = json_data.get("status")

            # 에러 체크
            if status != "000":
                print(f"Error: {json_data.get('message')}")
                break

            # 데이터 추가
            data_list = json_data.get("list", [])
            all_data.extend(data_list)

            disclosures = [DisclosureData(**data) for data in data_list]
            all_disclosures.extend(disclosures)
                        
            # 페이지 정보
            if page == 1:
                total_page = json_data.get("total_page", 1)
                total_count = json_data.get("total_count", 0)

            print(f"페이지 {page}/{total_page} 완료 (총 {total_count}건)")

            # 마지막 페이지면 종료
            if page >= total_page:
                break

            page += 1

        print(all_disclosures)

        # DataFrame 변환
        df = pd.DataFrame(all_data)
        return df

    def company(self, code):
        """
        OpenDart 공시정보 - 기업개황 (기업 정보 조회)
        corp_code, stock_code으로 조회가 가능하지만 기업명으로는 조회되지 않는다.

        Args:
            code (str): 기업 고유번호  (주식 코드(stock_code) 및 DART 고유번호 모두 가능(corp_code))
        Returns:
            Dict: 기업개황
        """
        
        params = self.params
        params["corp_code"] = code

        url = DISCLOSURE_URLS.get("기업개황")
        print(url, params)

        response = self.client._get(url, params=params)
        
        json_data = response.json()
            
        status = json_data.get("status")
        message = json_data.get("message")

        # 에러 체크
        if status != "000":
            print(f"Error: {message}")
            return {}

        self.corp_code = json_data.get("corp_code")
        self.corp_name = json_data.get("corp_name")
        self.stock_code = json_data.get("stock_code")

        del json_data["status"]
        del json_data["message"]
        
        disclosure = DisclosureData(**json_data)
        print(disclosure)

        return json_data

    def document(self, rcept_no, save_path: str | None = None):
        """
        OpenDart 공시정보 - 공시서류원본파일
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003

        Args:
            rcept_no (str): 접수번호
            save_path (str): 파일 저장 경로
        Returns:
            pd.DataFrame: 기업개황
        """
        
        params = self.params
        params["rcept_no"] = rcept_no

        url = DISCLOSURE_URLS.get("공시서류원본파일")

        response = self.client._get(url, params=params)

        response_headers = response.headers

        if response.status_code != 200:
            print(f"다운로드 실패: {response.status_code}")
            return None

        content_type = response_headers.get("Content-Type")
        if "application/xml" in content_type:
            text_data = response.text
            print(text_data)
            return None

        # 바이너리 데이터 
        binary_data = response.content

        if save_path is None:
            save_path = f"dart_{rcept_no}"
            save_path = save_zip_path(response_headers, save_path)
        
        # ZIP 파일 저장
        #self.__save_zip(binary_data, save_path)
        # ZIP 파일 압축해제 및 폴더에 저장
        #self.__save_unzip(binary_data, save_path)
        #return save_path

        result = {
            'rcept_no': rcept_no,
        }

        _result = parse_unzip_xml(response_headers, binary_data, save_path)
        result = result | _result
        
        """
        # ZIP 압축 해제 (메모리에서)
        with zipfile.ZipFile(io.BytesIO(binary_data)) as zf:
            file_list = zf.namelist()
            print(f"압축 파일 내 {len(file_list)}개 파일:")
            
            for fname in file_list:
                # 파일명 인코딩 수정
                enc_name = decode_euc_kr(fname)
                
                print(f"  - {enc_name}")
                result['files'].append(enc_name)
                
                # XML 파일만 파싱
                if fname.endswith('.xml'):
                    content = zf.read(fname)
                    parsed = self._parse_xml(content, enc_name)
                    result['xml_data'].append(parsed)
        """
        
        print(f"\n총 {len(result['xml_data'])}개 XML 파일 파싱 완료")
        return result

    def fetch_corp_code(self, save_path: str | None = None):
        """
        OpenDart 공시정보 - 고유번호
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018

        Args:
            save_path (str): 파일 저장 경로
        Returns:
            Dict: 기업 고유번호 목록
        """
        
        params = self.params

        url = DISCLOSURE_URLS.get("고유번호")
        #print(url, params)

        response = self.client._get(url, params=params)

        response_headers = response.headers

        if response.status_code != 200:
            print(f"다운로드 실패: {response.status_code}")
            return None

        content_type = response_headers.get("Content-Type")
        if "application/xml" in content_type:
            text_data = response.text
            #print(text_data)
            return parse_xml(text_data, "corpcode.xml")

        # 바이너리 데이터 
        binary_data = response.content

        if save_path is None:
            save_path = f"dart_corp_code"
            save_path = save_zip_path(response_headers, save_path)

        # ZIP 파일 저장
        #self.__save_zip(binary_data, save_path)
        # ZIP 파일 압축해제 및 폴더에 저장
        #self.__save_unzip(binary_data, save_path)
        #return save_path

        # ZIP 파일 압축해제 및 XML 파싱 
        result = parse_unzip_xml(response_headers, binary_data, save_path)
        
        print(f"\n총 {len(result['xml_data'])}개 XML 파일 파싱 완료")
        return result

    def _dateformat(self, date_str):
        """다양한 날짜 형식을 YYYYMMDD로 변환"""
        # 구분자(-, /, .) 제거
        return re.sub(r'[-/.]', '', date_str)
