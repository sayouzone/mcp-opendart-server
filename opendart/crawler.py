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
 
import json
import os
import pandas as pd
import re
import requests
import sys
import time
from typing import Dict, List, Optional, Tuple

from .client import OpenDartClient
from .utils import (
    duplicate_keys,
    DISCLOSURE_COLUMNS,
    REPORTS_COLUMNS,
    FINANCE_COLUMNS,
    OWNERSHIP_COLUMNS,
    MATERIAL_FACTS_COLUMNS,
    REGISTRATION_COLUMNS,
)

from .parsers import (
    OpenDartDocumentParser,
    OpenDartDocumentViewer,
    OpenDartDisclosureParser,
    OpenDartFinanceParser,
    OpenDartMaterialFactsParser,
    OpenDartOwnershipParser,
    OpenDartRegistrationParser,
    OpenDartReportsParser,
)

class OpenDartCrawler:
        
    """DART 공시 문서 크롤러.
    
    기업의 공시 문서를 DART에서 크롤링하여 GCS에 업로드합니다.
    """
    
    # 제외할 공시 유형
    EXCLUDED_REPORT_TYPES = frozenset({"기업설명회(IR)개최(안내공시)"})
    request_delay_seconds = 3

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/120.0.0.0 Safari/537.36'
    }
    
    def __init__(self, api_key: str, corpcode_filename: str = "corpcode.json"):
        """크롤러를 초기화합니다.
        
        Args:
            code: 기업 코드 (기본값: 삼성전자)
        """
        self.api_key = api_key
        self.client = OpenDartClient(self.api_key)

        # 파서 초기화
        self._document_parser = OpenDartDocumentParser(self.client)
        self._document_viewer = OpenDartDocumentViewer(self.client)
        self._disclosure_parser = OpenDartDisclosureParser(self.client)
        self._finance_parser = OpenDartFinanceParser(self.client)
        self._material_facts_parser = OpenDartMaterialFactsParser(self.client)
        self._ownership_parser = OpenDartOwnershipParser(self.client)
        self._registration_parser = OpenDartRegistrationParser(self.client)
        self._reports_parser = OpenDartReportsParser(self.client)
        self._corp_data : Optional[list] = None

        if os.path.exists(corpcode_filename):
            self.load_corp_data(corpcode_filename)
        else:
            result = self._disclosure_parser.fetch_corp_code()
            for data in result.get("xml_data"):
                filename = data.get("filename", "")
                print(f"Filename: {filename}")
                if filename != "CORPCODE.xml":
                    continue
                
                content = data.get("content", "")
                self._corp_data = content.get("result", {}).get("list", [])                

    @property
    def corp_data(self) -> list[dict]:
        """Lazy initialization of corpcode data"""
        if self._corp_data is None:
            result = self._disclosure_parser.fetch_corp_code()
            for data in result.get("xml_data"):
                filename = data.get("filename", "")
                print(f"Filename: {filename}")
                if filename != "CORPCODE.xml":
                    continue
                
                content = data.get("content", "")
                corp_list = content.get("result", {}).get("list", [])
                # "stock_code" 값이 있는 항목만 필터링
                listed = list(filter(lambda x: x["stock_code"], corp_list))
                self._corp_data = listed
        return self._corp_data

    def load_corp_data(self, filename: str):
        with open(filename, "r", encoding="utf-8") as json_file:
            self._corp_data = json.load(json_file)

    def save_corp_data(self, filename: str):
        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(self._corp_data, json_file, ensure_ascii=False, indent=4)

    def duplicate_keys(self):
        seen,duplicates = duplicate_keys(DISCLOSURE_COLUMNS)
        print(f"Disclosure Columns: {seen}")
        seen,duplicates = duplicate_keys(REPORTS_COLUMNS)
        print(f"Reports Columns: {seen}")
        seen,duplicates = duplicate_keys(FINANCE_COLUMNS)
        print(f"Finance Columns: {seen}")
        seen,duplicates = duplicate_keys(OWNERSHIP_COLUMNS)
        print(f"Ownership Columns: {seen}")
        seen,duplicates = duplicate_keys(MATERIAL_FACTS_COLUMNS)
        print(f"Material Facts Columns: {seen}")
        seen, duplicates = duplicate_keys(REGISTRATION_COLUMNS)
        print(f"Registration Columns: {seen}")

    def fetch_corp_code(self, company: str, limit: int = 10, flags: int = re.IGNORECASE) -> str:
        """
        corpcode.json 파일이 없으면 fetch_corp_code를 호출하여 데이터를 가져옵니다.
        corpcode.json 파일이 있으면 corp_data를 가져옵니다.
        corp_data를 통해 회사이름 또는 코드를 통해 corp_code를 찾습니다.
        """
        self.corp_data
        
        corp_code = self._fetch_corp_code_by_name(company, limit, flags)
        if corp_code:
            return corp_code

        corp_code = self._fetch_corp_code_by_stock(company) 

        return corp_code

    def fetch_stock_code(self, company: str, limit: int = 10, flags: int = re.IGNORECASE) -> str:
        """
        corpcode.json 파일이 없으면 fetch_corp_code를 호출하여 데이터를 가져옵니다.
        corpcode.json 파일이 있으면 corp_data를 가져옵니다.
        corp_data를 통해 회사이름 또는 코드를 통해 corp_code를 찾습니다.
        """
        self.corp_data
        
        stock_code = self._fetch_stock_code_by_name(company, limit, flags)

        return stock_code

    def reports(self, corp_code: str, year: str, quarter: int, api_no: int = -1, api_type: str = None):
        return self._reports_parser.fetch(corp_code, year, quarter, api_no, api_type)

    def ownership(self, corp_code: str, api_no: int = -1, api_type: str = None):
        return self._ownership_parser.fetch(corp_code, api_no, api_type)

    def material_facts(self, corp_code: str, start_date: str, end_date: str, api_no: int = -1, api_type: str = None):
        return self._material_facts_parser.fetch(corp_code, start_date, end_date, api_no, api_type)

    def registration(self, corp_code: str, start_date: str, end_date: str, api_no: int = -1, api_type: str = None):
        return self._registration_parser.fetch(corp_code, start_date, end_date, api_no, api_type)

    def merge(self, corp_code: str, start_date: str, end_date: str):
        """OpenDart 합병 현황"""
        api_no = 3
        api_type = "합병"
        return self._registration_parser.fetch(corp_code, start_date, end_date, api_type=api_type)

    def split(self, corp_code: str, start_date: str, end_date: str):
        """OpenDart 분할 현황"""
        api_no = 5
        api_type = "분할"
        return self._registration_parser.fetch(corp_code, start_date, end_date, api_type=api_type)

    def _fetch_stock_code_by_name(self, company: str, limit: int = 10, flags: int = re.IGNORECASE) -> str:
        print(f"Searching for corp_code by name: {company}")
        try:
            regex = re.compile(company, flags)
        except re.error as e:
            print(f"Invalid regex pattern: {e}")
            return []
        
        results = []
        for item in self._corp_data:
            corp_name = item.get("corp_name", "")
            if regex.search(corp_name) and item.get("stock_code") != "":
                results.append({
                    "stock_code": item.get("stock_code", ""),
                    "corp_code": item.get("corp_code", ""),
                    "corp_name": corp_name
                })
                if len(results) >= limit:
                    break

        if len(results) > 0:
            print(f"Found stock_code by name: {results[0].get('stock_code')}")
            return results[0].get("stock_code")

        print(f"No stock_code found for name: {company}")
        return None

    def _fetch_corp_code_by_name(self, company: str, limit: int = 10, flags: int = re.IGNORECASE) -> str:
        print(f"Searching for corp_code by name: {company}")
        try:
            regex = re.compile(company, flags)
        except re.error as e:
            print(f"Invalid regex pattern: {e}")
            return []
        
        results = []
        for item in self._corp_data:
            corp_name = item.get("corp_name", "")
            if regex.search(corp_name) and item.get("stock_code") != "":
                results.append({
                    "stock_code": item.get("stock_code", ""),
                    "corp_code": item.get("corp_code", ""),
                    "corp_name": corp_name
                })
                if len(results) >= limit:
                    break

        if len(results) > 0:
            print(f"Found corp_code by name: {results[0].get('corp_code')}")
            return results[0].get("corp_code")

        print(f"No corp_code found for name: {company}")
        return None

    def _fetch_corp_code_by_stock(self, stock: str, limit: int = 10) -> str:        
        results = []
        for item in self._corp_data:
            stock_code = item.get("stock_code", "")
            if stock_code != "" and stock_code == stock:
                results.append({
                    "stock_code": stock_code,
                    "corp_code": item.get("corp_code", ""),
                    "corp_name": item.get("corp_name", "")
                })
                if len(results) >= limit:
                    break
        
        if len(results) > 0:
            return results[0].get("corp_code")

        return None

    def fetch_corp_name(self, company: str, limit: int = 10) -> str:
        """
        """
        if self._corp_data is None:
            result = self._disclosure_parser.corp_code()
            with open("corpcode.json", "w", encoding="utf-8") as file:
                json.dump(result.get("xml_data")[0].get("list"), file, ensure_ascii=False, indent=4)
            self._corp_data = result.get("xml_data")[0].get("list")
        
        corp_name = self._fetch_corp_name_by_corp_code(company)
        if corp_name:
            return corp_name

        corp_name = self._fetch_corp_name_by_stock(company) 

        return corp_name

    def _fetch_corp_name_by_corp_code(self, corp_code: str, limit: int = 10) -> str:        
        results = []
        for item in self._corp_data:
            code = item.get("corp_code", "")
            if code != "" and corp_code == code:
                results.append({
                    "stock_code": item.get("stock_code", ""),
                    "corp_code": code,
                    "corp_name": item.get("corp_name", "")
                })
                if len(results) >= limit:
                    break
        
        if len(results) > 0:
            return results[0].get("corp_name")

        return None

    def _fetch_corp_name_by_stock(self, stock: str, limit: int = 10) -> str:        
        results = []
        for item in self._corp_data:
            stock_code = item.get("stock_code", "")
            if stock_code != "" and stock_code == stock:
                results.append({
                    "stock_code": stock_code,
                    "corp_code": item.get("corp_code", ""),
                    "corp_name": item.get("corp_name", "")
                })
                if len(results) >= limit:
                    break
        
        if len(results) > 0:
            return results[0].get("corp_name")

        return None
        
    def fetch(
        self,
        code: str | None = None,
        start_date: str = "2025-01-01",
        end_date: str = "2025-08-19",
        count: int = 5,
    ) -> None:
        """기업의 기본 공시 문서를 크롤링합니다.
        
        Args:
            code: 기업 코드 (None이면 초기화 시 설정된 값 사용)
            start_date: 검색 시작일 (YYYY-MM-DD)
            end_date: 검색 종료일 (YYYY-MM-DD)
            count: 크롤링할 최대 문서 수
        """
        if code:
            self.code = code
        
        company_name = None
        for item in self._corp_data:
            stock_code = item.get("stock_code", "")
            corp_code = item.get("corp_code", "")
            if stock_code == self.code or corp_code == self.code:
                company_name = item.get("corp_name", "")
                break
        
        folder_path = f"OpenDart/{company_name}/"
        
        list = self._fetch_list(
            code, start_date, end_date
        )
        #print(list)
        return list
        
    def company(self, code: str):
        """
        OpenDart 공시정보 - 기업개황 (기업 정보 조회)
        corp_code, stock_code으로 조회가 가능하지만 기업명으로는 조회되지 않는다.
        """
        return self._disclosure_parser.company(code)
        
    def finance(self, code: str, year: str, quarter: int = 4, api_type: str = "단일회사 전체 재무제표", indicator_code: str="M210000"):
        """
        OpenDart 정기보고서 재무정보
        단일회사 주요계정
        다중회사 주요계정
        단일회사 전체 재무제표
        XBRL택사노미재무제표양식
        단일회사 주요 재무지표
        다중회사 주요 재무지표
        """
        return self._finance_parser.finance(code, year, quarter, api_type=api_type, indicator_code=indicator_code)

    def finance_file(self, rcept_no, quarter: int = 4, save_path: str | None = None):
        """
        OpenDart 정기보고서 재무정보 - 재무제표 원본파일(XBRL)
        """
        return self._finance_parser.finance_file(rcept_no, quarter, save_path=save_path)

    def _fetch_list(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        df = self._disclosure_parser.list(code, start=start_date, end=end_date)
        df["report_nm"] = df["report_nm"].str.strip()

        return df
    
    def _get_reports_to_process(
        self,
        start_date: str,
        end_date: str,
        folder_path: str,
        count: int,
    ) -> List[Tuple[str, str]]:
        """
        처리할 공시 목록을 가져옵니다.

        Args:
            rcept_no (str):

        Returns:
            Dictionary
        """
        df = self.dart.list(self.code, start=start_date, end=end_date)
        #print(df)
        df["report_nm"] = df["report_nm"].str.strip()

        
        # 이미 다운로드된 파일 확인
        existing_files = self.gcs_manager.list_files(folder_name=folder_path)
        existing_rcept_nos = {
            f.split("_")[-1].replace(".pdf", "") for f in existing_files
        }
        
        # 필터링 조건 적용
        is_new = ~df["rcept_no"].isin(existing_rcept_nos)
        is_not_excluded = ~df["report_nm"].isin(self.EXCLUDED_REPORT_TYPES)
        
        filtered_df = df[is_new & is_not_excluded].head(count)
        
        return list(zip(filtered_df["rcept_no"], filtered_df["flr_nm"]))
    
    def _process_single_report(
        self,
        rcept_no: str,
        flr_nm: str,
        folder_path: str,
    ) -> None:
        """
        단일 공시를 처리합니다.

        Args:
            rcept_no (str):

        Returns:
            Dictionary
        """
        files = self._get_attachment_files(rcept_no)
        
        if not files:
            print(f"첨부파일을 찾지 못했습니다 (접수번호: {rcept_no})")
            return
        
        for filename, url in files.items():
            self._download_and_upload_file(filename, url, rcept_no, folder_path)
    
    def _get_attachment_files(self, rcept_no: str) -> Dict[str, str]:
        """
        첨부파일 목록을 가져옵니다.

        Args:
            rcept_no (str):

        Returns:
            Dictionary
        """
        # OpenDartReader의 내장 메서드 우선 시도
        files = self.dart.attach_files(rcept_no)
        
        if files:
            return files
        
        # 실패 시 직접 파싱 시도
        try:
            return DartDocumentParser.fetch(rcept_no)
        except requests.RequestException as e:
            print(f"네트워크 오류 (접수번호: {rcept_no}): {e}")
        except Exception as e:
            print(f"파싱 오류 (접수번호: {rcept_no}): {e}")
        
        return {}
    
    def _download_and_upload_file(
        self,
        filename: str,
        url: str,
        rcept_no: str,
        folder_path: str,
    ) -> None:
        """
        파일을 다운로드하고 GCS에 업로드합니다.

        Args:
            rcept_no (str):
        """
        # 파일명 정규화
        if not filename:
            filename = url.split("/")[-1]
        
        # HTML 파일 제외
        if filename.endswith(".html"):
            return
        
        # 접수번호를 파일명에 추가
        destination_name = filename.replace(".pdf", f"_{rcept_no}.pdf")
        
        try:
            response = requests.get(url, stream=True, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            self.gcs_manager.upload_file(
                source_file=response.content,
                destination_blob_name=folder_path + destination_name,
            )
        except requests.RequestException as e:
            print(f"파일 다운로드/업로드 실패 ({filename}): {e}")