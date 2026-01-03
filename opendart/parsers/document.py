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
 
import requests
from bs4 import BeautifulSoup, Tag

from ..client import OpenDartClient

from ..utils import (
    decode_euc_kr,
    API_URL,
    MAIN_URL,
    PDF_URL,
    PDF_MAIN_URL,
    VIEWER_URL
)

class OpenDartDocumentParser:
    """DART 문서 파싱 클래스"""
    
    def __init__(self, client: OpenDartClient):
        self.client = client
    
    def fetch(self, rcp_no: str) -> dict[str, str]:
        """공시 번호로부터 첨부파일 목록을 가져옵니다.
        
        Args:
            rcp_no: 접수번호(rcept_no)
            
        Returns:
            파일명과 다운로드 URL의 딕셔너리
        """

        params = {
            'rcpNo': rcp_no
        }
        print(f"OpenDart URL: {MAIN_URL}")
        
        response = self.client._get(MAIN_URL, params=params)

        toc = self._extract_toc(response.text)
        print(toc)
        """
        [
        {'text': '임원ㆍ주요주주 특정증권등 소유상황보고서', 'id': '1', 'rcpNo': '20251201000783', 'dcmNo': '10906378', 'eleId': '1', 'offset': '689', 'length': '2509', 'dtd': 'dart4.xsd', 'tocNo': '1', 'atocId': '1'}, 
        {'text': '1. 발행회사에 관한 사항', 'id': '2', 'rcpNo': '20251201000783', 'dcmNo': '10906378', 'eleId': '2', 'offset': '3477', 'length': '1495', 'dtd': 'dart4.xsd', 'tocNo': '2', 'atocId': '2'}, 
        {'text': '2. 보고자에 관한 사항', 'id': '3', 'rcpNo': '20251201000783', 'dcmNo': '10906378', 'eleId': '3', 'offset': '4976', 'length': '6149', 'dtd': 'dart4.xsd', 'tocNo': '3', 'atocId': '3'}, 
        {'text': '3. 특정증권등의 소유상황', 'id': '4', 'rcpNo': '20251201000783', 'dcmNo': '10906378', 'eleId': '4', 'offset': '11129', 'length': '14392', 'dtd': 'dart4.xsd', 'tocNo': '4', 'atocId': '4'}
        ]
        """

        viewer = DartDocumentViewer()
        for content in toc:
            result = viewer.fetch(content)

        pdf_info = self._extract_pdf_info(response.text)
        print(pdf_info)

        download_path = self._download_file(pdf_info)
        
        return self._parse_download_page(pdf_info)

    def _extract_toc(self, html_text: str):
        """
        DART 공시 페이지에서 목차(treeData) 추출
        
        Args:
            rcp_no: 접수번호 (예: "20251201000783")
        
        Returns:
            list: treeData 목록
        """
        
        # makeToc 함수 영역 추출
        toc_match = re.search(r'function makeToc\(\)\s*\{(.*?)\n\s*//js tree', html_text, re.DOTALL)
        
        if not toc_match:
            print("makeToc 함수를 찾을 수 없습니다.")
            return []
        
        toc_code = toc_match.group(1)
        
        # treeData 추출
        tree_data = []
        fields = ['text', 'id', 'rcpNo', 'dcmNo', 'eleId', 'offset', 'length', 'dtd', 'tocNo', 'atocId']
        
        # 각 node 블록 분리
        blocks = toc_code.split('treeData.push(node1);')
        
        for block in blocks[:-1]:
            node = {}
            for field in fields:
                # node1['field'] = "value"; 패턴 매칭
                pattern = rf"node1\['{field}'\]\s*=\s*\"([^\"]*)\""
                match = re.search(pattern, block)
                if match:
                    node[field] = match.group(1)
            
            if node:
                tree_data.append(node)
        
        return tree_data

    def _extract_pdf_info(self, html_text: str):
        # openPdfDownload('rcpNo', 'dcmNo') 패턴 찾기
        pattern = r"openPdfDownload\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
        match = re.search(pattern, html_text)
        
        if match:
            result = {
                'rcpNo': match.group(1),
                'dcmNo': match.group(2),
                'pdf_url': self.pdf_url.format(rcp_no=match.group(1),dcm_no=match.group(2))
            }
            return result
        
        # 다른 패턴 시도: 변수로 전달되는 경우
        # openPdfDownload(rcpNo, dcmNo) 형태
        pattern2 = r"openPdfDownload\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)"
        match2 = re.search(pattern2, html)
        
        if match2:
            # 변수 값 찾기
            var1_pattern = rf"var\s+{match2.group(1)}\s*=\s*['\"]([^'\"]+)['\"]"
            var2_pattern = rf"var\s+{match2.group(2)}\s*=\s*['\"]([^'\"]+)['\"]"
            
            var1_match = re.search(var1_pattern, html)
            var2_match = re.search(var2_pattern, html)
            
            if var1_match and var2_match:
                result = {
                    'rcpNo': var1_match.group(1),
                    'dcmNo': var2_match.group(1),
                    'pdf_url': self.pdf_url.format(rcp_no=var1_match.group(1),dcm_no=var2_match.group(1))
                }
                return result
        
        # rcpNo, dcmNo 직접 추출 시도
        rcp_pattern = r"rcpNo\s*[=:]\s*['\"]?(\d+)['\"]?"
        dcm_pattern = r"dcmNo\s*[=:]\s*['\"]?(\d+)['\"]?"
        
        rcp_match = re.search(rcp_pattern, html)
        dcm_match = re.search(dcm_pattern, html)
        
        if rcp_match and dcm_match:
            result = {
                'rcpNo': rcp_match.group(1),
                'dcmNo': dcm_match.group(1),
                'pdf_url': self.pdf_url.format(rcp_no=rcp_match.group(1),dcm_no=dcm_match.group(1))
            }
            return result
        
        return None

    def _download_file(self, pdf_info: dict, save_path: str = None):
        """
        DART 공시 PDF 다운로드
        
        Args:
            pdf_info: Dictionary (접수번호, 문서번호)
            save_path: 저장 경로 (None이면 자동 생성)
        """

        rcp_no = pdf_info.get("rcpNo")
        dcm_no = pdf_info.get("dcmNo")
        
        headers = self.headers
        headers['Referer'] = self.main_url.format(rcp_no=rcp_no)

        """
        https://dart.fss.or.kr/pdf/download/main.do?rcp_no=20251030800076&dcm_no=10857989
        https://dart.fss.or.kr/pdf/download/zip.do?rcp_no=20251030800076&dcm_no=10857989
        https://dart.fss.or.kr/pdf/download/pdf.do?rcp_no=20251201000615&dcm_no=10905849
        """
        """
        https://dart.fss.or.kr/pdf/download/pdf.do?rcp_no=20251030800076&dcm_no=10857989
        """

        download_urls = ['pdf.do', 'zip.do']

        for download_url in download_urls:
            url = self.pdf_url.format(rcp_no=rcp_no, dcm_no=dcm_no).replace('pdf.do',download_url)
            print(f"Downloading file: {url}")
            save_path = self.__download_file(url, headers, save_path)
            if save_path:
                break

        return save_path

    def __download_file(self, url, headers, save_path):
        response = self.client._get(url)
        
        if response.status_code != 200:
            print(f"다운로드 실패: {response.status_code}")
            return None
        
        response_headers = response.headers
        content_length = response_headers.get("Content-Length")
        #print(response_headers, content_length)

        if content_length == '0':
            print(f"다운로드 형색 변경 필요. {response.status_code}")
            return None
        
        if save_path is None:
            content_disposition = response_headers.get("Content-Disposition")
            # filename="..." 또는 filename*=UTF-8''... 패턴 찾기
            match = re.search(r"filename\*?=['\"]?(?:UTF-8'')?([^'\";\n]+)", content_disposition)
            if match:
                filename = match.group(1)

                # URL 인코딩된 경우
                if '%' in filename:
                    filename = unquote(filename)
                
                # 깨진 인코딩 복원
                save_path = decode_euc_kr(filename)
            else:
                save_path = f"dart_{rcp_no}_{dcm_no}.pdf"
    
        with open(save_path, 'wb') as f:
            f.write(response.content)
    
        print(f"PDF 저장 완료: {save_path}")
        return save_path
    
    def _parse_download_page(self, pdf_info: dict) -> dict[str, str]:
        """
        다운로드 페이지에서 첨부파일 목록을 파싱합니다.
        
        Args:
            pdf_info: Dictionary (접수번호, 문서번호)
            
        Returns:
            파일명과 다운로드 URL의 딕셔너리
        """
        rcp_no = pdf_info.get("rcpNo")
        dcm_no = pdf_info.get("dcmNo")
        
        params = {
            'rcp_no': rcp_no,
            'dcm_no': dcm_no
        }

        response = self.client._get(self.pdf_main_url, params=params)

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table")
        
        if not isinstance(table, Tag):
            return {}
        
        return self._extract_files_from_table(table)
    
    def _extract_files_from_table(self, table: Tag) -> dict[str, str]:
        """테이블에서 파일 정보를 추출합니다."""
        files: dict[str, str] = {}
        
        tbody = table.find("tbody")
        if not isinstance(tbody, Tag):
            return files
        
        for row in tbody.find_all("tr"):
            if not isinstance(row, Tag):
                continue
            
            file_info = self._extract_file_from_row(row)
            if file_info:
                filename, url = file_info
                files[filename] = url
        
        return files
    
    def _extract_file_from_row(self, row: Tag) -> tuple[str, str] | None:
        """테이블 행에서 파일 정보를 추출합니다."""
        cells = row.find_all("td")
        if len(cells) < 2:
            return None
        
        filename_cell, link_cell = cells[0], cells[1]
        if not (isinstance(filename_cell, Tag) and isinstance(link_cell, Tag)):
            return None
        
        anchor = link_cell.find("a")
        if not isinstance(anchor, Tag):
            return None
        
        href = anchor.get("href")
        if href is None:
            return None
        
        filename = filename_cell.get_text(strip=True)
        url = f"http://dart.fss.or.kr{href}"
        
        return filename, url