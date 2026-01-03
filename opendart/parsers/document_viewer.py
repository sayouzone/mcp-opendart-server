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

class OpenDartDocumentViewer:
    """DART 문서 뷰어 클래스"""

    def __init__(self, client: OpenDartClient):
        self.client = client
    
    @classmethod
    def fetch(self, content: dict) -> dict[str, str]:
        """공시 정보로부터 문서 정보를 가져옵니다.
        
        Args:
            content: 접수번호(rcept_no)
            
        Returns:
            파일명과 다운로드 URL의 딕셔너리
        """
        """
        {'text': '임원ㆍ주요주주 특정증권등 소유상황보고서', 'id': '1', 'rcpNo': '20251201000783', 'dcmNo': '10906378', 'eleId': '1', 'offset': '689', 'length': '2509', 'dtd': 'dart4.xsd', 'tocNo': '1', 'atocId': '1'}, 
        """
        result = self._parse_viewer(content)
        self._print_result(result)
        
        return result

    @classmethod
    def _parse_viewer(self, content: dict) -> dict[str, str]:
        """
        DART 공시 문서 내용 파싱
        
        Args:
            content (dict): Dictionary(
            rcp_no: 접수번호
            dcm_no: 문서번호
            ele_id: 요소 ID
            offset: 오프셋
            length: 길이
            dtd: DTD 파일명
            )
        
        Returns:
            dict: 파싱된 문서 내용
        """
        rcp_no = content.get("rcpNo")
        referer = f"{MAIN_URL}?rcp_no={rcp_no}"

        response = self.client._get(VIEWER_URL, params=content, referer=referer)
        html = response.text

        soup = BeautifulSoup(html, 'html.parser')
    
        result = {
            'raw_html': html,
            'title': None,
            'tables': [],
            'text_content': None
        }
        
        # 제목 추출
        title_tag = soup.find('title')
        if title_tag:
            result['title'] = title_tag.get_text(strip=True)
        
        # 테이블 추출
        tables = soup.find_all('table')
        for idx, table in enumerate(tables):
            table_data = self._parse_viewer_table(table)
            if table_data:
                result['tables'].append({
                    'index': idx,
                    'data': table_data
                })
        
        # 텍스트 내용 추출
        body = soup.find('body')
        if body:
            result['text_content'] = body.get_text(separator='\n', strip=True)
        
        return result

    @classmethod
    def _parse_viewer_table(self, table):
        """HTML 테이블을 2D 리스트로 변환"""
        rows = []
        
        for tr in table.find_all('tr'):
            row = []
            for cell in tr.find_all(['td', 'th']):
                text = cell.get_text(separator=' ', strip=True)
                text = re.sub(r'\s+', ' ', text)  # 공백 정리
                row.append(text)
            
            if row:
                rows.append(row)
        
        return rows

    @classmethod
    def _print_result(self, result):
        """결과 출력"""
        print("=" * 70)
        print(f"제목: {result['title']}")
        print("=" * 70)
        
        print("\n[텍스트 내용]")
        print("-" * 70)
        if result['text_content']:
            # 처음 1000자만 출력
            content = result['text_content'][:1000]
            print(content)
            if len(result['text_content']) > 1000:
                print(f"\n... (총 {len(result['text_content'])}자)")
        
        print(f"\n[테이블 수: {len(result['tables'])}개]")
        print("-" * 70)
        
        for table in result['tables']:
            print(f"\n테이블 #{table['index'] + 1}")
            for row in table['data'][:5]:  # 처음 5행만
                print(row)
            if len(table['data']) > 5:
                print(f"... (총 {len(table['data'])}행)")