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
 
import os
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

T = TypeVar("T", bound="BaseOpenDartData")

class CorpClass(Enum):
    """법인 구분"""
    KOSPI = "Y"      # 유가증권시장
    KOSDAQ = "K"     # 코스닥
    KONEX = "N"      # 코넥스
    ETC = "E"        # 기타

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional["CorpClass"]:
        """값으로부터 Enum 생성"""
        if not value:
            return None
        for member in cls:
            if member.value == value:
                return member
        return None

    @property
    def display_name(self) -> str:
        """표시명"""
        names = {
            "Y": "유가증권",
            "K": "코스닥",
            "N": "코넥스",
            "E": "기타",
        }
        return names.get(self.value, "알 수 없음")

class ReportCode(Enum):
    """보고서 코드"""
    BUSINESS_REPORT = "11011"      # 사업보고서
    FIRST_QUARTER_REPORT = "11012"     # 1분기보고서
    SECOND_QUARTER_REPORT = "11013"      # 2분기보고서
    THIRD_QUARTER_REPORT = "11014"        # 3분기보고서
    YEAR_REPORT = "11015"        # 사업보고서

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional["ReportCode"]:
        """값으로부터 Enum 생성"""
        if not value:
            return None
        for member in cls:
            if member.value == value:
                return member
        return None

    @property
    def display_name(self) -> str:
        """표시명"""
        names = {
            "11011": "사업보고서",
            "11012": "1분기보고서",
            "11013": "2분기보고서",
            "11014": "3분기보고서",
            "11015": "사업보고서",
        }
        return names.get(self.value, "알 수 없음")

class IndexClassCode(Enum):
    """지표분류코드"""
    PROFITABILITY = "M210000"      # 수익성지표
    STABILITY = "M220000"          # 안정성지표
    GROWTH = "M230000"             # 성장성지표
    ACTIVITY = "M240000"           # 활동성지표

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional["IndexClassCode"]:
        """값으로부터 Enum 생성"""
        if not value:
            return None
        for member in cls:
            if member.value == value:
                return member
        return None

    @property
    def display_name(self) -> str:
        """표시명"""
        names = {
            "M210000": "수익성지표",
            "M220000": "안정성지표",
            "M230000": "성장성지표",
            "M240000": "활동성지표",
        }
        return names.get(self.value, "알 수 없음")

class FinancialStatementCategory(Enum):
    """재무제표 구분"""

    BS1 = "재무상태표", "연결", "유동/비유동법"
    BS2 = "재무상태표", "개별", "유동/비유동법"
    BS3 = "재무상태표", "연결", "유동성배열법"
    BS4 = "재무상태표", "개별", "유동성배열법"
    IS1 = "별개의 손익계산서", "연결", "기능별분류"
    IS2 = "별개의 손익계산서", "개별", "기능별분류"
    IS3 = "별개의 손익계산서", "연결", "성격별분류"
    IS4 = "별개의 손익계산서", "개별", "성격별분류"
    CIS1 = "포괄손익계산서", "연결", "세후"
    CIS2 = "포괄손익계산서", "개별", "세후"
    CIS3 = "포괄손익계산서", "연결", "세전"
    CIS4 = "포괄손익계산서", "개별", "세전"
    
    DCIS1 = "단일 포괄손익계산서", "연결", "기능별분류", "세후포괄손익"
    DCIS2 = "단일 포괄손익계산서", "개별", "기능별분류", "세후포괄손익"
    DCIS3 = "단일 포괄손익계산서", "연결", "기능별분류", "세전"
    DCIS4 = "단일 포괄손익계산서", "개별", "기능별분류", "세전"
    DCIS5 = "단일 포괄손익계산서", "연결", "성격별분류", "세후포괄손익"
    DCIS6 = "단일 포괄손익계산서", "개별", "성격별분류", "세후포괄손익"
    DCIS7 = "단일 포괄손익계산서", "연결", "성격별분류", "세전"
    DCIS8 = "단일 포괄손익계산서", "개별", "성격별분류", "세전"
    
    CF1 = "현금흐름표", "연결", "직접법"
    CF2 = "현금흐름표", "개별", "직접법"
    CF3 = "현금흐름표", "연결", "간접법"
    CF4 = "현금흐름표", "개별", "간접법"
    SCE1 = "자본변동표", "연결"
    SCE2 = "자본변동표", "개별"

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional["FinancialStatementCategory"]:
        """값으로부터 Enum 생성"""
        if not value:
            return None
        for member in cls:
            if member.value == value:
                return member
        return None

    @property
    def display_name(self) -> str:
        """표시명"""
        names = {
            "BS1": "재무상태표",
            "BS2": "재무상태표",
            "BS3": "재무상태표",
            "BS4": "재무상태표",
            
            "IS1": "별개의 손익계산서",
            "IS2": "별개의 손익계산서",
            "IS3": "별개의 손익계산서",
            "IS4": "별개의 손익계산서",
            
            "CIS1": "포괄손익계산서",
            "CIS2": "포괄손익계산서",
            "CIS3": "포괄손익계산서",
            "CIS4": "포괄손익계산서",

            "DCIS1": "단일 포괄손익계산서",
            "DCIS2": "단일 포괄손익계산서",
            "DCIS3": "단일 포괄손익계산서",
            "DCIS4": "단일 포괄손익계산서",
            "DCIS5": "단일 포괄손익계산서",
            "DCIS6": "단일 포괄손익계산서",
            "DCIS7": "단일 포괄손익계산서",
            "DCIS8": "단일 포괄손익계산서",

            "CF1": "현금흐름표 연결 직접법",
            "CF2": "현금흐름표 개별 직접법",
            "CF3": "현금흐름표 연결 간접법",
            "CF4": "현금흐름표 개별 간접법",
            "SCE1": "자본변동표 연결",
            "SCE2": "자본변동표 개별",
        }
        return names.get(self.value, "알 수 없음")

def _convert_value(obj: Any) -> Any:
    """값을 JSON 직렬화 가능한 형태로 변환"""
    if obj is None:
        return None
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, list):
        return [_convert_value(item) for item in obj]
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return obj

@dataclass
class BaseOpenDartData:
    """OpenDart 데이터 베이스 클래스"""
    rcept_no: Optional[str] = None # 접수번호
    corp_code: Optional[str] = None # 고유번호
    corp_cls: Optional[CorpClass] = None # 법인구분
    corp_name: Optional[str] = None # 법인명

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (Enum은 value로 변환)"""
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            result[f.name] = _convert_value(value)
        return result

    @classmethod
    def from_raw_data(cls: Type[T], raw_data: Dict[str, Any]) -> T:
        """딕셔너리에서 데이터 클래스 생성
        
        - 유효한 필드만 추출
        - corp_cls는 CorpClass Enum으로 변환
        """
        valid_fields = {f.name for f in fields(cls)}
        filtered_data = {}
        
        for key, value in raw_data.items():
            if key not in valid_fields:
                continue
            
            # corp_cls 특별 처리
            if key == "corp_cls" and isinstance(value, str):
                filtered_data[key] = CorpClass.from_value(value)
            else:
                filtered_data[key] = value
                
        return cls(**filtered_data)

    @property
    def corp_class_display(self) -> str:
        """법인구분 표시명"""
        if self.corp_cls:
            return self.corp_cls.display_name
        return "알 수 없음"

@dataclass
class BaseFinanceData(BaseOpenDartData):
    """재무제표 데이터 베이스 클래스"""
    reprt_code: Optional[str] = None # 보고서 코드
    bsns_year: Optional[str] = None # 사업 연도
    stock_code: Optional[str] = None # 종목 코드
    sj_div: Optional[str] = None # 재무제표구분	BS:재무상태표, IS:손익계산서
    sj_nm: Optional[str] = None # 재무제표명	ex) 재무상태표 또는 손익계산서 출력
    fs_div: Optional[str] = None # 개별/연결구분	OFS:재무제표, CFS:연결재무제표
    fs_nm: Optional[str] = None # 개별/연결명	ex) 연결재무제표 또는 재무제표 출력
    account_id: Optional[str] = None # 계정ID	계정 고유명칭
    account_nm: Optional[str] = None # 계정명	ex) 자본총계

@dataclass
class BaseOwnershipData(BaseOpenDartData):
    """지분공시 데이터 베이스 클래스"""
    rcept_dt: Optional[str] = None # 접수일자	예) 공시 접수일자(YYYYMMDD)
    repror: Optional[str] = None # 대표보고자

@dataclass
class BaseRegistrationData(BaseOpenDartData):
    """지분공시 데이터 베이스 클래스"""
    tm: Optional[str] = None # 회차