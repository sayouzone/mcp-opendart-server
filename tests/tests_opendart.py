#!/usr/bin/env python3
"""
OpenDart Crawler 사용 예시
"""

import os
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv

from sayou.stock.opendart import OpenDartCrawler
from sayou.stock.opendart.utils import (
    REPORT_ITEMS,
    OWNERSHIP_ITEMS,
    MATERIAL_FACTS_ITEMS,
    REGISTRATION_ITEMS
)

def year_and_quarter(year: int, quarter: int):
    now = datetime.now()
    q = (now.month - 1) // 3
    default_year, default_quarter = (now.year - 1, 4) if q == 0 else (now.year, q)
    
    year = year or default_year
    quarter = quarter or (4 if year < now.year else default_quarter)
    return year, quarter

def demo_corp_code(crawler: OpenDartCrawler, code: str):
    """DART의 기업코드을 조회 데모"""
    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    print('='*60)

    #corp_code = crawler.fetch_corp_code("삼성전자")
    #print(corp_code)
    corp_code = crawler.fetch_corp_code(code)

    print(f"기업코드: {corp_code}")

def demo_base_documents(crawler: OpenDartCrawler, code: str):
    """기업의 기본 공시 문서 조회 데모"""
    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    print('='*60)

    corp_code = crawler.fetch_corp_code(code)
    data = crawler.company(corp_code) # 삼성전자, 005930, 00126380
    #ata = crawler.company(code) # 조회 안됨
    print(data, type(data))

    df = crawler.fetch(code)
    print(df)
    #print(df.to_string())
    #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #    print(df)

def demo_finance(crawler: OpenDartCrawler, corp_code: str):
    """정기보고서 재무정보 데모"""
    print(f"\n{'='*60}")
    print(f"정기보고서 재무정보 조회 - {code}")
    print('='*60)

    rcept_no = None


    year = 2024
    quarter = 4
    year, quarter = year_and_quarter(year, quarter)

    corp_name = crawler.fetch_corp_name(corp_code)

    # 단일회사 주요계정
    api_type = "단일회사 주요계정"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    # 다중회사 주요계정
    api_type = "다중회사 주요계정"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    # 단일회사 전체 재무제표
    api_type = "단일회사 전체 재무제표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    data = crawler.finance(corp_code, year, api_type=api_type)
    #print(data)
    outputs = []
    for item in data:
        outputs.append(item.to_dict())
    print(outputs)
    
    # 단일회사 주요 재무지표
    api_type = "단일회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    data = crawler.finance(corp_code, year, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    # 다중회사 주요 재무지표
    api_type = "다중회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, api_type=api_type)
    #print(data)
    for item in data:
        print(item)

    # 단일회사 주요계정
    api_type = "단일회사 주요계정"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    # 다중회사 주요계정
    api_type = "다중회사 주요계정"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    # 단일회사 전체 재무제표
    api_type = "단일회사 전체 재무제표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    # 수익성지표 : M210000 안정성지표 : M220000 성장성지표 : M230000 활동성지표 : M240000
    indicator_code = "M210000"

    # 단일회사 주요 재무지표
    api_type = "단일회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    api_type = "다중회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
    #print(data)
    for item in data:
        print(item)

    api_type = "다중회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type)
    #print(data)
    for item in data:
        print(item)
    
    indicator_code = "M220000"

    # 단일회사 주요 재무지표
    api_type = "단일회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    for item in data:
        print(item)
    
    # 다중회사 주요 재무지표
    api_type = "다중회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    for item in data:
        print(item)
    
    indicator_code = "M230000"

    # 단일회사 주요 재무지표
    api_type = "단일회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    for item in data:
        print(item)
    
    # 다중회사 주요 재무지표
    api_type = "다중회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    for item in data:
        print(item)
    
    indicator_code = "M240000"

    # 단일회사 주요 재무지표
    api_type = "단일회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    for item in data:
        print(item)

    # 다중회사 주요 재무지표
    api_type = "다중회사 주요 재무지표"
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.finance(corp_code, year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    for item in data:
        print(item)
    
    if data and len(data) > 0:
        rcept_no = data[0].rcept_no
        print(f"\n접수번호: {rcept_no}")
        print(f"\n{year}년 {quarter}분기 접수번호: {rcept_no}")

    return rcept_no

def demo_download_xbrl(crawler: OpenDartCrawler, rcept_no: str = None):
    """OpenDart 정기보고서 재무정보 - 재무제표 원본파일(XBRL). 다운로드"""
    print(f"\n{'='*60}")
    print(f"OpenDart 정기보고서 재무정보 - 재무제표 원본파일(XBRL) 다운로드 - {rcept_no}")
    print('='*60)

    #rcept_no = "20190401004781"
    rcept_no = "20250814003156" if not rcept_no else rcept_no
    save_path = crawler.finance_file(rcept_no, quarter = 4)
    
    if not save_path:
        print(f"파일이 존재하지 않습니다. {rcept_no}")
    else:
        print(f"저장 경로: {save_path}")

def demo_reports(crawler: OpenDartCrawler, corp_code: str):
    """정기보고서 주요정보 데모"""
    print(f"\n{'='*60}")
    print(f"정기보고서 주요정보 조회 - {code}")
    print('='*60)

    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    corp_name = crawler.fetch_corp_name(corp_code)

    # 정기보고서 주요정보 조회
    for item, value in REPORT_ITEMS.items():
        #print(item, value)
        api_type, api_desc = value
        
        year = "2024"
        quarter = 4
        api_no = int(item) - 1
        api_info = f"\n{api_type} ({corp_name}, {corp_code})"
        print(api_info)
        print('-'*(int(len(api_info)*1.5)))

        data = crawler.reports(corp_code, year=year, quarter=quarter, api_no=api_no)
        print(data)

def demo_ownership(crawler: OpenDartCrawler, corp_code: str):
    """지분공시 종합정보 데모"""
    print(f"\n{'='*60}")
    print(f"지분공시 종합정보를 조회 - {code}")
    print('='*60)
    corp_name = crawler.fetch_corp_name(corp_code)

    rcept_no = None

    # 대량보유 상황보고 현황
    api_no = 0
    api_type, api_desc = OWNERSHIP_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.ownership(corp_code, api_no=api_no)
    print(data)

    # 임원ㆍ주요주주 소유보고 현황
    api_no = 1
    api_type, api_desc = OWNERSHIP_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.ownership(corp_code, api_no=api_no)
    print(data)

    if data and len(data) > 0:
        rcept_no = data[0].rcept_no
        print(f"\n접수번호: {rcept_no}")
    
def demo_material_facts(crawler: OpenDartCrawler, corp_code: str):
    """주요사항보고서 주요정보 데모"""
    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 주요사항보고서 주요정보를 조회 - {code}")
    print('='*60)
    corp_name = crawler.fetch_corp_name(corp_code)

    rcept_no = None

    # 자산양수도(기타), 풋백옵션 현황
    corp_code = "00409681"
    corp_name = crawler.fetch_corp_name(corp_code)
    start_date = "20190101"
    end_date = "20251231"
    api_no = 0

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 부도발생 현황
    corp_code = "00112819"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 1

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 영업정지 현황
    corp_code = "00370006"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 2

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 회생절차 개시신청 현황
    corp_code = "00367482"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 3

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 해산사유 발생 현황
    corp_code = "01102590"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 4

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 유상증자 결정 현황
    corp_code = "00378363"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 5

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 무상증자 결정 현황
    corp_code = "00121932"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 6

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 유무상증자 결정 현황
    corp_code = "00359395"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 7

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 감자 결정 현황
    corp_code = "00121932"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 8

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 채권은행 등의 관리절차 개시 현황
    corp_code = "00295857"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 9

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 소송 등의 제기 현황
    corp_code = "00164830"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 10

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 해외 증권시장 주권등 상장 결정 현황
    corp_code = "00258801"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 11

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 해외 증권시장 주권등 상장폐지 결정 현황
    corp_code = "00344287"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 12

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 해외 증권시장 주권등 상장 현황
    corp_code = "01350869"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 13

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 해외 증권시장 주권등 상장폐지 현황
    corp_code = "00344287"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 14

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 전환사채권 발행결정 현황
    corp_code = "00155355"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 15

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 신주인수권부사채권 발행결정 현황
    corp_code = "00140131"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 16

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 교환사채권 발행결정 현황
    corp_code = "00273420"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 17

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 채권은행 등의 관리절차 중단 현황
    corp_code = "00141608"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 18

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 상각형 조건부자본증권 발행결정 현황
    corp_code = "00382199"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 19

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 자기주식 취득 결정 현황
    corp_code = "00164742"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 20

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 자기주식 처분 결정
    corp_code = "00121932"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 21

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 자기주식취득 신탁계약 체결 결정 현황
    corp_code = "00860332"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 22

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 자기주식취득 신탁계약 해지 결정 현황
    corp_code = "00382199"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 23

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 영업양수 결정 현황
    corp_code = "00140131"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 24

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 영업양도 결정 현황
    corp_code = "00131780"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 25

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 유형자산 양수 결정
    corp_code = "00160375"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 26

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    #유형자산 양도 결정 현황
    corp_code = "00106395"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 27

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info  = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 타법인 주식 및 출자증권 양수결정 현황
    corp_code = "00140131"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 28

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 타법인 주식 및 출자증권 양도결정 현황
    corp_code = "00230814"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 29

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 주권 관련 사채권 양수 결정 현황
    corp_code = "00173449"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 30

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 주권 관련 사채권 양도 결정 현황
    corp_code = "00125965"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 31

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 회사합병 결정 현황
    corp_code = "00155319"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 32

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 회사분할 결정 현황
    corp_code = "00266961"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 33

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 회사분할합병 결정 현황
    corp_code = "00306135"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 34

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

    # 주식교환·이전 결정 현황
    corp_code = "00219097"
    corp_name = crawler.fetch_corp_name(corp_code)
    api_no = 35

    api_type, api_desc = MATERIAL_FACTS_ITEMS.get(str(api_no + 1))
    api_info = f"\n\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.material_facts(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data)

def demo_registration(crawler: OpenDartCrawler, corp_code: str):
    """증권신고서 주요정보 데모"""

    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 증권신고서 주요정보를 조회 - {code}")
    print('='*60)
    #corp_name = crawler.fetch_corp_name(corp_code)
    #start_date = "20150101"
    #end_date = "20251231"

    rcept_no = None

    # 증자(감자) 현황
    corp_code = "00106395"
    corp_name = crawler.fetch_corp_name(corp_code)
    start_date = "20190101"
    end_date = "20251231"

    api_no = 0

    api_type, api_desc = REGISTRATION_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data.generals)
    print(data.stocks)
    print(data.acquirers)
    print(data.purposes)
    print(data.shareholders)
    print(data.put_back_options)

    # 채무증권 현황
    corp_code = "00858364"
    corp_name = crawler.fetch_corp_name(corp_code)
    start_date = "20190101"
    end_date = "20251231"

    api_no = 1

    api_type, api_desc = REGISTRATION_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data.generals)
    print(data.acquirers)
    print(data.purposes)
    print(data.shareholders)

    # 증권예탁증권 현황
    corp_code = "01338724"
    corp_name = crawler.fetch_corp_name(corp_code)
    start_date = "20190101"
    end_date = "20251231"

    api_no = 2

    api_type, api_desc = REGISTRATION_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    print(data.generals)
    print(data.stocks)
    print(data.acquirers)
    print(data.purposes)
    print(data.shareholders)

    # 합병 현황    
    corp_code = "00109718"
    corp_name = crawler.fetch_corp_name(corp_code)
    print(corp_code, corp_name)
    start_date = "20190101"
    end_date = "20251231"

    api_no = 3

    api_desc = REGISTRATION_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.merge(corp_code, start_date=start_date, end_date=end_date)
    print(data.generals)
    print(data.issued_securities)
    print(data.companies)

    # 분할 현황    
    corp_code = "00105271"
    corp_name = crawler.fetch_corp_name(corp_code)
    print(corp_code, corp_name)
    start_date = "20190101"
    end_date = "20251231"

    api_no = 5

    api_type, api_desc = REGISTRATION_ITEMS.get(str(api_no + 1))
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.split(corp_code, start_date=start_date, end_date=end_date)
    print(data.generals)
    print(data.issued_securities)
    print(data.companies)

def main(code: str):
    """메인 데모 실행"""
    
    load_dotenv()
    dart_api_key = os.getenv("DART_API_KEY", "")

    corpcode_filename = "corpcode.json"

    # OpenDart에서 요구하는 User-Agent 설정
    crawler = OpenDartCrawler(api_key=dart_api_key, )
    corp_data = crawler.corp_data
    #crawler.save_corp_data(corpcode_filename)

    # 회사이름으로 corp_code 검색
    #company_name = "삼성전자"
    #corp_code = crawler.fetch_corp_code(company_name)
    corp_code = crawler.fetch_corp_code(code)
    if not corp_code:
        print(f"Could not find corp_code for {code}")
        return

    print(f"\n{code} corp_code: {corp_code}")

    # 각 파일링 타입 데모
    demo_corp_code(crawler, code)
    demo_base_documents(crawler, code)
    rcept_no = demo_finance(crawler, corp_code)
    # 00126380 삼성전자 005930 반기보고서 (2025.06) 20250814003156 20250814
    rcept_no = rcept_no or "20251114002447"
    demo_download_xbrl(crawler, rcept_no=rcept_no)
    demo_reports(crawler, corp_code)
    demo_ownership(crawler, corp_code)
    demo_material_facts(crawler, corp_code)
    demo_registration(crawler, corp_code)
    #crawler.duplicate_keys()
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    code = "005930" # 삼성전자
    main(code)
