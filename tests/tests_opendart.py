#!/usr/bin/env python3
"""
OpenDart Crawler 사용 예시
"""

import os
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv

from sayou.stock.opendart import OpenDartCrawler
from sayou.stock.opendart.models import (
    IndexClassCode,
    FinanceStatus,
    ReportStatus,
    OwnershipStatus,
    MaterialFactStatus,
    RegistrationStatus,
)
from sayou.stock.opendart.utils import (
    REPORT_ITEMS,
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
    data = crawler.company(corp_code)
    print(data, type(data))

    df = crawler.fetch(code)
    print(df)

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
    api_no = FinanceStatus.SINGLE_COMPANY_MAIN_ACCOUNTS
    api_info = f"\n{api_no.display_name} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    #data = crawler.finance(corp_code, year, api_type=api_type)
    data = crawler.single_company_main_accounts(corp_code, year, quarter)
    #print(data)
    for item in data:
        print(item)
    
    # 다중회사 주요계정
    api_no = FinanceStatus.MULTI_COMPANY_MAIN_ACCOUNTS
    api_info = f"\n{api_no.display_name} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    #data = crawler.finance(corp_code, year, api_type=api_type)
    data = crawler.multi_company_main_accounts(corp_code, year, quarter)
    #print(data)
    for item in data:
        print(item)
    
    # 단일회사 전체 재무제표
    api_no = FinanceStatus.SINGLE_COMPANY_FINANCIAL_STATEMENT
    api_info = f"\n{api_no.display_name} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    #data = crawler.finance(corp_code, year, api_type=api_type)
    data = crawler.single_company_financial_statements(corp_code, year, quarter)
    #print(data)
    for item in data:
        print(item)
        rcept_no = item.rcept_no

    print(f"\n{'='*60}")
    print(f"재무제표 원본파일(XBRL) 다운로드 - {rcept_no}")
    print('='*60)

    rcept_no = rcept_no or "20250814003156"
    save_path = crawler.finance_file(rcept_no, quarter = 4)
    
    if not save_path:
        print(f"파일이 존재하지 않습니다. {rcept_no}")
    else:
        print(f"저장 경로: {save_path}")

    # 단일회사 주요 재무지표
    api_no = FinanceStatus.SINGLE_COMPANY_KEY_FINANCIAL_INDICATOR
    api_info = f"\n{api_no.display_name} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    for indicator_code in IndexClassCode:
        #data = crawler.finance(corp_code, year, api_type=api_type)
        data = crawler.single_company_key_financial_indicators(corp_code, year, quarter, indicator_code)
        #print(data)
        for item in data:
            print(item)

    # 다중회사 주요 재무지표
    api_no = FinanceStatus.MULTI_COMPANY_KEY_FINANCIAL_INDICATOR
    api_info = f"\n{api_no.display_name} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))
    
    for indicator_code in IndexClassCode:
        data = crawler.multi_company_key_financial_indicators(corp_code, year, quarter, indicator_code)
        #print(data)
        for item in data:
            print(item)

def demo_reports(crawler: OpenDartCrawler, corp_code: str):
    """정기보고서 주요정보 데모"""
    print(f"\n{'='*60}")
    print(f"정기보고서 주요정보 조회 - {code}")
    print('='*60)

    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    corp_name = crawler.fetch_corp_name(corp_code)

    
    # 정기보고서 주요정보 조회
    for api_no in ReportStatus:
        api_info = f"\n{api_no.display_name} ({corp_name}, {corp_code})"
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
    api_no = OwnershipStatus.MAJOR_OWNERSHIP
    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    #data = crawler.ownership(corp_code, api_no=api_no)
    data = crawler.major_ownership(corp_code)
    print(data)

    # 임원ㆍ주요주주 소유보고 현황
    api_no = OwnershipStatus.INSIDER_OWNERSHIP
    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    #data = crawler.ownership(corp_code, api_no=api_no)
    data = crawler.major_ownership(corp_code)
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

    corp_codes = {
        MaterialFactStatus.PUT_OPTION.value: "00409681",
        MaterialFactStatus.BANKRUPTCY.value: "00112819",
        MaterialFactStatus.SUSPENSION.value: "00370006",
        MaterialFactStatus.RESTORATION.value: "00367482",
        MaterialFactStatus.DISSOLUTION.value: "01102590",
        MaterialFactStatus.PUBLIC_ISSUANCE.value: "00378363",
        MaterialFactStatus.UNPUBLIC_ISSUANCE.value: "00121932",
        MaterialFactStatus.PUBLIC_UNPUBLIC_ISSUANCE.value: "00359395",
        MaterialFactStatus.CAPITAL_REDUCTION.value: "00121932",
        MaterialFactStatus.BANKRUPTCY_PROCEDURE.value: "00295857",
        MaterialFactStatus.LEGAL_ACT.value: "00164830",
        MaterialFactStatus.OVERSEAS_LISTING_DECISION.value: "00258801",
        MaterialFactStatus.OVERSEAS_DELISTING_DECISION.value: "00344287",
        MaterialFactStatus.OVERSEAS_LISTING.value: "01350869",
        MaterialFactStatus.OVERSEAS_DELISTING.value: "00344287",
        MaterialFactStatus.CB_ISSUANCE_DECISION.value: "00155355",
        MaterialFactStatus.BW_ISSUANCE_DECISION.value: "00140131",
        MaterialFactStatus.EB_ISSUANCE_DECISION.value: "00273420",
        MaterialFactStatus.BANKRUPTCY_PROCEDURE_SUSPENSION.value: "00141608",
        MaterialFactStatus.COCO_BOND_ISSUANCE_DECISION.value: "00382199",
        MaterialFactStatus.SHARE_BUYBACK_DECISION.value: "00164742",
        MaterialFactStatus.TREASURY_STOCK_DISPOSAL_DECISION.value: "00121932",
        MaterialFactStatus.TRUST_AGREEMENT_ACQUISITION_DECISION.value: "00860332",
        MaterialFactStatus.TRUST_AGREEMENT_RESOLUTION_DECISION.value: "00382199",
        MaterialFactStatus.BUSINESS_ACQUISITION_DECISION.value: "00140131",
        MaterialFactStatus.BUSINESS_TRANSFER_DECISION.value: "00131780",
        MaterialFactStatus.ASSET_ACQUISITION_DECISION.value: "00160375",
        MaterialFactStatus.ASSET_TRANSFER_DECISION.value: "00106395",
        MaterialFactStatus.OTHER_SHARE_ACQUISITION_DECISION.value: "00140131",
        MaterialFactStatus.OTHER_SHARE_TRANSFER_DECISION.value: "00230814",
        MaterialFactStatus.EQUITY_LINKED_BOND_ACQUISITION_DECISION.value: "00173449",
        MaterialFactStatus.EQUITY_LINKED_BOND_TRANSFER_DECISION.value: "00125965",
        MaterialFactStatus.COMPANY_MERGER_DECISION.value: "00155319",
        MaterialFactStatus.COMPANY_SPINOFF_DECISION.value: "00266961",
        MaterialFactStatus.COMPANY_SPINOFF_MERGER_DECISION.value: "00306135",
        MaterialFactStatus.SHARE_EXCHANGE_DECISION.value: "00219097",
    }
    start_date = "20160101"
    end_date = "20251231"

    for api_no in MaterialFactStatus:
        corp_code = corp_codes.get(api_no.value)
        corp_name = crawler.fetch_corp_name(corp_code)

        api_info = f"\n\n{api_no.display_name} ({corp_name}, {corp_code})"
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

    api_no = RegistrationStatus.EQUITY_SHARE

    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    if data:
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

    api_no = RegistrationStatus.DEBT_SHARE

    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    if data:
        print(data.generals)
        print(data.acquirers)
        print(data.purposes)
        print(data.shareholders)

    # 증권예탁증권 현황
    corp_code = "01338724"
    corp_name = crawler.fetch_corp_name(corp_code)
    start_date = "20190101"
    end_date = "20251231"

    api_no = RegistrationStatus.DEPOSITORY_RECEIPT

    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    if data:
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

    api_no = RegistrationStatus.COMPANY_MERGER

    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.company_merger(corp_code, start_date=start_date, end_date=end_date)
    if data:
        print(data.generals)
    print(data.issued_securities)
    print(data.companies)

    # 주식의포괄적교환·이전 현황    
    corp_code = "00219097"
    corp_name = crawler.fetch_corp_name(corp_code)
    print(corp_code, corp_name)
    start_date = "20190101"
    end_date = "20251231"

    api_no = RegistrationStatus.SHARE_EXCHANGE

    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.registration(corp_code, start_date=start_date, end_date=end_date, api_no=api_no)
    if data:
        print(data.generals)
        print(data.issued_securities)
        print(data.companies)

    # 분할 현황    
    corp_code = "00105271"
    corp_name = crawler.fetch_corp_name(corp_code)
    print(corp_code, corp_name)
    start_date = "20190101"
    end_date = "20251231"

    api_no = RegistrationStatus.COMPANY_SPINOFF

    api_type = api_no.display_name
    api_info = f"\n{api_type} ({corp_name}, {corp_code})"
    print(api_info)
    print('-'*(int(len(api_info)*1.5)))

    data = crawler.company_spinoff(corp_code, start_date=start_date, end_date=end_date)
    if data:
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
    demo_finance(crawler, corp_code)
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
