#!/usr/bin/env python3
"""
FnGuide Crawler 사용 예시
"""

import json
import os
import pandas as pd

from dotenv import load_dotenv

from sayou.stock.fnguide import FnGuideCrawler
from sayou.stock.fnguide.models import TableData, KeyValueData, HistoryData

def demo_main(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | Snapshot 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | Snapshot을 조회 - {code}")
    print('='*60)

    data = crawler.main(code)
    print(data)

    for key in data:
        list = data.get(key, [])
        if len(list) > 0:
            df = pd.DataFrame(list)
            print(df)

def demo_company(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 기업개요 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 기업개요를 조회 - {code}")
    print('='*60)

    results = crawler.company(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
        
        elif isinstance(data, HistoryData):
            print(f"헤더: {data.headers}")
            print(f"레코드 수: {len(data.records)}")
            for record in data.records[:2]:
                print(f"  {record}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def demo_finance(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 재무제표 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 재무제표를 조회 - {code}")
    print('='*60)

    # 기업 재무제표 조회
    print(f"\nFnGuide 기업 재무제표 (포괄손익계산서, 재무상태표, 현금흐름표) 조회 ({code})")
    data = crawler.finance(code)
    print(data)

    for item in data:
        key_name = item.get("key_name", "")
        key_type = item.get("key_type", "")
        list = item.get("records", [])
        if len(list) > 0:
            df = pd.DataFrame(list)
            print(f"{key_name} ({key_type})")
            print(df)

    # 기업 연간 포괄손익계산서 조회
    print(f"\nFnGuide 기업 연간 포괄손익계산서 조회 ({code})")
    data = crawler.income_statement(code)
    print(data)

    # 기업 분기별 포괄손익계산서 조회
    print(f"\nFnGuide 기업 분기별 포괄손익계산서 조회 ({code})")
    data = crawler.quarterly_income_statement(code)
    print(data)

    # 기업 연간 재무상태표 조회
    print(f"\nFnGuide 기업 연간 재무상태표 조회 ({code})")
    data = crawler.balance_sheet(code)
    print(data)

    # 기업 분기별 재무상태표 조회
    print(f"\nFnGuide 기업 분기별 재무상태표 조회 ({code})")
    data = crawler.quarterly_balance_sheet(code)
    print(data)

    # 기업 연간 현금흐름표 조회
    print(f"\nFnGuide 기업 연간 현금흐름표 조회 ({code})")
    data = crawler.cash_flow(code)
    print(data)

    # 기업 현금흐름표 조회
    print(f"\nFnGuide 기업 분기별 현금흐름표 조회 ({code})")
    data = crawler.quarterly_cash_flow(code)
    print(data)

def demo_finance_ratio(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 재무비율 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 재무비율을 조회 - {code}")
    print('='*60)

    # 기업 재무비율 조회
    print(f"\nFnGuide 기업 재무비율 (재무비율 누적, 재무비율 3개월) 조회 ({code})")
    data = crawler.finance_ratio(code)
    print(data)

def demo_invest(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 투자지표 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 투자지표를 조회 - {code}")
    print('='*60)

    results = crawler.invest(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def demo_consensus(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 컨센서스 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 컨센서스를 조회 - {code}")
    print('='*60)

    results = crawler.consensus(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def demo_industry(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 업종분석 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 업종분석을 조회 - {code}")
    print('='*60)

    results = crawler.industry(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def demo_comparison(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 경쟁사비교 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 경쟁사비교를 조회 - {code}")
    print('='*60)

    results = crawler.comparison(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def demo_disclosure(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | 거래소공시 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | 거래소공시를 조회 - {code}")
    print('='*60)

    results = crawler.disclosure(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def demo_dart(crawler: FnGuideCrawler, code: str):
    """FnGuide 기업 정보 | Dart 조회 데모"""
    print(f"\n{'='*60}")
    print(f"FnGuide 기업 정보 | Dart를 조회 - {code}")
    print('='*60)

    results = crawler.dart(code)
    print(results)

    for caption, data in results.items():
        print(f"\n### {caption} ###")
        print(f"타입: {type(data).__name__}")
        
        if isinstance(data, TableData):
            print(f"헤더: {data.headers}")
            print(f"항목 수: {len(data.data) + len(data.data_with_category)}")
            
            # 샘플 출력
            if data.data_with_category:
                for (cat, item), values in list(data.data_with_category.items())[:3]:
                    print(f"  [{cat}] {item}: {values}")
            elif data.data:
                for key, values in list(data.data.items())[:3]:
                    print(f"  {key}: {values}")
        
        elif isinstance(data, KeyValueData):
            print(f"항목 수: {len(data.data)}")
            for key, value in list(data.data.items())[:5]:
                print(f"  {key}: {value}")
    
    # JSON 출력
    print("\n" + "=" * 70)
    print("JSON 출력")
    print("=" * 70)
    
    json_output = {
        caption: data.to_dict()
        for caption, data in results.items()
    }
    print(json.dumps(json_output, ensure_ascii=False, indent=2)[:2000] + "...")

def main(stock: str):
    """메인 데모 실행"""
    
    load_dotenv()

    # FnGuide에서 요구하는 User-Agent 설정
    crawler = FnGuideCrawler()

    # 각 파일링 타입 데모
    demo_main(crawler, stock)
    demo_company(crawler, stock)
    demo_finance(crawler, stock)
    demo_finance_ratio(crawler, stock)
    demo_invest(crawler, stock)
    demo_consensus(crawler, stock)
    demo_industry(crawler, stock)
    demo_comparison(crawler, stock)
    demo_disclosure(crawler, stock)
    demo_dart(crawler, stock)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)

if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    code = "005930" # 삼성전자
    main(code)
