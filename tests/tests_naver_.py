#!/usr/bin/env python3
"""
Naver Crawler 사용 예시
"""

import os
import pandas as pd

from dotenv import load_dotenv

from sayou.stock.naver import NaverCrawler, NewsArticle

def demo_market(crawler: NaverCrawler, code: str):
    """Naver 주요 시세 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Naver 주요 시세 조회 - {code}")
    print('='*60)

    # Naver 일별 시세 조회
    start_date='2025-01-01'
    end_date='2025-12-31'
    print(f"\nNaver 일별 시세 조회 ({code}) {start_date} ~ {end_date}")

    data = crawler.market(code, start_date=start_date, end_date=end_date)
    print(data)

    # Naver 주요 시세 조회
    print("\nNaver 주요 시세 조회")

    df_main_prices = crawler.main_prices(code)
    print(df_main_prices)

    # Naver 기업 정보 조회
    metadata = crawler.company_metadata(code)
    print(metadata)

def demo_news(crawler: NaverCrawler, code: str):
    """Naver 뉴스 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Naver 뉴스 조회 - {code}")
    print('='*60)

    # Naver 뉴스 카테고리별 검색
    articles = crawler.category_news()
    #print(articles)

    for article in articles:
        print(article)
    
    # Naver 뉴스 검색
    query="삼성전자"
    print(f"\nNaver 뉴스 검색: {query}")

    articles = crawler.news(query=query, max_articles=10)
    #print(articles)

    for article in articles:
        print(article)

def main(stock: str):
    """메인 데모 실행"""
    
    load_dotenv()

    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')

    # Naver에서 요구하는 User-Agent 설정
    crawler = NaverCrawler(client_id, client_secret)

    # 각 파일링 타입 데모
    demo_market(crawler, stock)
    demo_news(crawler, stock)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    code = "005930" # 삼성전자
    main(code)
