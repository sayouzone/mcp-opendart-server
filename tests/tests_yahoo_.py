#!/usr/bin/env python3
"""
Yahoo Crawler 사용 예시
"""

from sayou.stock.yahoo import YahooCrawler

def demo_analysis(crawler: YahooCrawler, ticker: str):
    """Yahoo 주요 분석 조회 데모"""

    print(f"\n{'='*60}")
    print(f"Yahoo 주요 분석 조회 - {ticker}")
    print('='*60)

    ticker = "AAPL"

    # 기업 캘린더 조회
    print(f"\nYahoo 기업 캘린더 조회 ({ticker})")
    data = crawler.calendar(ticker)
    print(data)

    # 기업 수익 추정치 (Earning Estimate) 조회
    print(f"\nYahoo 기업 수익 추정치 (Earning Estimate) 조회 ({ticker})")
    data = crawler.earnings_estimate(ticker)
    print(data)

    # 기업 매출 추정치 (Revenue Estimate) 조회
    print(f"\nYahoo 기업 매출 추정치 (Revenue Estimate) 조회 ({ticker})")
    data = crawler.revenue_estimate(ticker)
    print(data)

    # 기업 수익 내역 (Earnings History) 조회
    print(f"\nYahoo 기업 수익 내역 (Earnings History) 조회 ({ticker})")
    data = crawler.earnings_history(ticker)
    print(data)

    # 기업 EPS 추세 (EPS Trend) 조회
    print(f"\nYahoo 기업 EPS 추세 (EPS Trend) 조회 ({ticker})")
    data = crawler.eps_trend(ticker)
    print(data)

    # 기업 EPS 수정치 (EPS Revisions) 조회
    print(f"\nYahoo 기업 EPS 수정치 (EPS Revisions) 조회 ({ticker})")
    data = crawler.eps_revisions(ticker)
    print(data)

    # 기업 성장 추정치 (Growth Estimate) 조회
    print(f"\nYahoo 기업 성장 추정치 (Growth Estimate) 조회 ({ticker})")
    data = crawler.growth_estimate(ticker)
    print(data)

def demo_quote(crawler: YahooCrawler, ticker: str):
    """Yahoo 주요 시세 조회 데모"""

    print(f"\n{'='*60}")
    print(f"Yahoo 주요 시세 조회 - {ticker}")
    print('='*60)

    # 기업 이익공시일 (Earning Calendar) 조회
    print(f"\nYahoo 기업 이익공시일 (Earning Calendar) 조회 ({ticker})")
    data = crawler.earning_calendar(ticker)
    print(data)

    # 기업 SEC 공시 정보 (SEC Filings) 조회
    print(f"\nYahoo 기업 SEC 공시 정보 (SEC Filings) 조회 ({ticker})")
    data = crawler.sec_filings(ticker)
    print(data)

    # 기업 추천도움 (Recommendation) 조회
    print(f"\nYahoo 기업 추천도움 (Recommendation) 조회 ({ticker})")
    data = crawler.recommendation(ticker)
    print(data)

def demo_market(crawler: YahooCrawler, ticker: str):
    """Yahoo 주요 시세 조회 데모"""

    print(f"\n{'='*60}")
    print(f"Yahoo 주요 시세 조회 - {ticker}")
    print('='*60)

    # 일별 시세 조회
    start_date='2025-12-01'
    end_date='2025-12-31'
    print(f"\nYahoo 일별 시세 조회 ({ticker}) {start_date} ~ {end_date}")

    data = crawler.chart(ticker, start_date=start_date, end_date=end_date)
    print(data)

    # 배당 조회
    ticker = "AAPL"
    print(f"\nYahoo 배당 조회 ({ticker})")
    data = crawler.dividends(ticker=ticker)
    print(data)

    # 양도소득 조회
    ticker = "AAPL"
    print(f"\nYahoo 양도소득 조회 ({ticker})")
    data = crawler.capital_gains(ticker=ticker)
    print(data)

    # 주식 분할 조회
    ticker = "AAPL"
    print(f"\nYahoo 주식 분할 조회 ({ticker})")
    data = crawler.splits(ticker=ticker)
    print(data)

def demo_news(crawler: YahooCrawler, ticker: str):
    """Yahoo 뉴스 조회 데모"""

    print(f"\n{'='*60}")
    print(f"Yahoo 뉴스 조회 - {ticker}")
    print('='*60)

    # Yahoo 뉴스 검색
    print(f"\nYahoo 뉴스 검색: {ticker}")
    data = crawler.news(query=ticker, max_articles=10)
    print(data)

def demo_financials(crawler: YahooCrawler, ticker: str):
    """Yahoo 재무제표 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 재무제표 조회 - {ticker}")
    print('='*60)

    # Yahoo 재무제표 손익계산서 (Income Statement)
    print(f"\nYahoo 재무제표 손익계산서 (Income Statement) 검색: {ticker}")
    data = crawler.income_statement(ticker)
    print(data)

    # Yahoo 재무제표 손익계산서 (Income Statement) (분기)
    print(f"\nYahoo 재무제표 손익계산서 (Income Statement) (분기) 검색: {ticker}")

    data = crawler.quarterly_income_statement(ticker)
    print(data)

    # 재무상태표 (Balance Sheet) (연간)
    print(f"\nYahoo 재무제표 재무상태표 (Balance Sheet) 검색: {ticker}")

    data = crawler.balance_sheet(ticker)
    print(data)

    # 재무상태표 (Balance Sheet) (분기)
    print(f"\nYahoo 재무제표 재무상태표 (Balance Sheet) (분기) 검색: {ticker}")

    data = crawler.quarterly_balance_sheet(ticker)
    print(data)

    # 현금흐름표 (Cash Flow) (연간)
    print(f"\nYahoo 재무제표 현금흐름표 (Cash Flow) 검색: {ticker}")

    data = crawler.cash_flow(ticker)
    print(data)

    # 현금흐름표 (Cash Flow) (분기)
    print(f"\nYahoo 재무제표 현금흐름표 (Cash Flow) (분기) 검색: {ticker}")

    data = crawler.quarterly_cash_flow(ticker)
    print(data)

def demo_holders(crawler: YahooCrawler, ticker: str):
    """Yahoo 소유주 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 소유주 조회 - {ticker}")
    print('='*60)

    title_mapping = {
        "insiderTransactions": "Insider Transactions Reported - Last Two Years",
        "insiderHolders": "Insider Roster",
        "fundOwnership": "Top Institutional Holders",
        "institutionOwnership": "Top Mutual Fund Holders",
        "majorDirectHolders": "Major Direct Holders",
        "majorHoldersBreakdown": "Major Holders",
        "netSharePurchaseActivity": "Insider Purchases Last 6 Months",
    }

    # 기업 소유주 조회
    print(f"\nYahoo 기업 소유주 조회 ({ticker})")
    data = crawler.holders(ticker)
    for key, value in data.items():
        print(f"\n{title_mapping.get(key, key)}")
        print(value)

def demo_summary(crawler: YahooCrawler, ticker: str):
    """Yahoo 요약 정보 조회 데모"""

    print(f"\n{'='*60}")
    print(f"Yahoo 요약 정보 조회 - {ticker}")
    print('='*60)

    # 기업 정보 조회
    print(f"\nYahoo 기업 요약 정보 조회 ({ticker})")
    data = crawler.summary(ticker)
    print(data)

def main(ticker: str):
    """메인 데모 실행"""

    # Yahoo에서 요구하는 User-Agent 설정
    crawler = YahooCrawler()

    # 각 파일링 타입 데모
    demo_analysis(crawler, ticker)
    demo_quote(crawler, ticker)
    demo_market(crawler, ticker)
    demo_news(crawler, ticker)
    demo_financials(crawler, ticker)
    demo_holders(crawler, ticker)
    demo_summary(crawler, ticker)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    #ticker = "TSLA" # Tesla
    ticker = "AAPL" # Apple
    #ticker = "GOOG"
    main(ticker)
