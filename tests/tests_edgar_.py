#!/usr/bin/env python3
"""
SEC EDGAR Crawler 사용 예시
"""

from sayou.stock.edgar import EDGARCrawler

def demo_10k(crawler: EDGARCrawler, cik: str, ticker: str):
    """10-K 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"EDGAR 10-K Annual Report - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_10k_filings(cik, count=1)
    filings = crawler.fetch_filings(cik, doc_type="10-K", count=1)
    if not filings:
        print("No 10-K filings found")
        return
    
    print(f"Latest filing: {filings[0].filing_date}")
    
    #print(filings)
    data = crawler.extract_10k(cik, filings[0].document_url, filings[0].accession_number)
    #for filing in filings:
    #    print(filing)
    #    data = crawler.extract_10k(cik, filing.document_url, filing.accession_number)
    
    #print(data)
    print(f"\nSections extracted: {list(data['sections'].keys())}")
    
    if data["financial_data"]:
        fd = data["financial_data"]
        print("\nFinancial Data:")
        if fd.revenue:
            print(f"  Revenue: ${fd.revenue:,.0f}")
        if fd.net_income:
            print(f"  Net Income: ${fd.net_income:,.0f}")
        if fd.total_assets:
            print(f"  Total Assets: ${fd.total_assets:,.0f}")
        if fd.eps_diluted:
            print(f"  Diluted EPS: ${fd.eps_diluted:.2f}")
    
    if data["risk_factors"]:
        print(f"\nRisk Factors: {len(data['risk_factors'])} identified")
        print(f"  First: {data['risk_factors'][0][:100]}...")


def demo_10q(crawler: EDGARCrawler, cik: str, ticker: str):
    """10-Q 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"EDGAR 10-Q Quarterly Report - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_10q_filings(cik, count=1)
    filings = crawler.fetch_filings(cik, doc_type="10-Q", count=1)
    if not filings:
        print("No 10-Q filings found")
        return
    
    print(f"Latest filing: {filings[0].filing_date}")
    
    #print(filings)
    data = crawler.extract_10q(cik, filings[0].document_url, filings[0].accession_number)
    #for filing in filings:
    #    print(filing)
    #    data = crawler.extract_10k(cik, filing.document_url, filing.accession_number)
    
    #print(data)
    print(f"\nSections extracted: {list(data['sections'].keys())}")
    
    if data["financial_data"]:
        fd = data["financial_data"]
        print("\nFinancial Data:")
        if fd.revenue:
            print(f"  Revenue: ${fd.revenue:,.0f}")
        if fd.net_income:
            print(f"  Net Income: ${fd.net_income:,.0f}")
        if fd.total_assets:
            print(f"  Total Assets: ${fd.total_assets:,.0f}")
        if fd.eps_diluted:
            print(f"  Diluted EPS: ${fd.eps_diluted:.2f}")
    
    #if data["risk_factors"]:
    #    print(f"\nRisk Factors: {len(data['risk_factors'])} identified")
    #    print(f"  First: {data['risk_factors'][0][:100]}...")


def demo_8k(crawler: EDGARCrawler, cik: str, ticker: str):
    """8-K 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"EDGAR 8-K Current Reports - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_8k_filings(cik, count=3)
    filings = crawler.fetch_filings(cik, doc_type="8-K", count=3)
    
    for filing in filings:
        print(f"\n{filing.filing_date}:")
        data = crawler.extract_8k(cik, filing.document_url, filing.accession_number)
        
        print(f"  Items: {data.items}")
        for event in data.event_descriptions[:2]:
            print(f"  - {event['item']}: {event['description']}")


def demo_13f(crawler: EDGARCrawler, cik: str, ticker: str):
    """13F 파일링 데모"""
    print(f"\n{'='*60}")
    #print("13F Institutional Holdings - Berkshire Hathaway")
    print(f"EDGAR 13F Institutional Holdings - {ticker}")
    print('='*60)
    
    #cik = "0001067983"
    #filings = crawler.fetch_13f_filings(cik, count=2)
    filings = crawler.fetch_filings(cik, doc_type="13F", count=2)
    
    if not filings:
        print("No 13F filings found")
        return
    
    # 최신 포트폴리오
    data = crawler.extract_13f(cik, filings[0].document_url, filings[0].accession_number)
    
    print(f"\nReport Period: {data.report_period}")
    print(f"Total AUM: ${data.total_value / 1000:,.2f}M")
    print(f"Holdings Count: {data.holdings_count}")
    
    print("\nTop 5 Holdings:")
    for i, h in enumerate(data.top_holdings[:5], 1):
        print(f"  {i}. {h['issuer']}: ${h['value_millions']:,.2f}M ({h['percentage']:.1f}%)")
    
    # 분기 비교
    if len(filings) >= 2:
        print("\nQuarter-over-Quarter Changes:")
        comparison = crawler.compare_13f(
            cik,
            filings[0].document_url,
            filings[1].document_url,
            filings[0].accession_number,
            filings[1].accession_number
        )
        
        if comparison["new_positions"]:
            print(f"\n  New Positions: {len(comparison['new_positions'])}")
            for p in comparison["new_positions"][:3]:
                print(f"    + {p['issuer']}: ${p['value_thousands']/1000:,.2f}M")
        
        if comparison["closed_positions"]:
            print(f"\n  Closed Positions: {len(comparison['closed_positions'])}")
            for p in comparison["closed_positions"][:3]:
                print(f"    - {p['issuer']}")


def demo_def14a(crawler: EDGARCrawler, cik: str, ticker: str):
    """DEF 14A 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"EDGAR DEF 14A Proxy Statement - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_def14a_filings(cik, count=1)
    filings = crawler.fetch_filings(cik, doc_type="DEF 14A", count=1)

    if not filings:
        print("No DEF 14A filings found")
        return
    
    data = crawler.extract_def14a(cik, filings[0].document_url, filings[0].accession_number)
    
    print(f"\nMeeting: {data.meeting_type} ({data.meeting_date})")
    
    # 임원 보상
    if data.executive_compensation:
        print("\nExecutive Compensation:")
        for exec_comp in data.executive_compensation[:5]:
            print(f"  {exec_comp.name}")
            print(f"    Salary: ${exec_comp.salary:,.0f}")
            print(f"    Stock Awards: ${exec_comp.stock_awards:,.0f}")
            print(f"    Total: ${exec_comp.total:,.0f}")
    
    # CEO Pay Ratio
    if data.ceo_pay_ratio:
        print(f"\nCEO Pay Ratio: {data.ceo_pay_ratio:.0f}:1")
    
    # 이사회
    print(f"\nBoard: {data.board_size} directors ({data.independent_directors} independent)")
    
    # 투표 안건
    if data.proposals:
        print("\nProposals:")
        for prop in data.proposals[:5]:
            rec = f" [{prop.board_recommendation}]" if prop.board_recommendation else ""
            print(f"  {prop.proposal_number}. {prop.title[:50]}{rec}")
    
    # 거버넌스
    if data.governance_highlights:
        print("\nGovernance Highlights:")
        for item in data.governance_highlights[:5]:
            print(f"  ✓ {item}")


def demo_peer_comparison(crawler: EDGARCrawler):
    """동종 업계 비교 데모"""
    print(f"\n{'='*60}")
    print("Peer Comparison - Big Tech CEO Compensation")
    print('='*60)
    
    tech_ciks = [
        "0000320193",  # Apple
        "0000789019",  # Microsoft
        "0001652044",  # Alphabet
        "0001326801",  # Meta
    ]
    
    comparison = crawler.compare_peer_compensation(tech_ciks)
    
    print("\nCEO Total Compensation Ranking:")
    for i, comp in enumerate(comparison, 1):
        if comp.get("ceo_total_compensation"):
            print(f"  {i}. {comp['ceo_name']}: ${comp['ceo_total_compensation']:,.0f}")
            if comp.get('ceo_pay_ratio'):
                print(f"     Pay Ratio: {comp['ceo_pay_ratio']:.0f}:1")


def main(ticker: str):
    """메인 데모 실행"""
    # SEC에서 요구하는 User-Agent 설정
    crawler = EDGARCrawler(user_agent="Sayouzone sjkim@sayouzone.com")
    
    # 회사이름으로 CIK 검색
    #company_name = "Microsoft"
    #cik = crawler.fetch_cik_by_company(company_name)[0]["cik"]
    
    #if not cik:
    #    print(f"Could not find CIK for {company_name}")
    #    return
    #print(f"\n{company_name} CIK: {cik}")
    
    # Ticker로 CIK 검색
    cik = crawler.fetch_cik_by_ticker(ticker)
    
    if not cik:
        print(f"Could not find CIK for {ticker}")
        return
    
    print(f"\n{ticker} CIK: {cik}")
    
    # 각 파일링 타입 데모
    demo_10k(crawler, cik, ticker)
    demo_10q(crawler, cik, ticker)
    demo_8k(crawler, cik, ticker)
    demo_13f(crawler, cik, ticker)
    demo_def14a(crawler, cik, ticker)
    demo_peer_comparison(crawler)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # Apple, Google, Microsoft, Meta, NVIDIA, Tesla 예시
    ticker = "AAPL" # Apple
    #ticker = "GOOG" # Google
    ticker = "MSFT" # Microsoft
    #ticker = "META" # Meta
    #ticker = "NVDA" # NVIDIA
    #ticker = "TSLA" # Tesla
    main(ticker)
