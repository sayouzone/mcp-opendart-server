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
 
"""
OpenDart 유틸리티 함수 및 상수
"""

import io
import re
import xmltodict
import zipfile

# ============================================================
# 상수 정의
# ============================================================

API_URL = "https://opendart.fss.or.kr/api"
MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
PDF_URL = "https://dart.fss.or.kr/pdf/download/pdf.do?rcp_no={rcp_no}&dcm_no={dcm_no}"
PDF_MAIN_URL = "http://dart.fss.or.kr/pdf/download/main.do"
VIEWER_URL = "https://dart.fss.or.kr/report/viewer.do"

"""
공시정보: Public Disclosure, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
정기보고서 주요정보: Key Information in Periodic Reports, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002
정기보고서 재무정보: Financial Information in Periodic Reports, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS003
지분공시 종합정보: Comprehensive Share Ownership Information, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS004
주요사항보고서 주요정보: Key Information in Reports on Material Facts, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005
증권신고서 주요정보: Key Information in Registration Statements, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS006
"""

# 공시정보
DISCLOSURE_URLS = {
    "공시검색": f"{API_URL}/list.json", # 공시 유형별, 회사별, 날짜별 등 여러가지 조건으로 공시보고서 검색기능을 제공합니다.
    "기업개황": f"{API_URL}/company.json", # DART에 등록되어있는 기업의 개황정보를 제공합니다.
    "공시서류원본파일": f"{API_URL}/document.xml", # 공시보고서 원본파일을 제공합니다.
    "고유번호": f"{API_URL}/corpCode.xml" # DART에 등록되어있는 공시대상회사의 고유번호,회사명,종목코드, 최근변경일자를 파일로 제공합니다.
}

# 정기보고서 주요정보
REPORTS_URLS = {
    "증자(감자) 현황": f"{API_URL}/irdsSttus.json",                    # 정기보고서(사업, 분기, 반기보고서) 내에 증자(감자) 현황을 제공합니다.
    "배당에 관한 사항": f"{API_URL}/alotMatter.json",                   # 정기보고서(사업, 분기, 반기보고서) 내에 배당에 관한 사항을 제공합니다.
    "자기주식 취득 및 처분 현황": f"{API_URL}/tesstkAcqsDspsSttus.json",   # 정기보고서(사업, 분기, 반기보고서) 내에 자기주식 취득 및 처분 현황을 제공합니다.
    "최대주주 현황": f"{API_URL}/hyslrSttus.json",                      # 정기보고서(사업, 분기, 반기보고서) 내에 최대주주 현황을 제공합니다.
    "최대주주 변동현황": f"{API_URL}/hyslrChgSttus.json",                # 정기보고서(사업, 분기, 반기보고서) 내에 최대주주 변동현황을 제공합니다.
    "소액주주 현황": f"{API_URL}/mrhlSttus.json",                       # 정기보고서(사업, 분기, 반기보고서) 내에 소액주주 현황을 제공합니다.
    "임원 현황": f"{API_URL}/exctvSttus.json",                         # 정기보고서(사업, 분기, 반기보고서) 내에 임원 현황을 제공합니다.
    "직원 현황": f"{API_URL}/empSttus.json",                           # 정기보고서(사업, 분기, 반기보고서) 내에 직원 현황을 제공합니다.
    "이사·감사의 개인별 보수현황(5억원 이상)": f"{API_URL}/hmvAuditIndvdlBySttus.json",         # 정기보고서(사업, 분기, 반기보고서) 내에 이사·감사의 개인별 보수현황(5억원 이상)을 제공합니다.
    "이사·감사 전체의 보수현황(보수지급금액 - 이사·감사 전체)": f"{API_URL}/hmvAuditAllSttus.json", # 정기보고서(사업, 분기, 반기보고서) 내에 이사·감사 전체의 보수현황(보수지급금액 - 이사·감사 전체)을 제공합니다.
    "개인별 보수지급 금액(5억이상 상위5인)": f"{API_URL}/indvdlByPay.json",                     # 정기보고서(사업, 분기, 반기보고서) 내에 개인별 보수지급 금액(5억이상 상위5인)을 제공합니다.
    "타법인 출자현황": f"{API_URL}/otrCprInvstmntSttus.json",                               # 정기보고서(사업, 분기, 반기보고서) 내에 타법인 출자현황을 제공합니다.
    "주식의 총수 현황": f"{API_URL}/stockTotqySttus.json",                                  # 정기보고서(사업, 분기, 반기보고서) 내에 주식의총수현황을 제공합니다.
    "채무증권 발행실적": f"{API_URL}/detScritsIsuAcmslt.json",                                 # 정기보고서(사업, 분기, 반기보고서) 내에 채무증권 발행실적을 제공합니다.
    "기업어음증권 미상환 잔액": f"{API_URL}/entrprsBilScritsNrdmpBlce.json",                     # 정기보고서(사업, 분기, 반기보고서) 내에 기업어음증권 미상환 잔액을 제공합니다.
    "단기사채 미상환 잔액": f"{API_URL}/srtpdPsndbtNrdmpBlce.json",                             # 정기보고서(사업, 분기, 반기보고서) 내에 단기사채 미상환 잔액을 제공합니다.
    "회사채 미상환 잔액": f"{API_URL}/cprndNrdmpBlce.json",                                     # 정기보고서(사업, 분기, 반기보고서) 내에 회사채 미상환 잔액을 제공합니다.
    "신종자본증권 미상환 잔액": f"{API_URL}/newCaplScritsNrdmpBlce.json",                        # 정기보고서(사업, 분기, 반기보고서) 내에 신종자본증권 미상환 잔액을 제공합니다.
    "조건부 자본증권 미상환 잔액": f"{API_URL}/cndlCaplScritsNrdmpBlce.json",                    # 정기보고서(사업, 분기, 반기보고서) 내에 조건부 자본증권 미상환 잔액을 제공합니다.
    "회계감사인의 명칭 및 감사의견": f"{API_URL}/accnutAdtorNmNdAdtOpinion.json",                 # 정기보고서(사업, 분기, 반기보고서) 내에 회계감사인의 명칭 및 감사의견을 제공합니다.
    "감사용역체결현황": f"{API_URL}/adtServcCnclsSttus.json",                                  # 정기보고서(사업, 분기, 반기보고서) 내에 감사용역체결현황을 제공합니다.
    "회계감사인과의 비감사용역 계약체결 현황": f"{API_URL}/accnutAdtorNonAdtServcCnclsSttus.json",  # 정기보고서(사업, 분기, 반기보고서) 내에 회계감사인과의 비감사용역 계약체결 현황을 제공합니다.
    "사외이사 및 그 변동현황": f"{API_URL}/outcmpnyDrctrNdChangeSttus.json",                    # 정기보고서(사업, 분기, 반기보고서) 내에 사외이사 및 그 변동현황을 제공합니다.
    "미등기임원 보수현황": f"{API_URL}/unrstExctvMendngSttus.json",                             # 정기보고서(사업, 분기, 반기보고서) 내에 미등기임원 보수현황을 제공합니다.
    "이사·감사 전체의 보수현황(주주총회 승인금액)": f"{API_URL}/drctrAdtAllMendngSttusGmtsckConfmAmount.json",      # 정기보고서(사업, 분기, 반기보고서) 내에 이사·감사 전체의 보수현황(주주총회 승인금액)을 제공합니다.
    "이사·감사 전체의 보수현황(보수지급금액 - 유형별)": f"{API_URL}/drctrAdtAllMendngSttusMendngPymntamtTyCl.json",  # 정기보고서(사업, 분기, 반기보고서) 내에 이사·감사 전체의 보수현황(보수지급금액 - 유형별)을 제공합니다.
    "공모자금의 사용내역": f"{API_URL}/pssrpCptalUseDtls.json",     # 정기보고서(사업, 분기, 반기보고서) 내에 공모자금의 사용내역을 제공합니다.
    "사모자금의 사용내역": f"{API_URL}/prvsrpCptalUseDtls.json",    # 정기보고서(사업, 분기, 반기보고서) 내에 사모자금의 사용내역을 제공합니다.
}

# 정기보고서 재무정보
FINANCE_URLS = {
    "단일회사 주요계정": f"{API_URL}/fnlttSinglAcnt.json",          # 상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요계정과목(재무상태표, 손익계산서)을 제공합니다.
    "다중회사 주요계정": f"{API_URL}/fnlttMultiAcnt.json",          # 상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요계정과목(재무상태표, 손익계산서)을 제공합니다. (대상법인 복수조회 복수조회 가능)
    "재무제표 원본파일(XBRL)": f"{API_URL}/fnlttXbrl.xml",          # 상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 원본파일(XBRL)을 제공합니다.
    "단일회사 전체 재무제표": f"{API_URL}/fnlttSinglAcntAll.json",   # 상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 모든계정과목을 제공합니다.
    "XBRL택사노미재무제표양식": f"{API_URL}/xbrlTaxonomy.json",      # 금융감독원 회계포탈에서 제공하는 IFRS 기반 XBRL 재무제표 공시용 표준계정과목체계(계정과목) 을 제공합니다.
    "단일회사 주요 재무지표": f"{API_URL}/fnlttSinglIndx.json",      # 상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요 재무지표를 제공합니다.
    "다중회사 주요 재무지표": f"{API_URL}/fnlttCmpnyIndx.json"       # 상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요 재무지표를 제공합니다.(대상법인 복수조회 가능)
}

# 지분공시 종합정보
OWNERSHIP_URLS = {
    "대량보유 상황보고": f"{API_URL}/majorstock.json",  # 주식등의 대량보유상황보고서 내에 대량보유 상황보고 정보를 제공합니다.
    "임원ㆍ주요주주 소유보고": f"{API_URL}/elestock.json" # 임원ㆍ주요주주특정증권등 소유상황보고서 내에 임원ㆍ주요주주 소유보고 정보를 제공합니다.
}

# 주요사항보고서 주요정보
MATERIAL_FACTS_URLS = {
    "자산양수도(기타), 풋백옵션": f"{API_URL}/astInhtrfEtcPtbkOpt.json", # 주요사항보고서(자산양수도(기타), 풋백옵션) 내에 주요 정보를 제공합니다.
    "부도발생": f"{API_URL}/dfOcr.json",                            # 주요사항보고서(부도발생) 내에 주요 정보를 제공합니다.
    "영업정지": f"{API_URL}/bsnSp.json",                            # 주요사항보고서(영업정지) 내에 주요 정보를 제공합니다.
    "회생절차 개시신청": f"{API_URL}/ctrcvsBgrq.json",                # 주요사항보고서(회생절차 개시신청) 내에 주요 정보를 제공합니다.
    "해산사유 발생": f"{API_URL}/dsRsOcr.json",                      # 주요사항보고서(해산사유 발생) 내에 주요 정보를 제공합니다.
    "유상증자 결정": f"{API_URL}/piicDecsn.json",                    # 주요사항보고서(유상증자 결정) 내에 주요 정보를 제공합니다.
    "무상증자 결정": f"{API_URL}/fricDecsn.json",                    # 주요사항보고서(무상증자 결정) 내에 주요 정보를 제공합니다.
    "유무상증자 결정": f"{API_URL}/pifricDecsn.json",                 # 주요사항보고서(유무상증자 결정) 내에 주요 정보를 제공합니다.
    "감자 결정": f"{API_URL}/crDecsn.json",                         # 주요사항보고서(감자 결정) 내에 주요 정보를 제공합니다.
    "채권은행 등의 관리절차 개시": f"{API_URL}/bnkMngtPcbg.json",        # 주요사항보고서(채권은행 등의 관리절차 개시) 내에 주요 정보를 제공합니다.
    "소송 등의 제기": f"{API_URL}/lwstLg.json",                      # 주요사항보고서(소송 등의 제기) 내에 주요 정보를 제공합니다.
    "해외 증권시장 주권등 상장 결정": f"{API_URL}/ovLstDecsn.json",      # 주요사항보고서(해외 증권시장 주권등 상장 결정) 내에 주요 정보를 제공합니다.
    "해외 증권시장 주권등 상장폐지 결정": f"{API_URL}/ovDlstDecsn.json",  # 주요사항보고서(해외 증권시장 주권등 상장폐지 결정) 내에 주요 정보를 제공합니다.
    "해외 증권시장 주권등 상장": f"{API_URL}/ovLst.json",               # 주요사항보고서(해외 증권시장 주권등 상장) 내에 주요 정보를 제공합니다.
    "해외 증권시장 주권등 상장폐지": f"{API_URL}/ovDlst.json",           # 주요사항보고서(해외 증권시장 주권등 상장폐지) 내에 주요 정보를 제공합니다.
    "전환사채권 발행결정": f"{API_URL}/cvbdIsDecsn.json",              # 주요사항보고서(전환사채권 발행결정) 내에 주요 정보를 제공합니다.
    "신주인수권부사채권 발행결정": f"{API_URL}/bdwtIsDecsn.json",        # 주요사항보고서(신주인수권부사채권 발행결정) 내에 주요 정보를 제공합니다.
    "교환사채권 발행결정": f"{API_URL}/exbdIsDecsn.json",              # 주요사항보고서(교환사채권 발행결정) 내에 주요 정보를 제공합니다.
    "채권은행 등의 관리절차 중단": f"{API_URL}/bnkMngtPcsp.json",        # 주요사항보고서(채권은행 등의 관리절차 중단) 내에 주요 정보를 제공합니다.
    "상각형 조건부자본증권 발행결정": f"{API_URL}/wdCocobdIsDecsn.json",  # 주요사항보고서(상각형 조건부자본증권 발행결정) 내에 주요 정보를 제공합니다.
    "자기주식 취득 결정": f"{API_URL}/tsstkAqDecsn.json",                  # 주요사항보고서(자기주식 취득 결정) 내에 주요 정보를 제공합니다.
    "자기주식 처분 결정": f"{API_URL}/tsstkDpDecsn.json",                  # 주요사항보고서(자기주식 처분 결정) 내에 주요 정보를 제공합니다.
    "자기주식취득 신탁계약 체결 결정": f"{API_URL}/tsstkAqTrctrCnsDecsn.json", # 주요사항보고서(자기주식취득 신탁계약 체결 결정) 내에 주요 정보를 제공합니다.
    "자기주식취득 신탁계약 해지 결정": f"{API_URL}/tsstkAqTrctrCcDecsn.json",  # 주요사항보고서(자기주식취득 신탁계약 해지 결정) 내에 주요 정보를 제공합니다.
    "영업양수 결정": f"{API_URL}/bsnInhDecsn.json",                           # 주요사항보고서(영업양수 결정) 내에 주요 정보를 제공합니다.
    "영업양도 결정": f"{API_URL}/bsnTrfDecsn.json",                           # 주요사항보고서(영업양도 결정) 내에 주요 정보를 제공합니다.
    "유형자산 양수 결정": f"{API_URL}/tgastInhDecsn.json",                     # 주요사항보고서(유형자산 양수 결정) 내에 주요 정보를 제공합니다.
    "유형자산 양도 결정": f"{API_URL}/tgastTrfDecsn.json",                     # 주요사항보고서(유형자산 양도 결정) 내에 주요 정보를 제공합니다.
    "타법인 주식 및 출자증권 양수결정": f"{API_URL}/otcprStkInvscrInhDecsn.json", # 주요사항보고서(타법인 주식 및 출자증권 양수결정) 내에 주요 정보를 제공합니다.
    "타법인 주식 및 출자증권 양도결정": f"{API_URL}/otcprStkInvscrTrfDecsn.json", # 주요사항보고서(타법인 주식 및 출자증권 양도결정) 내에 주요 정보를 제공합니다.
    "주권 관련 사채권 양수 결정": f"{API_URL}/stkrtbdInhDecsn.json",            # 주요사항보고서(주권 관련 사채권 양수 결정) 내에 주요 정보를 제공합니다.
    "주권 관련 사채권 양도 결정": f"{API_URL}/stkrtbdTrfDecsn.json",            # 주요사항보고서(주권 관련 사채권 양도 결정) 내에 주요 정보를 제공합니다.
    "회사합병 결정": f"{API_URL}/cmpMgDecsn.json",                           # 주요사항보고서(회사합병 결정) 내에 주요 정보를 제공합니다.
    "회사분할 결정": f"{API_URL}/cmpDvDecsn.json",                           # 주요사항보고서(회사분할 결정) 내에 주요 정보를 제공합니다.
    "회사분할합병 결정": f"{API_URL}/cmpDvmgDecsn.json",                      # 주요사항보고서(회사분할합병 결정) 내에 주요 정보를 제공합니다.
    "주식교환·이전 결정": f"{API_URL}/stkExtrDecsn.json",                     # 주요사항보고서(주식교환·이전 결정) 내에 주요 정보를 제공합니다.
}

# 증권신고서 주요정보
REGISTRATION_URLS = {
    "지분증권": f"{API_URL}/estkRs.json",           # 증권신고서(지분증권) 내에 요약 정보를 제공합니다.
    "채무증권": f"{API_URL}/bdRs.json",             # 증권신고서(채무증권) 내에 요약 정보를 제공합니다.
    "증권예탁증권": f"{API_URL}/stkdpRs.json",       # 증권신고서(증권예탁증권) 내에 요약 정보를 제공합니다.
    "합병": f"{API_URL}/mgRs.json",                # 증권신고서(합병) 내에 요약 정보를 제공합니다.
    "주식의포괄적교환·이전": f"{API_URL}/extrRs.json",  # 증권신고서(주식의포괄적교환·이전) 내에 요약 정보를 제공합니다.
    "분할": f"{API_URL}/dvRs.json"                 # 증권신고서(분할) 내에 요약 정보를 제공합니다.
}

quarters = {
    "1": "11013", # 1분기보고서
    "2": "11012", # 반기보고서
    "3": "11014", # 3분기보고서
    "4": "11011"  # 사업보고서
}

# 공시정보

DISCLOSURE_ITEMS = {
    "1": ("공시검색", "공시 유형별, 회사별, 날짜별 등 여러가지 조건으로 공시보고서 검색기능을 제공합니다."),
    "2": ("기업개황", "DART에 등록되어있는 기업의 개황정보를 제공합니다."),
    "3": ("공시서류원본파일", "공시보고서 원본파일을 제공합니다."),
    "4": ("고유번호", "DART에 등록되어있는 공시대상회사의 고유번호,회사명,종목코드, 최근변경일자를 파일로 제공합니다."),
}

# 정기보고서 주요정보

REPORT_ITEMS = {
    "1": ("증자(감자) 현황", "정기보고서(사업, 분기, 반기보고서) 내에 증자(감자) 현황을 제공합니다."),
    "2": ("배당에 관한 사항", "정기보고서(사업, 분기, 반기보고서) 내에 배당에 관한 사항을 제공합니다."),
    "3": ("자기주식 취득 및 처분 현황", "정기보고서(사업, 분기, 반기보고서) 내에 자기주식 취득 및 처분 현황을 제공합니다."),
    "4": ("최대주주 현황", "정기보고서(사업, 분기, 반기보고서) 내에 최대주주 현황을 제공합니다."),
    "5": ("최대주주 변동현황", "정기보고서(사업, 분기, 반기보고서) 내에 최대주주 변동현황을 제공합니다."),
    "6": ("소액주주 현황", "정기보고서(사업, 분기, 반기보고서) 내에 소액주주 현황을 제공합니다."),
    "7": ("임원 현황", "정기보고서(사업, 분기, 반기보고서) 내에 임원 현황을 제공합니다."),
    "8": ("직원 현황", "정기보고서(사업, 분기, 반기보고서) 내에 직원 현황을 제공합니다."),
    "9": ("이사·감사의 개인별 보수현황(5억원 이상)", "정기보고서(사업, 분기, 반기보고서) 내에 이사·감사의 개인별 보수현황(5억원 이상)을 제공합니다."),
    "10": ("이사·감사 전체의 보수현황(보수지급금액 - 이사·감사 전체)", "정기보고서(사업, 분기, 반기보고서) 내에 이사·감사 전체의 보수현황(보수지급금액 - 이사·감사 전체)을 제공합니다."),
    "11": ("개인별 보수지급 금액(5억이상 상위5인)", "정기보고서(사업, 분기, 반기보고서) 내에 개인별 보수지급 금액(5억이상 상위5인)을 제공합니다."),
    "12": ("타법인 출자현황", "정기보고서(사업, 분기, 반기보고서) 내에 타법인 출자현황을 제공합니다."),
    "13": ("주식의 총수 현황", "정기보고서(사업, 분기, 반기보고서) 내에 주식의총수현황을 제공합니다."),
    "14": ("채무증권 발행실적", "정기보고서(사업, 분기, 반기보고서) 내에 채무증권 발행실적을 제공합니다."),
    "15": ("기업어음증권 미상환 잔액", "정기보고서(사업, 분기, 반기보고서) 내에 기업어음증권 미상환 잔액을 제공합니다."),
    "16": ("단기사채 미상환 잔액", "정기보고서(사업, 분기, 반기보고서) 내에 단기사채 미상환 잔액을 제공합니다."),
    "17": ("회사채 미상환 잔액", "정기보고서(사업, 분기, 반기보고서) 내에 회사채 미상환 잔액을 제공합니다."),
    "18": ("신종자본증권 미상환 잔액", "정기보고서(사업, 분기, 반기보고서) 내에 신종자본증권 미상환 잔액을 제공합니다."),
    "19": ("조건부 자본증권 미상환 잔액", "정기보고서(사업, 분기, 반기보고서) 내에 조건부 자본증권 미상환 잔액을 제공합니다."),
    "20": ("회계감사인의 명칭 및 감사의견", "정기보고서(사업, 분기, 반기보고서) 내에 회계감사인의 명칭 및 감사의견을 제공합니다."),
    "21": ("감사용역체결현황", "정기보고서(사업, 분기, 반기보고서) 내에 감사용역체결현황을 제공합니다."),
    "22": ("회계감사인과의 비감사용역 계약체결 현황", "정기보고서(사업, 분기, 반기보고서) 내에 회계감사인과의 비감사용역 계약체결 현황을 제공합니다."),
    "23": ("사외이사 및 그 변동현황", "정기보고서(사업, 분기, 반기보고서) 내에 사외이사 및 그 변동현황을 제공합니다."),
    "24": ("미등기임원 보수현황", "정기보고서(사업, 분기, 반기보고서) 내에 미등기임원 보수현황을 제공합니다."),
    "25": ("이사·감사 전체의 보수현황(주주총회 승인금액)", "정기보고서(사업, 분기, 반기보고서) 내에 이사·감사 전체의 보수현황(주주총회 승인금액)을 제공합니다."),
    "26": ("이사·감사 전체의 보수현황(보수지급금액 - 유형별)", "정기보고서(사업, 분기, 반기보고서) 내에 이사·감사 전체의 보수현황(보수지급금액 - 유형별)을 제공합니다."),
    "27": ("공모자금의 사용내역", "정기보고서(사업, 분기, 반기보고서) 내에 공모자금의 사용내역을 제공합니다."),
    "28": ("사모자금의 사용내역", "정기보고서(사업, 분기, 반기보고서) 내에 사모자금의 사용내역을 제공합니다."),
}

# 정기보고서 재무정보

FINANCE_ITEMS = {
    "1": ("단일회사 주요계정", "상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요계정과목(재무상태표, 손익계산서)을 제공합니다."),
    "2": ("다중회사 주요계정", "상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요계정과목(재무상태표, 손익계산서)을 제공합니다. (대상법인 복수조회 복수조회 가능)"),
    "3": ("재무제표 원본파일(XBRL)", "상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 원본파일(XBRL)을 제공합니다."),
    "4": ("단일회사 전체 재무제표", "상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 모든계정과목을 제공합니다."),
    "5": ("XBRL택사노미재무제표양식", "금융감독원 회계포탈에서 제공하는 IFRS 기반 XBRL 재무제표 공시용 표준계정과목체계(계정과목) 을 제공합니다."),
    "6": ("단일회사 주요 재무지표", "상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요 재무지표를 제공합니다."),
    "7": ("다중회사 주요 재무지표", "상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요 재무지표를 제공합니다.(대상법인 복수조회 가능)"),
}

# 지분공시 종합정보

OWNERSHIP_ITEMS = {
    "1": ("대량보유 상황보고", "주식등의 대량보유상황보고서 내에 대량보유 상황보고 정보를 제공합니다."),
    "2": ("임원ㆍ주요주주 소유보고", "임원ㆍ주요주주특정증권등 소유상황보고서 내에 임원ㆍ주요주주 소유보고 정보를 제공합니다."),
}

# 주요사항보고서 주요정보

MATERIAL_FACTS_ITEMS = {
    "1": ("자산양수도(기타), 풋백옵션", "주요사항보고서(자산양수도(기타), 풋백옵션) 내에 주요 정보를 제공합니다."),
    "2": ("부도발생", "주요사항보고서(부도발생) 내에 주요 정보를 제공합니다."),
    "3": ("영업정지", "주요사항보고서(영업정지) 내에 주요 정보를 제공합니다."),
    "4": ("회생절차 개시신청", "주요사항보고서(회생절차 개시신청) 내에 주요 정보를 제공합니다."),
    "5": ("해산사유 발생", "주요사항보고서(해산사유 발생) 내에 주요 정보를 제공합니다."),
    "6": ("유상증자 결정", "주요사항보고서(유상증자 결정) 내에 주요 정보를 제공합니다."),
    "7": ("무상증자 결정", "주요사항보고서(무상증자 결정) 내에 주요 정보를 제공합니다."),
    "8": ("유무상증자 결정", "주요사항보고서(유무상증자 결정) 내에 주요 정보를 제공합니다."),
    "9": ("감자 결정", "주요사항보고서(감자 결정) 내에 주요 정보를 제공합니다."),
    "10": ("채권은행 등의 관리절차 개시", "주요사항보고서(채권은행 등의 관리절차 개시) 내에 주요 정보를 제공합니다."),
    "11": ("소송 등의 제기", "주요사항보고서(소송 등의 제기) 내에 주요 정보를 제공합니다."),
    "12": ("해외 증권시장 주권등 상장 결정", "주요사항보고서(해외 증권시장 주권등 상장 결정) 내에 주요 정보를 제공합니다."),
    "13": ("해외 증권시장 주권등 상장폐지 결정", "주요사항보고서(해외 증권시장 주권등 상장폐지 결정) 내에 주요 정보를 제공합니다."),
    "14": ("해외 증권시장 주권등 상장", "주요사항보고서(해외 증권시장 주권등 상장) 내에 주요 정보를 제공합니다."),
    "15": ("해외 증권시장 주권등 상장폐지", "주요사항보고서(해외 증권시장 주권등 상장폐지) 내에 주요 정보를 제공합니다."),
    "16": ("전환사채권 발행결정", "주요사항보고서(전환사채권 발행결정) 내에 주요 정보를 제공합니다."),
    "17": ("신주인수권부사채권 발행결정", "주요사항보고서(신주인수권부사채권 발행결정) 내에 주요 정보를 제공합니다."),
    "18": ("교환사채권 발행결정", "주요사항보고서(교환사채권 발행결정) 내에 주요 정보를 제공합니다."),
    "19": ("채권은행 등의 관리절차 중단", "주요사항보고서(채권은행 등의 관리절차 중단) 내에 주요 정보를 제공합니다."),
    "20": ("상각형 조건부자본증권 발행결정", "주요사항보고서(상각형 조건부자본증권 발행결정) 내에 주요 정보를 제공합니다."),
    "21": ("자기주식 취득 결정", "주요사항보고서(자기주식 취득 결정) 내에 주요 정보를 제공합니다."),
    "22": ("자기주식 처분 결정", "주요사항보고서(자기주식 처분 결정) 내에 주요 정보를 제공합니다."),
    "23": ("자기주식취득 신탁계약 체결 결정", "주요사항보고서(자기주식취득 신탁계약 체결 결정) 내에 주요 정보를 제공합니다."),
    "24": ("자기주식취득 신탁계약 해지 결정", "주요사항보고서(자기주식취득 신탁계약 해지 결정) 내에 주요 정보를 제공합니다."),
    "25": ("영업양수 결정", "주요사항보고서(영업양수 결정) 내에 주요 정보를 제공합니다."),
    "26": ("영업양도 결정", "주요사항보고서(영업양도 결정) 내에 주요 정보를 제공합니다."),
    "27": ("유형자산 양수 결정", "주요사항보고서(유형자산 양수 결정) 내에 주요 정보를 제공합니다."),
    "28": ("유형자산 양도 결정", "주요사항보고서(유형자산 양도 결정) 내에 주요 정보를 제공합니다."),
    "29": ("타법인 주식 및 출자증권 양수결정", "주요사항보고서(타법인 주식 및 출자증권 양수결정) 내에 주요 정보를 제공합니다."),
    "30": ("타법인 주식 및 출자증권 양도결정", "주요사항보고서(타법인 주식 및 출자증권 양도결정) 내에 주요 정보를 제공합니다."),
    "31": ("주권 관련 사채권 양수 결정", "주요사항보고서(주권 관련 사채권 양수 결정) 내에 주요 정보를 제공합니다."),
    "32": ("주권 관련 사채권 양도 결정", "주요사항보고서(주권 관련 사채권 양도 결정) 내에 주요 정보를 제공합니다."),
    "33": ("회사합병 결정", "주요사항보고서(회사합병 결정) 내에 주요 정보를 제공합니다."),
    "34": ("회사분할 결정", "주요사항보고서(회사분할 결정) 내에 주요 정보를 제공합니다."),
    "35": ("회사분할합병 결정", "주요사항보고서(회사분할합병 결정) 내에 주요 정보를 제공합니다."),
    "36": ("주식교환·이전 결정", "주요사항보고서(주식교환·이전 결정) 내에 주요 정보를 제공합니다."),
}

# 증권신고서 주요정보

REGISTRATION_ITEMS = {
    "1": ("지분증권", "증권신고서(지분증권) 내에 요약 정보를 제공합니다."),
    "2": ("채무증권", "증권신고서(채무증권) 내에 요약 정보를 제공합니다."),
    "3": ("증권예탁증권", "증권신고서(증권예탁증권) 내에 요약 정보를 제공합니다."),
    "4": ("합병", "증권신고서(합병) 내에 요약 정보를 제공합니다."),
    "5": ("주식의포괄적교환·이전", "증권신고서(주식의포괄적교환·이전) 내에 요약 정보를 제공합니다."),
    "6": ("분할", "증권신고서(분할) 내에 요약 정보를 제공합니다."),
}

# 공시정보

DISCLOSURE_COLUMNS = {
    "crtfc_key": "API 인증키",
    "bgn_de": "시작일",
    "end_de": "종료일",
    "last_reprt_at": "최종보고서 검색여부",
    "pblntf_ty": "공시유형",
    "pblntf_detail_ty": "공시상세유형",
    "sort": "정렬",
    "sort_mth": "정렬방법",
    "page_no": "페이지 번호",
    "page_count": "페이지 별 건수",
    "corp_cls": "법인구분",
    "corp_code": "고유번호",
    "corp_name": "종목명(법인명)",
    "corp_name_eng": "영문명칭",
    "corp_eng_name": "영문 정식명칭",
    "stock_name": "종목명(상장사) 또는 약식명칭(기타법인)",
    "stock_code": "상장회사인 경우 주식의 종목코드",
    "report_nm": "보고서명",
    "rcept_no": "접수번호",
    "flr_nm": "공시 제출인명",
    "rm": "비고",
    "ceo_nm": "대표자명",
    "jurir_no": "법인등록번호",
    "bizr_no": "사업자등록번호",
    "adres": "주소",
    "hm_url": "홈페이지",
    "ir_url": "IR홈페이지",
    "phn_no": "전화번호",
    "fax_no": "팩스번호",
    "induty_code": "업종코드",
    "est_dt": "설립일(YYYYMMDD)",
    "acc_mt": "결산월(MM)",
    "rcept_dt": "접수일자",
    "modify_date": "최종변경일자",
}

# 정기보고서 주요정보

REPORTS_COLUMNS = {
    "crtfc_key": "API 인증키",
    "corp_code": "고유번호",
    "bsns_year": "사업연도",
    "reprt_code": "보고서 코드",
    "rcept_no": "접수번호",
    "corp_cls": "법인구분",
    "corp_name": "법인명",
    "isu_dcrs_de": "주식발행 감소일자",
    "isu_dcrs_stle": "발행 감소 형태",
    "isu_dcrs_stock_knd": "발행 감소 주식 종류",
    "isu_dcrs_qy": "발행 감소 수량",
    "isu_dcrs_mstvdv_fval_amount": "발행 감소 주당 액면 가액",
    "isu_dcrs_mstvdv_amount": "발행 감소 주당 가액",
    "se": "구분",
    "se_nm": "구분",
    "stock_knd": "주식 종류",
    "thstrm": "당기",
    "frmtrm": "전기",
    "lwfr": "전전기",
    "acqs_mth1": "취득방법 대분류",
    "acqs_mth2": "취득방법 중분류",
    "acqs_mth3": "취득방법 소분류",
    "bsis_qy": "기초 수량",
    "change_qy_acqs": "변동 수량 취득",
    "change_qy_dsps": "변동 수량 처분",
    "change_qy_incnr": "변동 수량 소각",
    "trmend_qy": "기말 수량",
    "nm": "성명",
    "relate": "관계",
    "bsis_posesn_stock_co": "기초 소유 주식 수",
    "bsis_posesn_stock_qota_rt": "기초 소유 주식 지분율",
    "trmend_posesn_stock_co": "기말 소유 주식 수",
    "trmend_posesn_stock_qota_rt": "기말 소유 주식 지분율",
    "change_on": "변동일",
    "mxmm_shrholdr_nm": "최대 주주명",
    "posesn_stock_co": "소유 주식수",
    "qota_rt": "지분율",
    "change_cause": "변동원인",
    "shrholdr_co": "주주수",
    "shrholdr_tot_co": "전체 주주수",
    "shrholdr_rate": "주주 비율",
    "hold_stock_co": "보유 주식수",
    "stock_tot_co": "총발행 주식수",
    "hold_stock_rate": "보유 주식 비율",
    "sexdstn": "성별",
    "birth_ym": "출생 년월",
    "ofcps": "직위",
    "rgist_exctv_at": "등기 임원 여부",
    "fte_at": "상근 여부",
    "chrg_job": "담당 업무",
    "main_career": "주요 경력",
    "mxmm_shrholdr_relate": "최대 주주 관계",
    "hffc_pd": "재직 기간",
    "tenure_end_on": "임기 만료일",
    "fo_bbm": "사업부문",
    "reform_bfe_emp_co_rgllbr": "개정 전 직원 수 정규직",
    "reform_bfe_emp_co_cnttk": "개정 전 직원 수 계약직",
    "reform_bfe_emp_co_etc": "개정 전 직원 수 기타",
    "rgllbr_co": "정규직 수",
    "rgllbr_abacpt_labrr_co": "정규직 단시간 근로자 수",
    "cnttk_co": "계약직 수",
    "cnttk_abacpt_labrr_co": "계약직 단시간 근로자 수",
    "sm": "합계",
    "avrg_cnwk_sdytrn": "평균 근속 연수",
    "fyer_salary_totamt": "연간 급여 총액",
    "jan_salary_am": "1인평균 급여 액",
    "mendng_totamt": "보수 총액",
    "mendng_totamt_ct_incls_mendng": "보수 총액 비 포함 보수",
    "nmpr": "인원수",
    "jan_avrg_mendng_am": "1인 평균 보수 액",
    "inv_prm": "법인명",
    "frst_acqs_de": "최초 취득 일자",
    "invstmnt_purps": "출자 목적",
    "frst_acqs_amount": "최초 취득 금액",
    "bsis_blce_qy": "기초 잔액 수량",
    "bsis_blce_qota_rt": "기초 잔액 지분율",
    "bsis_blce_acntbk_amount": "기초 잔액 장부 가액",
    "incrs_dcrs_acqs_dsps_qy": "증가 감소 취득 처분 수량",
    "incrs_dcrs_acqs_dsps_amount": "증가 감소 취득 처분 금액",
    "incrs_dcrs_evl_lstmn": "증가 감소 평가 손액",
    "trmend_blce_qy": "기말 잔액 수량",
    "trmend_blce_qota_rt": "기말 잔액 지분율",
    "trmend_blce_acntbk_amount": "기말 잔액 장부 가액",
    "recent_bsns_year_fnnr_sttus_tot_assets": "최근 사업 연도 재무 현황 총 자산",
    "recent_bsns_year_fnnr_sttus_thstrm_ntpf": "최근 사업 연도 재무 현황 당기 순이익",
    "isu_stock_totqy": "발행할 주식의 총수",
    "now_to_isu_stock_totqy": "현재까지 발행한 주식의 총수",
    "now_to_dcrs_stock_totqy": "현재까지 감소한 주식의 총수",
    "redc": "감자",
    "profit_incnr": "이익소각",
    "rdmstk_repy": "상환주식의 상환",
    "etc": "기타",
    "istc_totqy": "발행주식의 총수",
    "tesstk_co": "자기주식수",
    "distb_stock_co": "유통주식수",
    "scrits_knd_nm": "증권종류",
    "isu_mth_nm": "발행방법",
    "isu_de": "발행일자",
    "facvalu_totamt": "권면(전자등록)총액",
    "intrt": "이자율",
    "evl_grad_instt": "평가등급(평가기관)",
    "mtd": "만기일",
    "repy_at": "상환여부",
    "mngt_cmpny": "주관회사",
    "remndr_exprtn1": "잔여만기",
    "remndr_exprtn2": "잔여만기",
    "de10_below": "10일 이하",
    "de10_excess_de30_below": "10일초과 30일이하",
    "de30_excess_de90_below": "30일초과 90일이하",
    "de90_excess_de180_below": "90일초과 180일이하",
    "de180_excess_yy1_below": "180일초과 1년이하",
    "yy1_below": "1년 이하",
    "yy1_excess_yy2_below": "1년초과 2년이하",
    "yy2_excess_yy3_below": "2년초과 3년이하",
    "yy3_excess_yy4_below": "3년초과 4년이하",
    "yy4_excess_yy5_below": "4년초과 5년이하",
    "yy5_excess_yy10_below": "5년초과 10년이하",
    "yy10_excess_yy20_below": "10년초과 20년이하",
    "yy10_excess": "10년초과",
    "yy1_excess_yy5_below": "1년초과 5년이하",
    "yy10_excess_yy15_below": "10년초과 15년이하",
    "yy15_excess_yy20_below": "15년초과 20년이하",
    "yy20_excess_yy30_below": "20년초과 30년이하",
    "yy30_excess": "30년초과",
    "isu_lmt": "발행 한도",
    "remndr_lmt": "잔여 한도",
    "adtor": "감사인",
    "adt_opinion": "감사의견",
    "adt_reprt_spcmnt_matter": "감사보고서 특기사항",
    "emphs_matter": "강조사항 등",
    "core_adt_matter": "핵심감사사항",
    "cn": "내용",
    "mendng": "보수",
    "tot_reqre_time": "총소요시간",
    "adt_cntrct_dtls_mendng": "감사계약내역(보수)",
    "adt_cntrct_dtls_time": "감사계약내역(시간)",
    "real_exc_dtls_mendng": "실제수행내역(보수)",
    "real_exc_dtls_time": "실제수행내역(시간)",
    "cntrct_cncls_de": "계약체결일",
    "servc_cn": "용역내용",
    "servc_exc_pd": "용역수행기간",
    "servc_mendng": "용역보수",
    "drctr_co": "이사의 수",
    "otcmp_drctr_co": "사외이사 수",
    "apnt": "사외이사 변동현황(선임)",
    "rlsofc": "사외이사 변동현황(해임)",
    "mdstrm_resig": "사외이사 변동현황(중도퇴임)",
    "gmtsck_confm_amount": "주주총회 승인금액",
    "pymnt_totamt": "보수총액",
    "psn1_avrg_pymntamt": "1인당 평균보수액",
    "tm": "회차",
    "pay_de": "납입일",
    "pay_amount": "납입금액",
    "on_dclrt_cptal_use_plan": "신고서상 자금사용 계획",
    "real_cptal_use_sttus": "실제 자금사용 현황",
    "rs_cptal_use_plan_useprps": "증권신고서 등의 자금사용 계획(사용용도)",
    "rs_cptal_use_plan_prcure_amount": "증권신고서 등의 자금사용 계획(조달금액)",
    "real_cptal_use_dtls_cn": "실제 자금사용 내역(내용)",
    "real_cptal_use_dtls_amount": "실제 자금사용 내역(금액)",
    "dffrnc_occrrnc_resn": "차이발생 사유 등",
    "cptal_use_plan": "자금사용 계획",
    "mtrpt_cptal_use_plan_useprps": "주요사항보고서의 자금사용 계획(사용용도)",
    "mtrpt_cptal_use_plan_prcure_amount": "주요사항보고서의 자금사용 계획(조달금액)",
    "rm": "비고",
    "stlm_dt": "결산기준일",
}

# 정기보고서 재무정보

FINANCE_COLUMNS = {
    "crtfc_key": "API 인증키",
    "corp_code": "고유번호",
    "bsns_year": "사업연도",
    "reprt_code": "보고서 코드",
    "idx_cl_code": "지표분류코드",
    "rcept_no": "접수번호",
    "stock_code": "종목 코드",
    "fs_div": "개별/연결구분",
    "fs_nm": "개별/연결명",
    "sj_div": "재무제표구분",
    "sj_nm": "재무제표명",
    "thstrm_nm": "당기명",
    "thstrm_dt": "당기일자",
    "thstrm_amount": "당기금액",
    "thstrm_add_amount": "당기누적금액",
    "account_id": "계정ID",
    "account_nm": "계정명",
    "account_detail": "계정상세",
    "frmtrm_nm": "전기명",
    "frmtrm_dt": "전기일자",
    "frmtrm_amount": "전기금액",
    "frmtrm_q_amount": "전기금액(분/반기)",
    "frmtrm_add_amount": "전기누적금액",
    "bfefrmtrm_nm": "전전기명",
    "bfefrmtrm_dt": "전전기일자",
    "bfefrmtrm_amount": "전전기금액",
    "bfefrmtrm_add_amount": "전전기누적금액",
    "ord": "계정과목 정렬순서",
    "currency": "통화 단위",
    "bsns_de": "기준일",
    "label_kor": "한글 출력명",
    "label_eng": "영문 출력명",
    "data_tp": "데이터 유형",
    "ifrs_ref": "IFRS Reference",
    "stlm_dt": "결산기준일",
    "idx_cl_nm": "지표분류명",
    "idx_code": "지표코드",
    "idx_nm": "지표명",
    "idx_val": "지표값",
}

# 지분공시 종합정보

OWNERSHIP_COLUMNS = {
    "crtfc_key": "API 인증키",
    "corp_code": "고유번호",
    "rcept_no": "접수번호",
    "rcept_dt": "접수일자",
    "corp_name": "회사명",
    "report_tp": "보고구분",
    "repror": "대표보고자",
    "stkqy": "보유주식등의 수",
    "stkqy_irds": "보유주식등의 증감",
    "stkrt": "보유비율",
    "stkrt_irds": "보유비율 증감",
    "ctr_stkqy": "주요체결 주식등의 수",
    "ctr_stkrt": "주요체결 보유비율",
    "report_resn": "보고사유",
    "isu_exctv_rgist_at": "발행 회사 관계 임원(등기여부)",
    "isu_exctv_ofcps": "발행 회사 관계 임원 직위",
    "isu_main_shrholdr": "발행 회사 관계 주요 주주",
    "sp_stock_lmp_cnt": "특정 증권 등 소유 수",
    "sp_stock_lmp_irds_cnt": "특정 증권 등 소유 증감 수",
    "sp_stock_lmp_rate": "특정 증권 등 소유 비율",
    "sp_stock_lmp_irds_rate": "특정 증권 등 소유 증감 비율",
}

# 주요사항보고서 주요정보

MATERIAL_FACTS_COLUMNS = {
    "crtfc_key": "API 인증키",
    "corp_code": "고유번호",
    "bgn_de": "시작일(최초접수일)",
    "end_de": "종료일(최초접수일)",
    "rcept_no": "접수번호",
    "corp_cls": "법인구분",
    "corp_name": "법인명",
    "rp_rsn": "보고 사유",
    "ast_inhtrf_prc": "자산양수ㆍ도 가액",
    "df_cn": "부도내용",
    "df_amt": "부도금액",
    "df_bnk": "부도발생은행",
    "dfd": "최종부도(당좌거래정지)일자",
    "df_rs": "부도사유 및 경위",
    "bsnsp_rm": "영업정지 분야",
    "bsnsp_amt": "영업정지 내역(영업정지금액)",
    "rsl": "영업정지 내역(최근매출총액)",
    "sl_vs": "영업정지 내역(매출액 대비)",
    "ls_atn": "영업정지 내역(대규모법인여부)",
    "krx_stt_atn": "영업정지 내역(거래소 의무공시 해당 여부)",
    "bsnsp_cn": "영업정지 내용",
    "bsnsp_rs": "영업정지사유",
    "ft_ctp": "향후대책",
    "bsnsp_af": "영업정지영향",
    "bsnspd": "영업정지일자",
    "bddd": "이사회결의일(결정일)",
    "od_a_at_t": "사외이사참석여부(참석(명))",
    "od_a_at_b": "사외이사참석여부(불참(명))",
    "adt_a_atn": "감사(사외이사가 아닌 감사위원) 참석여부",
    "apcnt": "신청인 (회사와의 관계)",
    "cpct": "관할법원",
    "rq_rs": "신청사유",
    "rqd": "신청일자",
    "ft_ctp_sc": "향후대책 및 일정",
    "ds_rs": "해산사유",
    "ds_rsd": "해산사유발생일(결정일)",
    "nstk_ostk_cnt": "신주의 종류와 수(보통주식 (주))",
    "nstk_estk_cnt": "신주의 종류와 수(기타주식 (주))",
    "fv_ps": "1주당 액면가액 (원)",
    "bfic_tisstk_ostk": "증자전 발행주식총수 (주)(보통주식 (주))",
    "bfic_tisstk_estk": "증자전 발행주식총수 (주)(기타주식 (주))",
    "fdpp_fclt": "자금조달의 목적(시설자금 (원))",
    "fdpp_bsninh": "자금조달의 목적(영업양수자금 (원))",
    "fdpp_op": "자금조달의 목적(운영자금 (원))",
    "fdpp_dtrp": "자금조달의 목적(채무상환자금 (원))",
    "fdpp_ocsa": "자금조달의 목적(타법인 증권 취득자금 (원))",
    "fdpp_etc": "자금조달의 목적(기타자금 (원))",
    "ic_mthn": "증자방식",
    "ssl_at": "공매도 해당여부",
    "ssl_bgd": "공매도 시작일",
    "ssl_edd": "공매도 종료일",
    "nstk_asstd": "신주배정기준일",
    "nstk_ascnt_ps_ostk": "1주당 신주배정 주식수(보통주식 (주))",
    "nstk_ascnt_ps_estk": "1주당 신주배정 주식수(기타주식 (주))",
    "nstk_dividrk": "신주의 배당기산일",
    "nstk_dlprd": "신주권교부예정일",
    "nstk_lstprd": "신주의 상장 예정일",
    "piic_nstk_ostk_cnt": "유상증자(신주의 종류와 수(보통주식 (주)))",
    "piic_nstk_estk_cnt": "유상증자(신주의 종류와 수(기타주식 (주)))",
    "piic_fv_ps": "유상증자(1주당 액면가액 (원))",
    "piic_bfic_tisstk_ostk": "유상증자(증자전 발행주식총수 (주)(보통주식 (주)))",
    "piic_bfic_tisstk_estk": "유상증자(증자전 발행주식총수 (주)(기타주식 (주)))",
    "piic_fdpp_fclt": "유상증자(자금조달의 목적(시설자금 (원)))",
    "piic_fdpp_bsninh": "유상증자(자금조달의 목적(영업양수자금 (원)))",
    "piic_fdpp_op": "유상증자(자금조달의 목적(운영자금 (원)))",
    "piic_fdpp_dtrp": "유상증자(자금조달의 목적(채무상환자금 (원)))",
    "piic_fdpp_ocsa": "유상증자(자금조달의 목적(타법인 증권 취득자금 (원)))",
    "piic_fdpp_etc": "유상증자(자금조달의 목적(기타자금 (원)))",
    "piic_ic_mthn": "유상증자(증자방식)",
    "fric_nstk_ostk_cnt": "무상증자(신주의 종류와 수(보통주식 (주)))",
    "fric_nstk_estk_cnt": "무상증자(신주의 종류와 수(기타주식 (주)))",
    "fric_fv_ps": "무상증자(1주당 액면가액 (원))",
    "fric_bfic_tisstk_ostk": "무상증자(증자전 발행주식총수(보통주식 (주)))",
    "fric_bfic_tisstk_estk": "무상증자(증자전 발행주식총수(기타주식 (주)))",
    "fric_nstk_asstd": "무상증자(신주배정기준일)",
    "fric_nstk_ascnt_ps_ostk": "무상증자(1주당 신주배정 주식수(보통주식 (주)))",
    "fric_nstk_ascnt_ps_estk": "무상증자(1주당 신주배정 주식수(기타주식 (주)))",
    "fric_nstk_dividrk": "무상증자(신주의 배당기산일)",
    "fric_nstk_dlprd": "무상증자(신주권교부예정일)",
    "fric_nstk_lstprd": "무상증자(신주의 상장 예정일)",
    "fric_bddd": "무상증자(이사회결의일(결정일))",
    "fric_od_a_at_t": "무상증자(사외이사 참석여부(참석(명)))",
    "fric_od_a_at_b": "무상증자(사외이사 참석여부(불참(명)))",
    "fric_adt_a_atn": "무상증자(감사(감사위원)참석 여부)",
    "crstk_ostk_cnt": "감자주식의 종류와 수(보통주식 (주))",
    "crstk_estk_cnt": "감자주식의 종류와 수(기타주식 (주))",
    "bfcr_cpt": "감자전후 자본금(감자전 (원))",
    "atcr_cpt": "감자전후 자본금(감자후 (원))",
    "bfcr_tisstk_ostk": "감자전후 발행주식수(보통주식 (주)(감자전 (원)))",
    "atcr_tisstk_ostk": "감자전후 발행주식수(보통주식 (주)(감자후 (원)))",
    "bfcr_tisstk_estk": "감자전후 발행주식수(기타주식 (주)(감자전 (원)))",
    "atcr_tisstk_estk": "감자전후 발행주식수(기타주식 (주)(감자후 (원)))",
    "cr_rt_ostk": "감자비율(보통주식 (%))",
    "cr_rt_estk": "감자비율(기타주식 (%))",
    "cr_std": "감자기준일",
    "cr_mth": "감자방법",
    "cr_rs": "감자사유",
    "crsc_gmtsck_prd": "감자일정(주주총회 예정일)",
    "crsc_trnmsppd": "감자일정(명의개서정지기간)",
    "crsc_osprpd": "감자일정(구주권 제출기간)",
    "crsc_trspprpd": "감자일정(매매거래 정지예정기간)",
    "crsc_osprpd_bgd": "감자일정(구주권 제출기간(시작일))",
    "crsc_osprpd_edd": "감자일정(구주권 제출기간(종료일))",
    "crsc_trspprpd_bgd": "감자일정(매매거래 정지예정기간(시작일))",
    "crsc_trspprpd_edd": "감자일정(매매거래 정지예정기간(종료일))",
    "crsc_nstkdlprd": "감자일정(신주권교부예정일)",
    "crsc_nstklstprd": "감자일정(신주상장예정일)",
    "cdobprpd_bgd": "채권자 이의제출기간(시작일)",
    "cdobprpd_edd": "채권자 이의제출기간(종료일)",
    "ospr_nstkdl_pl": "구주권제출 및 신주권교부장소",
    "ftc_stt_atn": "공정거래위원회 신고대상 여부",
    "mngt_pcbg_dd": "관리절차개시 결정일자",
    "mngt_int": "관리기관",
    "mngt_pd": "관리기간",
    "mngt_rs": "관리사유",
    "cfd": "확인일자",
    "icnm": "사건의 명칭",
    "ac_ap": "원고ㆍ신청인",
    "rq_cn": "청구내용",
    "lgd": "제기일자",
    "lstprstk_ostk_cnt": "상장예정주식 종류ㆍ수(주)(보통주식)",
    "lstprstk_estk_cnt": "상장예정주식 종류ㆍ수(주)(기타주식)",
    "tisstk_ostk": "발행주식 총수(주)(보통주식)",
    "tisstk_estk": "발행주식 총수(주)(기타주식)",
    "psmth_nstk_sl": "공모방법(신주발행 (주))",
    "psmth_ostk_sl": "공모방법(구주매출 (주))",
    "fdpp": "자금조달(신주발행) 목적",
    "lststk_orlst": "상장증권(원주상장 (주))",
    "lststk_drlst": "상장증권(DR상장 (주))",
    "lstex_nt": "상장거래소(소재국가)",
    "lstpp": "해외상장목적",
    "lstprd": "상장예정일자",
    "dlststk_ostk_cnt": "상장폐지주식 종류ㆍ수(주)(보통주식)",
    "dlststk_estk_cnt": "상장폐지주식 종류ㆍ수(주)(기타주식)",
    "dlstrq_prd": "폐지신청예정일자",
    "dlst_prd": "폐지(예정)일자",
    "tredd": "매매거래종료일",
    "dlst_rs": "폐지사유",
    "stk_cd": "종목 명 (code)",
    "lstd": "상장일자",
    "bd_tm": "사채의 종류(회차)",
    "bd_knd": "사채의 종류(종류)",
    "bd_fta": "사채의 권면(전자등록)총액 (원)",
    "atcsc_rmislmt": "정관상 잔여 발행한도 (원)",
    "ovis_fta": "해외발행(권면(전자등록)총액)",
    "ovis_fta_crn": "해외발행(권면(전자등록)총액(통화단위))",
    "ovis_ster": "해외발행(기준환율등)",
    "ovis_isar": "해외발행(발행지역)",
    "ovis_mktnm": "해외발행(해외상장시 시장의 명칭)",
    "bd_intr_ex": "사채의 이율(표면이자율 (%))",
    "bd_intr_sf": "사채의 이율(만기이자율 (%))",
    "bd_mtd": "사채만기일",
    "bdis_mthn": "사채발행방법",
    "cv_rt": "전환에 관한 사항(전환비율 (%))",
    "cv_prc": "전환에 관한 사항(전환가액 (원/주))",
    "cvisstk_knd": "전환에 관한 사항(전환에 따라 발행할 주식(종류))",
    "cvisstk_cnt": "전환에 관한 사항(전환에 따라 발행할 주식(주식수))",
    "cvisstk_tisstk_vs": "전환에 관한 사항(전환에 따라 발행할 주식(주식총수 대비 비율(%)))",
    "cvrqpd_bgd": "전환에 관한 사항(전환청구기간(시작일))",
    "cvrqpd_edd": "전환에 관한 사항(전환청구기간(종료일))",
    "act_mktprcfl_cvprc_lwtrsprc": "전환에 관한 사항(시가하락에 따른 전환가액 조정(최저 조정가액 (원)))",
    "act_mktprcfl_cvprc_lwtrsprc_bs": "전환에 관한 사항(시가하락에 따른 전환가액 조정(최저 조정가액 근거))",
    "rmislmt_lt70p": "전환에 관한 사항(시가하락에 따른 전환가액 조정(발행당시 전환가액의 70% 미만으로 조정가능한 잔여 발행한도 (원)))",
    "abmg": "합병 관련 사항",
    "sbd": "청약일",
    "pymd": "납입일",
    "rpmcmp": "대표주관회사",
    "grint": "보증기관",
    "rs_sm_atn": "증권신고서 제출대상 여부",
    "ex_sm_r": "제출을 면제받은 경우 그 사유",
    "ovis_ltdtl": "당해 사채의 해외발행과 연계된 대차거래 내역",
    "ex_rt": "신주인수권에 관한 사항(행사비율 (%))",
    "ex_prc": "신주인수권에 관한 사항(행사가액 (원/주))",
    "ex_prc_dmth": "신주인수권에 관한 사항(행사가액 결정방법)",
    "bdwt_div_atn": "신주인수권에 관한 사항(사채와 인수권의 분리여부)",
    "nstk_pym_mth": "신주인수권에 관한 사항(신주대금 납입방법)",
    "nstk_isstk_knd": "신주인수권에 관한 사항(신주인수권 행사에 따라 발행할 주식(종류))",
    "nstk_isstk_cnt": "신주인수권에 관한 사항(신주인수권 행사에 따라 발행할 주식(주식수))",
    "nstk_isstk_tisstk_vs": "신주인수권에 관한 사항(신주인수권 행사에 따라 발행할 주식(주식총수 대비 비율(%)))",
    "expd_bgd": "신주인수권에 관한 사항(권리행사기간(시작일))",
    "expd_edd": "신주인수권에 관한 사항(권리행사기간(종료일))",
    "extg": "교환에 관한 사항(교환대상(종류))",
    "extg_stkcnt": "교환에 관한 사항(교환대상(주식수))",
    "extg_tisstk_vs": "교환에 관한 사항(교환대상(주식총수 대비 비율(%)))",
    "exrqpd_bgd": "교환에 관한 사항(교환청구기간(시작일))",
    "exrqpd_edd": "교환에 관한 사항(교환청구기간(종료일))",
    "mngt_pcsp_dd": "관리절차중단 결정일자",
    "dbtrs_sc": "채무재조정에 관한 사항(채무재조정의 범위)",
    "aqpln_stk_ostk": "취득예정주식(주)(보통주식)",
    "aqpln_stk_estk": "취득예정주식(주)(기타주식)",
    "aqpln_prc_ostk": "취득예정금액(원)(보통주식)",
    "aqpln_prc_estk": "취득예정금액(원)(기타주식)",
    "aqexpd_bgd": "취득예상기간(시작일)",
    "aqexpd_edd": "취득예상기간(종료일)",
    "hdexpd_bgd": "보유예상기간(시작일)",
    "hdexpd_edd": "보유예상기간(종료일)",
    "aq_pp": "취득목적",
    "aq_mth": "취득방법",
    "cs_iv_bk": "위탁투자중개업자",
    "aq_wtn_div_ostk": "취득 전 자기주식 보유현황(배당가능이익 범위 내 취득(주)(보통주식))",
    "aq_wtn_div_ostk_rt": "취득 전 자기주식 보유현황(배당가능이익 범위 내 취득(주)(비율(%)))",
    "aq_wtn_div_estk": "취득 전 자기주식 보유현황(배당가능이익 범위 내 취득(주)(기타주식))",
    "aq_wtn_div_estk_rt": "취득 전 자기주식 보유현황(배당가능이익 범위 내 취득(주)(비율(%)))",
    "eaq_ostk": "취득 전 자기주식 보유현황(기타취득(주)(보통주식))",
    "eaq_ostk_rt": "취득 전 자기주식 보유현황(기타취득(주)(비율(%)))",
    "eaq_estk": "취득 전 자기주식 보유현황(기타취득(주)(기타주식))",
    "eaq_estk_rt": "취득 전 자기주식 보유현황(기타취득(주)(비율(%)))",
    "aq_dd": "취득결정일",
    "d1_prodlm_ostk": "1일 매수 주문수량 한도(보통주식)",
    "d1_prodlm_estk": "1일 매수 주문수량 한도(기타주식)",
    "dppln_stk_ostk": "처분예정주식(주)(보통주식)",
    "dppln_stk_estk": "처분예정주식(주)(기타주식)",
    "dpstk_prc_ostk": "처분 대상 주식가격(원)(보통주식)",
    "dpstk_prc_estk": "처분 대상 주식가격(원)(기타주식)",
    "dppln_prc_ostk": "처분예정금액(원)(보통주식)",
    "dppln_prc_estk": "처분예정금액(원)(기타주식)",
    "dpprpd_bgd": "처분예정기간(시작일)",
    "dpprpd_edd": "처분예정기간(종료일)",
    "dp_pp": "처분목적",
    "dp_m_mkt": "처분방법(시장을 통한 매도(주))",
    "dp_m_ovtm": "처분방법(시간외대량매매(주))",
    "dp_m_otc": "처분방법(장외처분(주))",
    "dp_m_etc": "처분방법(기타(주))",
    "dp_dd": "처분결정일",
    "d1_slodlm_ostk": "1일 매도 주문수량 한도(보통주식)",
    "d1_slodlm_estk": "1일 매도 주문수량 한도(기타주식)",
    "ctr_prc": "계약금액(원)",
    "ctr_pd_bgd": "계약기간(시작일)",
    "ctr_pd_edd": "계약기간(종료일)",
    "ctr_pp": "계약목적",
    "ctr_cns_int": "계약체결기관",
    "ctr_cns_prd": "계약체결 예정일자",
    "ctr_prc_bfcc": "계약금액(원)(해지 전)",
    "ctr_prc_atcc": "계약금액(원)(해지 후)",
    "ctr_pd_bfcc_bgd": "해지 전 계약기간(시작일)",
    "ctr_pd_bfcc_edd": "해지 전 계약기간(종료일)",
    "cc_pp": "해지목적",
    "cc_int": "해지기관",
    "cc_prd": "해지예정일자",
    "tp_rm_atcc": "해지후 신탁재산의 반환방법",
    "inh_bsn": "양수영업",
    "inh_bsn_mc": "양수영업 주요내용",
    "inh_prc": "양수가액(원)",
    "absn_inh_atn": "영업전부의 양수 여부",
    "ast_inh_bsn": "재무내용(원)(자산액(양수대상 영업부문(A)))",
    "ast_cmp_all": "재무내용(원)(자산액(당사전체(B)))",
    "ast_rt": "재무내용(원)(자산액(비중(%)(A/B)))",
    "sl_inh_bsn": "재무내용(원)(매출액(양수대상 영업부문(A)))",
    "sl_cmp_all": "재무내용(원)(매출액(당사전체(B)))",
    "sl_rt": "재무내용(원)(매출액(비중(%)(A/B)))",
    "dbt_inh_bsn": "재무내용(원)(부채액(양수대상 영업부문(A)))",
    "dbt_cmp_all": "재무내용(원)(부채액(당사전체(B)))",
    "dbt_rt": "재무내용(원)(부채액(비중(%)(A/B)))",
    "inh_pp": "양수목적",
    "inh_af": "양수영향",
    "inh_prd_ctr_cnsd": "양수예정일자(계약체결일)",
    "inh_prd_inh_std": "양수예정일자(양수기준일)",
    "dlptn_cmpnm": "거래상대방(회사명(성명))",
    "dlptn_cpt": "거래상대방(자본금(원))",
    "dlptn_mbsn": "거래상대방(주요사업)",
    "dlptn_hoadd": "거래상대방(본점소재지(주소))",
    "dlptn_rl_cmpn": "거래상대방(회사와의 관계)",
    "inh_pym": "양수대금지급",
    "exevl_atn": "외부평가에 관한 사항(외부평가 여부)",
    "exevl_bs_rs": "외부평가에 관한 사항(근거 및 사유)",
    "exevl_intn": "외부평가에 관한 사항(외부평가기관의 명칭)",
    "exevl_pd": "외부평가에 관한 사항(외부평가 기간)",
    "exevl_op": "외부평가에 관한 사항(외부평가 의견)",
    "gmtsck_spd_atn": "주주총회 특별결의 여부",
    "gmtsck_prd": "주주총회 예정일",
    "aprskh_plnprc": "주식매수청구권에 관한 사항(매수예정가격)",
    "aprskh_pym_plpd_mth": "주식매수청구권에 관한 사항(지급예정시기, 지급방법)",
    "aprskh_lmt": "주식매수청구권에 관한 사항(주식매수청구권 제한 관련 내용)",
    "aprskh_ctref": "주식매수청구권에 관한 사항(계약에 미치는 효력)",
    "bdlst_atn": "우회상장 해당 여부",
    "n6m_tpai_plann": "향후 6월이내 제3자배정 증자 등 계획",
    "otcpr_bdlst_sf_atn": "타법인의 우회상장 요건 충족 여부",
    "popt_ctr_atn": "풋옵션 등 계약 체결여부",
    "popt_ctr_cn": "계약내용",
    "trf_bsn": "양도영업",
    "trf_bsn_mc": "양도영업 주요내용",
    "trf_prc": "양도가액(원)",
    "ast_trf_bsn": "재무내용(원)(자산액(양도대상 영업부문(A)))",
    "sl_trf_bsn": "재무내용(원)(매출액(양도대상 영업부문(A)))",
    "trf_pp": "양도목적",
    "trf_af": "양도영향",
    "trf_prd_ctr_cnsd": "양도예정일자(계약체결일)",
    "trf_prd_trf_std": "양도예정일자(양도기준일)",
    "trf_pym": "양도대금지급",
    "ast_sen": "자산구분",
    "ast_nm": "자산명",
    "inhdtl_inhprc": "양수내역(양수금액(원)(A))",
    "inhdtl_tast": "양수내역(총자산(원)(B))",
    "inhdtl_tast_vs": "양수내역(총자산대비(%)(A/B))",
    "inh_prd_rgs_prd": "양수예정일자(등기예정일)",
    "dl_pym": "거래대금지급",
    "aprskh_exrq": "주식매수청구권에 관한 사항(행사요건)",
    "aprskh_ex_pc_mth_pd_pl": "주식매수청구권에 관한 사항(행사절차, 방법, 기간, 장소)",
    "trfdtl_trfprc": "양도내역(양도금액(원)(A))",
    "trfdtl_tast": "양도내역(총자산(원)(B))",
    "trfdtl_tast_vs": "양도내역(총자산대비(%)(A/B))",
    "trf_prd_rgs_prd": "양도예정일자(등기예정일)",
    "iscmp_cmpnm": "발행회사(회사명)",
    "iscmp_nt": "발행회사(국적)",
    "iscmp_rp": "발행회사(대표자)",
    "iscmp_cpt": "발행회사(자본금(원))",
    "iscmp_rl_cmpn": "발행회사(회사와 관계)",
    "iscmp_tisstk": "발행회사(발행주식 총수(주))",
    "iscmp_mbsn": "발행회사(주요사업)",
    "l6m_tpa_nstkaq_atn": "최근 6월 이내 제3자 배정에 의한 신주취득 여부",
    "inhdtl_stkcnt": "양수내역(양수주식수(주))",
    "inhdtl_ecpt": "양수내역(자기자본(원)(C))",
    "inhdtl_ecpt_vs": "양수내역(자기자본대비(%)(A/C))",
    "atinh_owstkcnt": "양수후 소유주식수 및 지분비율(소유주식수(주))",
    "atinh_eqrt": "양수후 소유주식수 및 지분비율(지분비율(%))",
    "inh_prd": "양수예정일자",
    "iscmp_bdlst_sf_atn": "발행회사(타법인)의 우회상장 요건 충족여부",
    "trfdtl_stkcnt": "양도내역(양도주식수(주))",
    "trfdtl_ecpt": "양도내역(자기자본(원)(C))",
    "trfdtl_ecpt_vs": "양도내역(자기자본대비(%)(A/C))",
    "attrf_owstkcnt": "양도후 소유주식수 및 지분비율(소유주식수(주))",
    "attrf_eqrt": "양도후 소유주식수 및 지분비율(지분비율(%))",
    "trf_prd": "양도예정일자",
    "stkrtbd_kndn": "주권 관련 사채권의 종류",
    "tm": "주권 관련 사채권의 종류(회차)",
    "knd": "주권 관련 사채권의 종류(종류)",
    "bdiscmp_cmpnm": "사채권 발행회사(회사명)",
    "bdiscmp_nt": "사채권 발행회사(국적)",
    "bdiscmp_rp": "사채권 발행회사(대표자)",
    "bdiscmp_cpt": "사채권 발행회사(자본금(원))",
    "bdiscmp_rl_cmpn": "사채권 발행회사(회사와 관계)",
    "bdiscmp_tisstk": "사채권 발행회사(발행주식 총수(주))",
    "bdiscmp_mbsn": "사채권 발행회사(주요사업)",
    "inhdtl_bd_fta": "양수내역(사채의 권면(전자등록)총액(원))",
    "aqd": "취득일자",
    "trfdtl_bd_fta": "양도내역(사채의 권면(전자등록)총액(원))",
    "mg_mth": "합병방법",
    "mg_stn": "합병에 관한 사항(합병형태)",
    "mg_pp": "합병목적",
    "mg_rt": "합병비율",
    "mg_rt_bs": "합병비율 산출근거",
    "mgnstk_ostk_cnt": "합병신주의 종류와 수(주)(보통주식)",
    "mgnstk_cstk_cnt": "합병신주의 종류와 수(주)(종류주식)",
    "mgptncmp_cmpnm": "합병에 관한 사항(합병상대 회사(회사명))",
    "mgptncmp_mbsn": "합병에 관한 사항(합병상대 회사(주요사업))",
    "mgptncmp_rl_cmpn": "합병에 관한 사항(합병상대 회사(회사와의 관계))",
    "rbsnfdtl_tast": "교환ㆍ이전 대상법인(최근 사업연도 요약재무내용(원)(자산총계))",
    "rbsnfdtl_tdbt": "교환ㆍ이전 대상법인(최근 사업연도 요약재무내용(원)(부채총계))",
    "rbsnfdtl_teqt": "교환ㆍ이전 대상법인(최근 사업연도 요약재무내용(원)(자본총계))",
    "rbsnfdtl_cpt": "교환ㆍ이전 대상법인(최근 사업연도 요약재무내용(원)(자본금))",
    "rbsnfdtl_sl": "합병에 관한 사항(합병상대 회사(최근 사업연도 재무내용(원)(매출액)))",
    "rbsnfdtl_nic": "합병에 관한 사항(합병상대 회사(최근 사업연도 재무내용(원)(당기순이익)))",
    "eadtat_intn": "합병에 관한 사항(합병상대 회사(외부감사 여부(기관명)))",
    "eadtat_op": "합병에 관한 사항(합병상대 회사(외부감사 여부(감사의견)))",
    "nmgcmp_cmpnm": "합병에 관한 사항(합병신설 회사(회사명))",
    "ffdtl_tast": "분할에 관한 사항(분할설립 회사(설립시 재무내용(원)(자산총계)))",
    "ffdtl_tdbt": "분할에 관한 사항(분할설립 회사(설립시 재무내용(원)(부채총계)))",
    "ffdtl_teqt": "분할에 관한 사항(분할설립 회사(설립시 재무내용(원)(자본총계)))",
    "ffdtl_cpt": "분할에 관한 사항(분할설립 회사(설립시 재무내용(원)(자본금)))",
    "ffdtl_std": "분할에 관한 사항(분할설립 회사(설립시 재무내용(원)(현재기준)))",
    "nmgcmp_nbsn_rsl": "신설합병회사(신설사업부문 최근 사업연도 매출액(원))",
    "nmgcmp_mbsn": "합병에 관한 사항(합병신설 회사(주요사업))",
    "nmgcmp_rlst_atn": "합병에 관한 사항(합병신설 회사(재상장신청 여부))",
    "mgsc_mgctrd": "합병일정(합병계약일)",
    "mgsc_shddstd": "합병일정(주주확정기준일)",
    "mgsc_shclspd_bgd": "합병일정(주주명부 폐쇄기간(시작일))",
    "mgsc_shclspd_edd": "합병일정(주주명부 폐쇄기간(종료일))",
    "mgsc_mgop_rcpd_bgd": "합병일정(합병반대의사통지 접수기간(시작일))",
    "mgsc_mgop_rcpd_edd": "합병일정(합병반대의사통지 접수기간(종료일))",
    "mgsc_gmtsck_prd": "합병일정(주주총회예정일자)",
    "mgsc_aprskh_expd_bgd": "합병일정(주식매수청구권 행사기간(시작일))",
    "mgsc_aprskh_expd_edd": "합병일정(주식매수청구권 행사기간(종료일))",
    "mgsc_osprpd_bgd": "합병일정(구주권 제출기간(시작일))",
    "mgsc_osprpd_edd": "합병일정(구주권 제출기간(종료일))",
    "mgsc_trspprpd_bgd": "합병일정(매매거래 정지예정기간(시작일))",
    "mgsc_trspprpd_edd": "합병일정(매매거래 정지예정기간(종료일))",
    "mgsc_cdobprpd_bgd": "합병일정(채권자이의 제출기간(시작일))",
    "mgsc_cdobprpd_edd": "합병일정(채권자이의 제출기간(종료일))",
    "mgsc_mgdt": "합병일정(합병기일)",
    "mgsc_ergmd": "합병일정(종료보고 총회일)",
    "mgsc_mgrgsprd": "합병일정(합병등기예정일자)",
    "mgsc_nstkdlprd": "합병일정(신주권교부예정일)",
    "mgsc_nstklstprd": "합병일정(신주의 상장예정일)",
    "dv_mth": "분할방법",
    "dv_impef": "분할의 중요영향 및 효과",
    "dv_rt": "분할비율",
    "dv_trfbsnprt_cn": "분할에 관한 사항(분할로 이전할 사업 및 재산의 내용)",
    "atdv_excmp_cmpnm": "분할에 관한 사항(분할 후 존속회사(회사명))",
    "atdvfdtl_tast": "분할에 관한 사항(분할 후 존속회사(분할후 재무내용(원)(자산총계)))",
    "atdvfdtl_tdbt": "분할에 관한 사항(분할 후 존속회사(분할후 재무내용(원)(부채총계)))",
    "atdvfdtl_teqt": "분할에 관한 사항(분할 후 존속회사(분할후 재무내용(원)(자본총계)))",
    "atdvfdtl_cpt": "분할에 관한 사항(분할 후 존속회사(분할후 재무내용(원)(자본금)))",
    "atdvfdtl_std": "분할에 관한 사항(분할 후 존속회사(분할후 재무내용(원)(현재기준)))",
    "atdv_excmp_exbsn_rsl": "분할에 관한 사항(분할 후 존속회사(존속사업부문 최근 사업연도매출액(원)))",
    "atdv_excmp_mbsn": "분할에 관한 사항(분할 후 존속회사(주요사업))",
    "atdv_excmp_atdv_lstmn_atn": "분할에 관한 사항(분할 후 존속회사(분할 후 상장유지 여부))",
    "dvfcmp_cmpnm": "분할에 관한 사항(분할설립 회사(회사명))",
    "dvfcmp_nbsn_rsl": "분할에 관한 사항(분할설립 회사(신설사업부문 최근 사업연도 매출액(원)))",
    "dvfcmp_mbsn": "분할에 관한 사항(분할설립 회사(주요사업))",
    "dvfcmp_rlst_atn": "분할설립회사(재상장신청 여부)",
    "abcr_crrt": "분할에 관한 사항(감자에 관한 사항(감자비율(%)))",
    "abcr_osprpd_bgd": "분할에 관한 사항(감자에 관한 사항(구주권 제출기간(시작일)))",
    "abcr_osprpd_edd": "분할에 관한 사항(감자에 관한 사항(구주권 제출기간(종료일)))",
    "abcr_trspprpd_bgd": "분할에 관한 사항(감자에 관한 사항(매매거래정지 예정기간(시작일)))",
    "abcr_trspprpd_edd": "분할에 관한 사항(감자에 관한 사항(매매거래정지 예정기간(종료일)))",
    "abcr_nstkascnd": "분할에 관한 사항(감자에 관한 사항(신주배정조건))",
    "abcr_shstkcnt_rt_at_rs": "분할에 관한 사항(감자에 관한 사항(주주 주식수 비례여부 및 사유))",
    "abcr_nstkasstd": "분할에 관한 사항(감자에 관한 사항(신주배정기준일))",
    "abcr_nstkdlprd": "분할에 관한 사항(감자에 관한 사항(신주권교부예정일))",
    "abcr_nstklstprd": "분할에 관한 사항(감자에 관한 사항(신주의 상장예정일))",
    "dvdt": "분할기일",
    "dvrgsprd": "분할등기 예정일",
    "dvmg_mth": "분할합병 방법",
    "dvmg_impef": "분할합병의 중요영향 및 효과",
    "dvfcmp_atdv_lstmn_at": "분할에 관한 사항(분할설립 회사(분할후 상장유지여부))",
    "dvmgnstk_ostk_cnt": "합병에 관한 사항(분할합병신주의 종류와 수(주)(보통주식))",
    "dvmgnstk_cstk_cnt": "합병에 관한 사항(분할합병신주의 종류와 수(주)(종류주식))",
    "nmgcmp_cpt": "합병에 관한 사항(합병신설 회사(자본금(원)))",
    "dvmg_rt": "분할합병비율",
    "dvmg_rt_bs": "분할합병비율 산출근거",
    "dvmgsc_dvmgctrd": "분할합병일정(분할합병계약일)",
    "dvmgsc_shddstd": "분할합병일정(주주확정기준일)",
    "dvmgsc_shclspd_bgd": "분할합병일정(주주명부 폐쇄기간(시작일))",
    "dvmgsc_shclspd_edd": "분할합병일정(주주명부 폐쇄기간(종료일))",
    "dvmgsc_dvmgop_rcpd_bgd": "분할합병일정(분할합병반대의사통지 접수기간(시작일))",
    "dvmgsc_dvmgop_rcpd_edd": "분할합병일정(분할합병반대의사통지 접수기간(종료일))",
    "dvmgsc_gmtsck_prd": "분할합병일정(주주총회예정일자)",
    "dvmgsc_aprskh_expd_bgd": "분할합병일정(주식매수청구권 행사기간(시작일))",
    "dvmgsc_aprskh_expd_edd": "분할합병일정(주식매수청구권 행사기간(종료일))",
    "dvmgsc_cdobprpd_bgd": "분할합병일정(채권자 이의 제출기간(시작일))",
    "dvmgsc_cdobprpd_edd": "분할합병일정(채권자 이의 제출기간(종료일))",
    "dvmgsc_dvmgdt": "분할합병일정(분할합병기일)",
    "dvmgsc_ergmd": "분할합병일정(종료보고 총회일)",
    "dvmgsc_dvmgrgsprd": "분할합병일정(분할합병등기예정일)",
    "extr_sen": "구분",
    "extr_stn": "교환ㆍ이전 형태",
    "extr_tgcmp_cmpnm": "교환ㆍ이전 대상법인(회사명)",
    "extr_tgcmp_rp": "교환ㆍ이전 대상법인(대표자)",
    "extr_tgcmp_mbsn": "교환ㆍ이전 대상법인(주요사업)",
    "extr_tgcmp_rl_cmpn": "교환ㆍ이전 대상법인(회사와의 관계)",
    "extr_tgcmp_tisstk_ostk": "교환ㆍ이전 대상법인(발행주식총수(주)(보통주식))",
    "extr_tgcmp_tisstk_cstk": "교환ㆍ이전 대상법인(발행주식총수(주)(종류주식))",
    "extr_rt": "교환ㆍ이전 비율",
    "extr_rt_bs": "교환ㆍ이전 비율 산출근거",
    "extr_pp": "교환ㆍ이전 목적",
    "extrsc_extrctrd": "교환ㆍ이전일정(교환ㆍ이전계약일)",
    "extrsc_shddstd": "교환ㆍ이전일정(주주확정기준일)",
    "extrsc_shclspd_bgd": "교환ㆍ이전일정(주주명부 폐쇄기간(시작일))",
    "extrsc_shclspd_edd": "교환ㆍ이전일정(주주명부 폐쇄기간(종료일))",
    "extrsc_extrop_rcpd_bgd": "교환ㆍ이전일정(주식교환ㆍ이전 반대의사 통지접수기간(시작일))",
    "extrsc_extrop_rcpd_edd": "교환ㆍ이전일정(주식교환ㆍ이전 반대의사 통지접수기간(종료일))",
    "extrsc_gmtsck_prd": "교환ㆍ이전일정(주주총회 예정일자)",
    "extrsc_aprskh_expd_bgd": "교환ㆍ이전일정(주식매수청구권 행사기간(시작일))",
    "extrsc_aprskh_expd_edd": "교환ㆍ이전일정(주식매수청구권 행사기간(종료일))",
    "extrsc_osprpd_bgd": "교환ㆍ이전일정(구주권제출기간(시작일))",
    "extrsc_osprpd_edd": "교환ㆍ이전일정(구주권제출기간(종료일))",
    "extrsc_trspprpd": "교환ㆍ이전일정(매매거래정지예정기간)",
    "extrsc_trspprpd_bgd": "교환ㆍ이전일정(매매거래정지예정기간(시작일))",
    "extrsc_trspprpd_edd": "교환ㆍ이전일정(매매거래정지예정기간(종료일))",
    "extrsc_extrdt": "교환ㆍ이전일정(교환ㆍ이전일자)",
    "extrsc_nstkdlprd": "교환ㆍ이전일정(신주권교부예정일)",
    "extrsc_nstklstprd": "교환ㆍ이전일정(신주의 상장예정일)",
    "atextr_cpcmpnm": "교환ㆍ이전 후 완전모회사명"
}

# 증권신고서 주요정보

REGISTRATION_COLUMNS = {
    "crtfc_key": "API 인증키",
    "bgn_de": "시작일(최초접수일)",
    "end_de": "종료일(최초접수일)",
    "rcept_no": "접수번호",
    "corp_cls": "법인구분",
    "corp_code": "고유번호",
    "corp_name": "회사명",
    "stock_code": "종목코드",
    "stn": "형태",
    "bddd": "이사회 결의일",
    "ctrd": "계약일",
    "gmtsck_shddstd": "주주총회를 위한 주주확정일",
    "ap_gmtsck": "승인을 위한 주주총회일",
    "aprskh_pd_bgd": "주식매수청구권 행사 기간 및 가격(시작일)",
    "aprskh_pd_edd": "주식매수청구권 행사 기간 및 가격(종료일)",
    "aprskh_prc": "주식매수청구권 행사 기간 및 가격((주식매수청구가격-회사제시))",
    "mgdt_etc": "합병기일등",
    "rt_vl": "비율 또는 가액",
    "exevl_int": "외부평가기관",
    "grtmn_etc": "지급 교부금 등",
    "rpt_rcpn": "주요사항보고서(접수번호)",
    "cmpnm": "회사명",
    "fv": "액면가액",
    "slprc": "모집(매출)가액",
    "slta": "모집(매출)총액",
    "kndn": "종류",
    "cnt": "수량",
    "sen": "구분",
    "tast": "총자산",
    "cpt": "자본금",
    "isstk_knd": "발행주식수(주식의종류)",
    "isstk_cnt": "발행주식수(주식수)",
    "sbd": "청약기일",
    "pymd": "납입기일",
    "sband": "청약공고일",
    "asand": "배정공고일",
    "asstd": "배정기준일",
    "exstk": "신주인수권에 관한 사항(행사대상증권)",
    "exprc": "신주인수권에 관한 사항(행사가격)",
    "expd": "신주인수권에 관한 사항(행사기간)",
    "stksen": "증권의종류",
    "stkcnt": "증권수량",
    "slmthn": "모집(매출)방법",
    "actsen": "인수인구분",
    "actnmn": "인수인명",
    "udtcnt": "인수수량",
    "udtamt": "인수금액",
    "udtprc": "인수대가",
    "udtmth": "인수방법",
    "se": "구분",
    "amt": "금액",
    "hdr": "보유자",
    "rl_cmp": "회사와의관계",
    "bfsl_hdstk": "매출전보유증권수",
    "slstk": "매출증권수",
    "atsl_hdstk": "매출후보유증권수",
    "tm": "회차",
    "bdnmn": "채무증권 명칭",
    "slmth": "모집(매출)방법",
    "fta": "권면(전자등록)총액",
    "isprc": "발행가액",
    "intr": "이자율",
    "isrr": "발행수익률",
    "rpd": "상환기일",
    "print_pymint": "원리금지급대행기관",
    "mngt_cmp": "(사채)관리회사",
    "cdrt_int": "신용등급(신용평가기관)",
    "dpcrn": "표시통화",
    "dpcr_amt": "표시통화기준발행규모",
    "usarn": "사용지역",
    "usntn": "사용국가",
    "wnexpl_at": "원화 교환 예정 여부",
    "udtintnm": "인수기관명",
    "grt_int": "보증을 받은 경우(보증기관)",
    "grt_amt": "보증을 받은 경우(보증금액)",
    "icmg_mgknd": "담보 제공의 경우(담보의 종류)",
    "icmg_mgamt": "담보 제공의 경우(담보금액)",
    "estk_exstk": "지분증권과 연계된 경우(행사대상증권)",
    "estk_exrt": "지분증권과 연계된 경우(권리행사비율)",
    "estk_exprc": "지분증권과 연계된 경우(권리행사가격)",
    "estk_expd": "지분증권과 연계된 경우(권리행사기간)",
    "drcb_at": "파생결합사채해당여부",
    "drcb_uast": "파생결합사채(기초자산)",
    "drcb_optknd": "파생결합사채(옵션종류)",
    "drcb_mtd": "파생결합사채(만기일)",
}

def decode_euc_kr(text):
    """깨진 한글 인코딩 복원"""
    # EUC-KR로 인코딩된 문자열이 Latin-1(ISO-8859-1)로 잘못 해석된 경우
    try:
        enc_text = text.encode('latin-1').decode('euc-kr')
        return enc_text
    except:
        pass
    
    # CP949로 시도
    try:
        enc_text = text.encode('latin-1').decode('cp949')
        return enc_text
    except:
        pass
    
    return text

def duplicate_keys(columns: dict[str, str]):
    """Dict 리터럴 중복 키 감지"""

    seen = {}
    duplicates = []
    print(f"Columns: {len(columns)}")
    
    for key, value in columns.items():
        #print(key, value)
        if key in seen:
            #print(f"Duplicate key: {key}")
            duplicates.append(key)
        seen[key] = value
    
    print(f"Seen: {len(seen)}")
    print(f"Duplicates: {len(duplicates)}")
    return seen, duplicates

def save_zip_path(headers, save_path):
    save_path = None
    
    content_disposition = headers.get("Content-Disposition", "")
    # filename="..." 또는 filename*=UTF-8''... 패턴 찾기
    match = re.search(r"filename\*?=['\"]?(?:UTF-8'')?([^'\";\n]+)", content_disposition)
    if match:
        filename = match.group(1)

        # URL 인코딩된 경우
        if '%' in filename:
            filename = unquote(filename)
        
        # 깨진 인코딩 복원
        save_path = decode_euc_kr(filename)
            
        # .zip 확장자 제거하여 폴더명으로 사용
        save_path = filename.replace('.zip', '')

    return save_path

def parse_xml(content, filename):
    """Placeholder for XML parsing logic"""

    print(f"Parsing XML file: {filename}")
    # Implement actual XML parsing here
    content = content.decode('utf-8')
    #print(content)
    return {"filename": filename, "content": xmltodict.parse(content)}

def parse_unzip_xml(headers, binary_data, save_path):
    #print(headers, binary_data, save_path)
    result = {
        'files': [],
        'xml_data': [],
        'save_path': save_path
    }

    if headers.get("Content-Type", "") == "application/json":
        return result

    with zipfile.ZipFile(io.BytesIO(binary_data)) as zf:
        file_list = zf.namelist()
        print(f"압축 파일 내 {len(file_list)}개 파일:")
        
        for fname in file_list:
            enc_name = decode_euc_kr(fname)
            print(f"  - {enc_name}")
            result['files'].append(enc_name)
            
            if fname.endswith('.xml'):
                content = zf.read(fname)
                parsed = parse_xml(content, enc_name)
                result['xml_data'].append(parsed)
    return result

def save_zip(binary_data, save_path):
    # 파일 저장
    with open(save_path, 'wb') as f:
        f.write(binary_data)
        print(f"저장 완료: {save_path}")

def save_unzip(binary_data, save_path):
    # ZIP 압축 해제
    with zipfile.ZipFile(io.BytesIO(binary_data)) as zf:
        # 파일 목록 출력
        file_list = zf.namelist()
        print(f"압축 파일 내 {len(file_list)}개 파일:")
        for fname in file_list:
            print(f"  - {fname}")
        
        # 전체 압축 해제
        zf.extractall(save_path)
    
    print(f"압축 해제 완료: {save_path}")