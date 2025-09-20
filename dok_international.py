# -*- coding: utf-8 -*-
"""
독독독 국외(EMS) 주문서 작성 모듈
"""
import pandas as pd
from datetime import datetime
import os
from common_utils import DOWNLOAD_DIR, is_korean_address, apply_string_format, is_pure_digital_product, processing_results, filter_korean_recipients
from mini_international import process_overseas_addresses

def process_dok_international_orders(df):
    """독독독 국외(EMS) 주문서 처리"""
    if df.empty:
        print("✅ 독독독: 국외 주문 없음")
        processing_results.add_international_orders(0)
        return
    
    print("--- 독독독 국외(EMS) 주문서 작성 시작 ---")
    
    # 해외 주소만 필터링 (주소가 있고 한국어가 아닌 경우)
    overseas = df[
        df["배송지주소"].notna() & 
        (df["배송지주소"].str.strip() != "") &
        (~df["배송지주소"].apply(is_korean_address))
    ].copy()
    
    # 추가 한국 주소 제외 (국가코드 KR 확인)
    if not overseas.empty:
        print(f"🔍 해외 주소 후보: {len(overseas)}개")
        
        # 각 주소를 다시 한 번 확인
        truly_overseas = []
        for idx, row in overseas.iterrows():
            addr = str(row["배송지주소"])
            
            # KR이 포함되어 있으면 한국 주소로 간주
            if not is_korean_address(addr):
                truly_overseas.append(idx)
            else:
                print(f"⚠️ 한국 주소로 재분류: {addr[:50]}...")
        
        overseas = overseas.loc[truly_overseas].copy()
        print(f"🌍 실제 해외 주소: {len(overseas)}개")
    
    if overseas.empty:
        print("✅ 독독독: 유효한 해외 배송 주소 없음")
        return
    
    # B2B 상품 및 디지털 상품 제외 (전체 SKU 문자열에서 확인)
    overseas = overseas[
        (~overseas["SKU"].str.contains("\\[B2B\\]", na=False)) &
        (~overseas["SKU"].str.contains("\\[디지털\\]", na=False))
    ].copy()
    
    if overseas.empty:
        print("✅ 독독독: 해외 실물 배송 주문 없음 (디지털/B2B 제외)")
        return
    
    # 한글 수령인명 필터링 (EMS는 영문 이름만 허용)
    print("🔍 한글 수령인명 확인 중...")
    overseas, korean_recipients = filter_korean_recipients(overseas)
    
    # 한글 수령인명 이슈 기록
    if not korean_recipients.empty:
        processing_results.add_korean_recipient_issue(korean_recipients, "독독독")
    
    if overseas.empty:
        print("✅ 독독독: 한글 수령인명 제외 후 유효한 해외 배송 주문 없음")
        processing_results.add_international_orders(0)
        return
    
    # 데이터 처리 (쇼핑몰상품코드는 이미 원래 SKU로 설정됨)
    overseas["수령인연락처1"] = overseas["수령인 연락처"]
    overseas["수령인연락처2"] = overseas["수령인 이메일"]  # EMS는 연락처2에 이메일 사용
    overseas["송장번호"] = ""
    overseas["국가코드"] = ""
    
    # 주문번호 접두사 추가 (독독독은 "D")
    overseas["주문번호"] = overseas["주문번호"].apply(lambda x: "D" + str(x))
    
    # Google Maps 주소 정규화
    google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    overseas = process_overseas_addresses(overseas, google_api_key)
    
    # EMS용 표준 컬럼 선택 (배송 메모는 빈 값)
    overseas["배송메세지"] = ""  # EMS는 배송 메모 비움
    
    ems_columns = [
        "주문번호", "상품명", "품번코드", "쇼핑몰상품코드", "수량",
        "수령인명", "수령인연락처1", "수령인연락처2", "우편번호",
        "배송지주소", "배송메세지", "송장번호", "국가코드"
    ]
    overseas_ems = overseas[ems_columns].copy()
    overseas_ems = overseas_ems.sort_values(by="주문번호")
    
    # Excel 파일 저장 (EMS 파일에 통합)
    today_str = datetime.today().strftime('%y%m%d')
    ems_path = f"{DOWNLOAD_DIR}/{today_str} 노이지콘텐츠주문서(EMS).xlsx"
    
    # 기존 EMS 파일과 통합
    if os.path.exists(ems_path):
        old = pd.read_excel(ems_path)
        overseas_ems = pd.concat([old, overseas_ems], ignore_index=True)
    
    overseas_ems.to_excel(ems_path, index=False)
    apply_string_format(ems_path, ["수량", "수령인연락처1", "수령인연락처2", "우편번호"])
    print(f"📦 독독독 EMS 주문서 저장 완료: {len(overseas_ems)}건 - {ems_path}")
    
    # 결과 수집
    processing_results.add_international_orders(len(overseas_ems))
    
    return ems_path
