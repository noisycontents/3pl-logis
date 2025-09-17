# -*- coding: utf-8 -*-
"""
독독독 국내 주문서 작성 모듈
"""
import pandas as pd
from datetime import datetime
from common_utils import DOWNLOAD_DIR, is_korean_address, clean_korean_address, apply_string_format

def process_dok_domestic_orders(df):
    """독독독 국내 주문서 처리"""
    if df.empty:
        print("✅ 독독독: 국내 주문 없음")
        return
    
    print("--- 독독독 국내 주문서 작성 시작 ---")
    
    # 국내 주소만 필터링
    domestic = df[df["배송지주소"].apply(is_korean_address)].copy()
    
    if domestic.empty:
        print("✅ 독독독: 국내 배송 주문 없음")
        return
    
    # B2B 상품 및 디지털 상품 제외
    domestic = domestic[
        (~domestic["SKU"].str.contains("[B2B]", na=False)) &
        (~domestic["SKU"].str.contains("[디지털]", na=False))
    ].copy()
    
    if domestic.empty:
        print("✅ 독독독: B2B 제외 후 국내 주문 없음")
        return
    
    # 데이터 처리 (쇼핑몰상품코드는 이미 원래 SKU로 설정됨)
    domestic["수령인연락처1"] = domestic["수령인 연락처"]
    domestic["수령인연락처2"] = domestic["수령인 연락처"]
    domestic["송장번호"] = ""
    domestic["국가코드"] = ""
    
    # 주문번호 접두사 추가 (독독독은 "D")
    domestic["주문번호"] = domestic["주문번호"].apply(lambda x: "D" + str(x))
    
    # 국내 주소에서 "KR" 제거
    domestic["배송지주소"] = domestic["배송지주소"].apply(clean_korean_address)
    
    # 최종 컬럼 선택
    domestic = domestic[[
        "주문번호", "상품명", "품번코드", "쇼핑몰상품코드", "수량",
        "수령인명", "수령인연락처1", "수령인연락처2", "우편번호",
        "배송지주소", "배송메세지", "송장번호", "국가코드"
    ]]
    
    domestic = domestic.sort_values(by="주문번호")
    
    # Excel 파일 저장
    today_str = datetime.today().strftime('%y%m%d')
    dom_path = f"{DOWNLOAD_DIR}/{today_str} 노이지콘텐츠주문서(독독독_국내).xlsx"
    
    domestic.to_excel(dom_path, index=False)
    apply_string_format(dom_path, ["수량", "수령인연락처1", "수령인연락처2", "우편번호"])
    print("📦 독독독 국내 주문서 저장 완료:", dom_path)
    
    return dom_path
