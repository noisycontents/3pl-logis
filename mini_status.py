# -*- coding: utf-8 -*-
"""
미니학습지 주문상태 변경 모듈 (WooCommerce API 직접 변경 + CSV 파일 생성)
"""
import pandas as pd
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from common_utils import DOWNLOAD_DIR

load_dotenv()

def update_order_status_in_woocommerce(order_id, new_status):
    """WooCommerce API를 통한 주문상태 직접 변경"""
    
    # 미니학습지 환경변수
    base_url = os.getenv('WP_BASE_URL')
    consumer_key = os.getenv('WP_WOO_Consumer_KEY')
    consumer_secret = os.getenv('WP_WOO_Consumer_SECRET')
    
    if not all([base_url, consumer_key, consumer_secret]):
        print("❌ 미니학습지 WooCommerce API 환경변수가 설정되지 않았습니다")
        return False
    
    order_url = f"{base_url}/wp-json/wc/v3/orders/{order_id}"
    
    # 주문상태 업데이트 데이터
    update_data = {
        "status": new_status
    }
    
    try:
        if base_url.startswith('https://'):
            # HTTPS - URL 파라미터로 인증
            params = {
                'consumer_key': consumer_key,
                'consumer_secret': consumer_secret
            }
            response = requests.put(order_url, json=update_data, params=params, timeout=10)
        else:
            # HTTP - Basic Auth
            auth = (consumer_key, consumer_secret)
            response = requests.put(order_url, json=update_data, auth=auth, timeout=10)
        
        if response.status_code == 200:
            updated_order = response.json()
            print(f"✅ 미니학습지 주문상태 변경 성공: {order_id} -> {updated_order.get('status')}")
            return True
        else:
            print(f"❌ 미니학습지 주문상태 변경 실패: {order_id}, Status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 미니학습지 API 호출 오류: {e}")
        return False

def create_csv_for_condition(df, condition_name, order_status, filename_suffix):
    """조건별 CSV 파일 생성"""
    if df.empty:
        return
    
    print(f"--- 미니학습지 {condition_name} 상품 처리 시작 ---")
    
    csv_output = pd.DataFrame()
    csv_output["order_id"] = df["주문번호"].astype(str)
    csv_output["order_item_id"] = ""
    csv_output["div_code"] = ""
    csv_output["sheet_no"] = ""
    csv_output["order_status"] = order_status
    
    # 중복 제거
    csv_output = csv_output.drop_duplicates(subset=["order_id"], keep="first")
    
    # CSV 파일 저장
    today_str = datetime.today().strftime('%y%m%d')
    
    if filename_suffix == "배송완료":
        csv_filename = f"CJGLS_sheet_upload_{filename_suffix}_미니.csv"
    else:
        csv_filename = f"CJGLS_sheet_upload_{today_str}_{filename_suffix}_미니.csv"
    
    csv_path = f"{DOWNLOAD_DIR}/{csv_filename}"
    csv_output.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"📦 미니학습지 {condition_name} CSV 저장 완료: {csv_path}")
    
    return csv_path

def process_mini_reservation_status_change(df):
    """미니학습지 예약 상품 상태 변경 (주문서 작성 전)"""
    if df.empty:
        print("✅ 미니학습지: 예약 상품 없음")
        return []
    
    print("--- 미니학습지 예약 상품 상태 변경 시작 ---")
    
    # 예약 상품만 필터링
    reservation_mask = df["SKU"].str.contains("예약", na=False)
    reservation_df = df[reservation_mask].copy()
    
    if reservation_df.empty:
        print("✅ 미니학습지: 예약 상품 없음")
        return []
    
    # 예약 상품 상태 변경
    print("🔄 미니학습지 예약 상품 WooCommerce 상태 변경 중...")
    
    updated_count = 0
    for _, row in reservation_df.iterrows():
        order_id = row["주문번호"]
        success = update_order_status_in_woocommerce(order_id, "processing")
        if success:
            updated_count += 1
    
    print(f"✅ 미니학습지 {updated_count}개 주문 상태 변경 완료")
    
    # CSV 파일 생성
    today_str = datetime.today().strftime('%y%m%d')
    csv_path = create_csv_for_condition(reservation_df, "예약 상품 (처리중)", "processing", f"{today_str}_처리중")
    
    return [csv_path] if csv_path else []

def process_mini_digital_status_change(df):
    """미니학습지 디지털 상품 상태 변경"""
    if df.empty:
        print("✅ 미니학습지: 디지털 상품 없음")
        return []
    
    print("--- 미니학습지 디지털 상품 (배송완료) 상품 처리 시작 ---")
    
    # 디지털 상품만 필터링
    digital_mask = df["SKU"].str.endswith("[디지털]", na=False)
    digital_df = df[digital_mask].copy()
    
    if digital_df.empty:
        print("✅ 미니학습지: 디지털 상품 없음")
        return []
    
    # 디지털 상품 상태 변경
    print("🔄 미니학습지 디지털 상품 WooCommerce 상태 변경 중...")
    
    updated_count = 0
    for _, row in digital_df.iterrows():
        order_id = row["주문번호"]
        success = update_order_status_in_woocommerce(order_id, "shipped")
        if success:
            updated_count += 1
    
    print(f"✅ 미니학습지 {updated_count}개 주문 상태 변경 완료")
    
    # CSV 파일 생성
    csv_path = create_csv_for_condition(digital_df, "디지털 상품 (배송완료)", "shipped", "배송완료")
    
    return [csv_path] if csv_path else []

def process_mini_status_changes(df):
    """미니학습지 주문상태 변경 처리 (전체) - 레거시 함수"""
    if df.empty:
        print("✅ 미니학습지: 상태 변경할 주문 없음")
        return []
    
    print("--- 미니학습지 주문상태 변경 처리 시작 ---")
    
    # SKU 조건별 분류 (정확한 패턴 매칭)
    conditions = {
        "디지털": {
            "mask": df["SKU"].str.endswith("[디지털]", na=False),  # 정확히 끝에 있는 경우만
            "status": "shipped",
            "filename": "배송완료",
            "description": "디지털 상품 (배송완료)"
        },
        "예약": {
            "mask": df["SKU"].str.contains("예약", na=False),
            "status": "processing", 
            "filename": "처리중",
            "description": "예약 상품 (처리중)"
        }
    }
    
    created_files = []
    processed_orders = set()
    
    # 각 조건별 처리
    for condition_name, config in conditions.items():
        condition_mask = config["mask"]
        condition_df = df[condition_mask].copy()
        
        if not condition_df.empty:
            # 혼합 주문 제외 로직: 디지털과 실물이 섞인 주문 제외
            
            # 현재 조건의 주문번호들
            condition_orders = condition_df["주문번호"].unique()
            
            # 각 주문번호별로 혼합 여부 확인
            pure_orders = []
            for order_num in condition_orders:
                order_items = df[df["주문번호"] == order_num]
                
                # 해당 주문의 모든 SKU 확인
                has_digital = order_items["SKU"].str.endswith("[디지털]", na=False).any()
                has_physical = (~order_items["SKU"].str.endswith("[디지털]", na=False) & 
                              ~order_items["SKU"].str.contains("예약", na=False) &
                              ~order_items["SKU"].str.contains("B2B", na=False)).any()
                
                # 디지털만 있는 주문 (실물 없음) 또는 예약만 있는 주문만 처리
                if condition_name == "디지털" and has_digital and not has_physical:
                    pure_orders.append(order_num)
                elif condition_name == "예약" and not has_digital and not has_physical:
                    # 예약 상품만 있는 주문
                    has_reservation = order_items["SKU"].str.contains("예약", na=False).any()
                    if has_reservation:
                        pure_orders.append(order_num)
            
            pure_condition_df = condition_df[condition_df["주문번호"].isin(pure_orders)].copy()
            
            if not pure_orders:
                print(f"⚠️ 미니학습지 {condition_name}: 혼합 주문으로 인해 처리할 순수 주문 없음")
            
            if not pure_condition_df.empty:
                print(f"🔄 미니학습지 {condition_name} 상품 WooCommerce 상태 변경 중...")
                
                # WooCommerce에서 실제 주문 상태 변경
                successful_updates = []
                for order_num in pure_orders:
                    success = update_order_status_in_woocommerce(order_num, config["status"])
                    if success:
                        successful_updates.append(order_num)
                
                if successful_updates:
                    print(f"✅ 미니학습지 {len(successful_updates)}개 주문 상태 변경 완료")
                    
                    # 성공한 주문들만 CSV 파일 생성
                    successful_df = pure_condition_df[pure_condition_df["주문번호"].isin(successful_updates)].copy()
                    
                    # 주문번호 접두사 추가
                    successful_df["주문번호"] = successful_df["주문번호"].apply(lambda x: "S" + str(x))
                    
                    csv_path = create_csv_for_condition(
                        successful_df, 
                        config["description"], 
                        config["status"], 
                        config["filename"]
                    )
                    if csv_path:
                        created_files.append(csv_path)
                    processed_orders.update(successful_df["주문번호"].unique())
                else:
                    print(f"❌ 미니학습지 {condition_name} 상품 상태 변경 실패")
    
    return created_files
