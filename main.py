# -*- coding: utf-8 -*-
"""
3PL 주문 처리 시스템 - 메인 실행 파일
"""
import os
from dotenv import load_dotenv
from common_utils import (
    get_date_range, 
    get_woocommerce_auth, 
    fetch_orders_from_wp, 
    convert_orders_to_dataframe,
    should_skip_today
)
from mini_domestic import process_mini_domestic_orders
from mini_international import process_mini_international_orders
from mini_status import process_mini_reservation_status_change, process_mini_digital_status_change
from dok_domestic import process_dok_domestic_orders
from dok_international import process_dok_international_orders
from dok_status import process_dok_reservation_status_change, process_dok_digital_status_change
from email_sender import collect_shipping_files, send_shipping_files_email
from happy_together_processor import process_single_order

# .env 파일 로드
load_dotenv()

# 환경 변수 설정
MINI_WP_BASE_URL = os.getenv('WP_BASE_URL')
MINI_WP_CONSUMER_KEY = os.getenv('WP_WOO_Consumer_KEY')
MINI_WP_CONSUMER_SECRET = os.getenv('WP_WOO_Consumer_SECRET')

DOK_WP_BASE_URL = os.getenv('DOK_WP_BASE_URL')
DOK_WP_CONSUMER_KEY = os.getenv('DOK_WP_WOO_Consumer_KEY')
DOK_WP_CONSUMER_SECRET = os.getenv('DOK_WP_WOO_Consumer_SECRET')

def process_happy_together_for_site(site_name, base_url, consumer_key, consumer_secret, start_date, end_date):
    """사이트별 해피투게더 처리 (completed 상태 주문만)"""
    
    print(f"\n🎁 {site_name} 해피투게더 처리 시작...")
    
    # 미니학습지만 해피투게더 처리 (독독독은 해당 없음)
    if site_name != "미니학습지":
        print(f"⚠️ {site_name}는 해피투게더 대상이 아닙니다")
        return
    
    # 인증 설정
    auth = get_woocommerce_auth(base_url, consumer_key, consumer_secret)
    if not auth:
        print(f"❌ {site_name} 인증 실패")
        return
    
    # completed 상태 주문만 조회
    orders = fetch_orders_from_wp(base_url, auth, start_date, end_date, status='completed')
    if not orders:
        print(f"📭 {site_name} completed 주문 없음")
        return
    
    print(f"📊 {site_name} completed 주문 {len(orders)}개 조회")
    
    # 각 주문에 대해 해피투게더 처리
    processed_count = 0
    for order in orders:
        order_id = order.get('id')
        
        # 스타터팩 상품이 있는지 확인
        has_starter_pack = False
        for item in order.get('line_items', []):
            if "스타터팩" in item.get('name', ''):
                has_starter_pack = True
                break
        
        if has_starter_pack:
            print(f"\n🎯 주문 {order_id} 해피투게더 처리 중...")
            try:
                success = process_single_order(order_id)
                if success:
                    processed_count += 1
                    print(f"✅ 주문 {order_id} 해피투게더 처리 완료")
                else:
                    print(f"⚠️ 주문 {order_id} 해피투게더 처리 실패 또는 조건 불만족")
            except Exception as e:
                print(f"❌ 주문 {order_id} 해피투게더 처리 오류: {e}")
    
    print(f"\n🎁 {site_name} 해피투게더 처리 완료: {processed_count}개 주문 처리")

def process_site_orders(site_name, base_url, consumer_key, consumer_secret, start_date, end_date):
    """사이트별 주문 처리"""
    print(f"\n=== {site_name} 처리 시작 ===")
    
    # 인증 설정
    auth = get_woocommerce_auth(base_url, consumer_key, consumer_secret)
    if not auth:
        print(f"❌ {site_name} 인증 실패")
        return
    
    # 주문 데이터 수집
    orders = fetch_orders_from_wp(base_url, auth, start_date, end_date)
    if not orders:
        print(f"❌ {site_name} 주문 데이터 없음")
        return
    
    # DataFrame 변환
    df = convert_orders_to_dataframe(orders, site_name)
    if df.empty:
        print(f"❌ {site_name} 변환된 주문 데이터 없음")
        return
    
    # 완료된 주문만 필터링
    df = df[df["주문상태"] == "완료됨"].copy()
    if df.empty:
        print(f"✅ {site_name}: 완료된 주문 없음")
        return
    
    print(f"📊 {site_name} 주문 분류 및 처리 시작...")
    
    # 사이트별 처리
    if site_name == "미니학습지":
        # 1. 미니학습지 예약 상품 상태 변경 (주문서 작성 전)
        process_mini_reservation_status_change(df)
        
        # 2. 미니학습지 국내 주문서
        process_mini_domestic_orders(df)
        
        # 3. 미니학습지 국외 주문서 (EMS 통합)
        process_mini_international_orders(df)
        
        # 4. 미니학습지 디지털 상품 상태 변경
        process_mini_digital_status_change(df)
        
    elif site_name == "독독독":
        # 5. 독독독 예약 상품 상태 변경 (주문서 작성 전)
        process_dok_reservation_status_change(df)
        
        # 6. 독독독 국내 주문서
        process_dok_domestic_orders(df)
        
        # 7. 독독독 국외 주문서 (EMS 통합)
        process_dok_international_orders(df)
        
        # 8. 독독독 디지털 상품 상태 변경
        process_dok_digital_status_change(df)
    
    print(f"✅ {site_name} 처리 완료")

def main():
    """메인 실행 함수"""
    print("=== 3PL 주문 처리 시스템 시작 ===")
    print("📋 처리 순서: 1) 해피투게더 → 2) 국내/국외 주문서 → 3) 상태변경 CSV")
    
    # 공휴일/주말 체크
    if should_skip_today():
        print("\n🚫 오늘은 작업을 건너뜁니다.")
        return
    
    # 날짜 범위 계산
    start_date, end_date = get_date_range()
    
    # 환경변수 확인
    print("\n🔍 환경변수 확인...")
    print(f"미니학습지 URL: {MINI_WP_BASE_URL}")
    print(f"미니학습지 Key: {MINI_WP_CONSUMER_KEY[:10] if MINI_WP_CONSUMER_KEY else 'None'}...")
    print(f"독독독 URL: {DOK_WP_BASE_URL}")
    print(f"독독독 Key: {DOK_WP_CONSUMER_KEY[:10] if DOK_WP_CONSUMER_KEY else 'None'}...")
    
    # 🎁 1단계: 해피투게더 처리 (가장 먼저!)
    print("\n" + "="*60)
    print("🎁 1단계: 해피투게더 처리 (completed 주문만)")
    print("="*60)
    
    if MINI_WP_BASE_URL and MINI_WP_CONSUMER_KEY and MINI_WP_CONSUMER_SECRET:
        process_happy_together_for_site("미니학습지", MINI_WP_BASE_URL, MINI_WP_CONSUMER_KEY, MINI_WP_CONSUMER_SECRET, start_date, end_date)
    else:
        print("❌ 미니학습지 환경변수 설정이 불완전합니다 - 해피투게더 건너뜀")
    
    # 📦 2단계: 일반 주문 처리
    print("\n" + "="*60)
    print("📦 2단계: 일반 주문 처리 (국내/국외/상태변경)")
    print("="*60)
    
    # 독독독 사이트 처리
    if DOK_WP_BASE_URL and DOK_WP_CONSUMER_KEY and DOK_WP_CONSUMER_SECRET:
        process_site_orders("독독독", DOK_WP_BASE_URL, DOK_WP_CONSUMER_KEY, DOK_WP_CONSUMER_SECRET, start_date, end_date)
    else:
        print("❌ 독독독 사이트 환경변수 설정이 불완전합니다")
    
    # 미니학습지 사이트 처리
    if MINI_WP_BASE_URL and MINI_WP_CONSUMER_KEY and MINI_WP_CONSUMER_SECRET:
        process_site_orders("미니학습지", MINI_WP_BASE_URL, MINI_WP_CONSUMER_KEY, MINI_WP_CONSUMER_SECRET, start_date, end_date)
    else:
        print("❌ 미니학습지 사이트 환경변수 설정이 불완전합니다")
    
    print("\n=== 3PL 주문 처리 시스템 완료 ===")
    
    # 배송 주문서 이메일 발송
    print("\n📧 배송 주문서 이메일 발송 시작...")
    shipping_files = collect_shipping_files()
    
    if shipping_files:
        recipient_email = os.getenv('EMAIL_RECIPIENT')
        if not recipient_email:
            print("❌ EMAIL_RECIPIENT 환경변수가 설정되지 않았습니다.")
            print("❌ 이메일 발송 실패")
        else:
            success = send_shipping_files_email(shipping_files, recipient_email)
            if success:
                print("✅ 배송 주문서 이메일 발송 완료!")
            else:
                print("❌ 이메일 발송 실패")
    else:
        print("⚠️ 발송할 배송 주문서가 없습니다.")
    
    print("\n=== 전체 처리 완료 ===")

if __name__ == "__main__":
    main()
