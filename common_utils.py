# -*- coding: utf-8 -*-
"""
공통 유틸리티 함수들
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import re
import time
from dotenv import load_dotenv
from pathlib import Path

# .env 파일 로드
load_dotenv()

# 기본 설정 - GitHub Actions에서는 임시 디렉토리 사용
if os.getenv('GITHUB_ACTIONS'):
    # GitHub Actions 환경에서는 임시 디렉토리 사용 (업로드하지 않음)
    DOWNLOAD_DIR = "/tmp/3pl_temp"
    print("🔒 GitHub Actions 환경: 임시 디렉토리 사용 (보안)")
else:
    # 로컬 환경에서는 기존 경로 사용
    DOWNLOAD_DIR = os.path.join(Path.home(), "Documents", "3pl", "daily")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_korean_holidays(year):
    """Nager.Date API를 통해 한국 공휴일 정보 가져오기"""
    try:
        import requests
        url = f'https://date.nager.at/api/v3/PublicHolidays/{year}/KR'
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            holidays = response.json()
            holiday_dates = {holiday['date'] for holiday in holidays}
            print(f"📅 {year}년 한국 공휴일 {len(holiday_dates)}개 로드됨")
            return holiday_dates
        else:
            print(f"⚠️ 공휴일 API 오류 (상태: {response.status_code}), 하드코딩된 공휴일 사용")
            return get_fallback_holidays(year)
            
    except Exception as e:
        print(f"⚠️ 공휴일 API 연결 실패: {e}, 하드코딩된 공휴일 사용")
        return get_fallback_holidays(year)

def get_fallback_holidays(year):
    """API 실패 시 사용할 하드코딩된 공휴일 (2025년 기준)"""
    if year == 2025:
        return {
            "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30",
            "2025-03-01", "2025-05-01", "2025-05-05", "2025-05-06",
            "2025-06-03", "2025-06-06", "2025-08-15",
            "2025-10-01", "2025-10-02", "2025-10-03",
            "2025-10-06", "2025-10-07", "2025-10-08", "2025-10-09", "2025-12-25"
        }
    else:
        # 다른 연도는 기본 공휴일만 포함
        return {
            f"{year}-01-01",  # 신정
            f"{year}-03-01",  # 삼일절
            f"{year}-05-05",  # 어린이날
            f"{year}-06-06",  # 현충일
            f"{year}-08-15",  # 광복절
            f"{year}-10-03",  # 개천절
            f"{year}-10-09",  # 한글날
            f"{year}-12-25"   # 크리스마스
        }

def is_holiday(date):
    """공휴일 확인 (API 기반)"""
    year = date.year
    holidays = get_korean_holidays(year)
    return date.strftime("%Y-%m-%d") in holidays

def get_last_business_day(date):
    """마지막 영업일 계산"""
    while date.weekday() >= 5 or is_holiday(date):
        date -= timedelta(days=1)
    return date

def should_skip_today():
    """오늘 작업을 건너뛸지 확인 (공휴일/주말 체크)"""
    # KST(한국 표준시) = UTC+9
    kst_offset = timezone(timedelta(hours=9))
    today_kst = datetime.now(kst_offset).date()
    
    # 요일 확인 (0=월, 6=일)
    weekday = today_kst.weekday()
    weekday_names = ['월', '화', '수', '목', '금', '토', '일']
    is_weekend = weekday >= 5  # 토(5), 일(6)
    
    # 공휴일 확인
    is_holiday_today = is_holiday(today_kst)
    
    print(f"📅 오늘 날짜: {today_kst} ({weekday_names[weekday]}요일)")
    
    if is_holiday_today:
        print(f"🎉 오늘은 공휴일입니다.")
    
    if is_weekend:
        print(f"🏖️ 오늘은 주말입니다.")
    
    should_skip = is_holiday_today or is_weekend
    
    if should_skip:
        skip_reason = []
        if is_holiday_today:
            skip_reason.append("공휴일")
        if is_weekend:
            skip_reason.append("주말")
        
        print(f"⏭️ 작업 건너뛰기: {', '.join(skip_reason)}")
        return True
    else:
        print(f"✅ 작업 진행 가능: 평일")
        return False

def get_date_range():
    """주문 조회 날짜 범위 계산 (월요일 특별 처리)"""
    # KST(한국 표준시) = UTC+9
    kst_offset = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst_offset)
    today_kst = now_kst.date()
    
    # 오늘이 월요일인지 확인 (weekday(): 월=0, 화=1, ..., 일=6)
    is_monday = today_kst.weekday() == 0
    
    if is_monday:
        # 월요일: 지난주 금요일 12시부터 당일 월요일 12시까지
        # 금요일은 3일 전 (월요일 기준)
        last_friday = today_kst - timedelta(days=3)
        
        start_date = datetime.combine(last_friday, datetime.min.time().replace(hour=12))
        start_date = start_date.replace(tzinfo=kst_offset)
        
        end_date = datetime.combine(today_kst, datetime.min.time().replace(hour=12))
        end_date = end_date.replace(tzinfo=kst_offset)
        
        print(f"📅 월요일 특별 처리: 지난주 금요일 ~ 당일 월요일")
        print(f"📅 주문 조회 시간 (KST): {start_date.strftime('%Y-%m-%d %H:%M %z')} ~ {end_date.strftime('%Y-%m-%d %H:%M %z')}")
        print(f"📅 처리 기간: {(end_date - start_date).days}일 + {(end_date - start_date).seconds // 3600}시간")
        
    else:
        # 화~금요일: 전날 12시부터 당일 12시까지 (기존 로직)
        start_date = datetime.combine(today_kst - timedelta(days=1), datetime.min.time().replace(hour=12))
        start_date = start_date.replace(tzinfo=kst_offset)
        
        end_date = datetime.combine(today_kst, datetime.min.time().replace(hour=12))
        end_date = end_date.replace(tzinfo=kst_offset)
        
        weekday_name = ['월', '화', '수', '목', '금', '토', '일'][today_kst.weekday()]
        print(f"📅 {weekday_name}요일 일반 처리: 전날 ~ 당일")
        print(f"📅 주문 조회 시간 (KST): {start_date.strftime('%Y-%m-%d %H:%M %z')} ~ {end_date.strftime('%Y-%m-%d %H:%M %z')}")
        print(f"📅 처리 기간: 24시간")
    
    return start_date, end_date

def get_woocommerce_auth(base_url, consumer_key, consumer_secret):
    """WooCommerce Consumer Key/Secret 인증 설정 및 테스트"""
    print(f"🔍 WooCommerce Consumer Key/Secret 인증 준비: {base_url}")
    if consumer_key and consumer_secret:
        print(f"✅ WooCommerce 인증 정보 준비 완료: {base_url}")
        print(f"🔍 Consumer Key: {consumer_key[:10]}...")
        
        # 간단한 연결 테스트
        test_url = f"{base_url}/wp-json/wc/v3/system_status"
        
        if base_url.startswith('https://'):
            test_params = {
                'consumer_key': consumer_key,
                'consumer_secret': consumer_secret
            }
            test_auth = None
        else:
            test_params = {}
            test_auth = (consumer_key, consumer_secret)
        
        try:
            test_response = requests.get(test_url, auth=test_auth, params=test_params, timeout=10)
            print(f"🔍 연결 테스트 응답: {test_response.status_code}")
            if test_response.status_code == 200:
                print(f"✅ WooCommerce API 연결 성공: {base_url}")
            else:
                print(f"❌ WooCommerce API 연결 실패: {test_response.status_code}")
        except Exception as e:
            print(f"❌ WooCommerce API 테스트 오류: {e}")
        
        return (consumer_key, consumer_secret)
    else:
        print(f"❌ WooCommerce Consumer Key 또는 Secret이 없습니다: {base_url}")
        return None

def fetch_orders_from_wp(base_url, auth_info, start_date, end_date, status='completed'):
    """WooCommerce REST API를 통한 주문 데이터 수집"""
    orders_url = f"{base_url}/wp-json/wc/v3/orders"
    
    print(f"🔍 WooCommerce API 호출: {base_url}")
    
    consumer_key, consumer_secret = auth_info
    
    # HTTPS 사이트의 경우 URL 파라미터로 인증 정보 전달
    if base_url.startswith('https://'):
        print(f"🔍 HTTPS 사이트 - URL 파라미터 방식 사용")
        auth = None
        headers = {'Content-Type': 'application/json'}
        
        # KST 시간을 ISO 형식으로 변환 (UTC 오프셋 포함)
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        
        print(f"🔍 조회 기간 (ISO): {start_iso} ~ {end_iso}")
        
        # 날짜 범위 파라미터 + 인증 파라미터
        params = {
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret,
            'after': start_iso,
            'before': end_iso,
            'status': status,
            'per_page': 100,
            'page': 1
        }
    else:
        print(f"🔍 HTTP 사이트 - Basic Auth 방식 사용")
        auth = auth_info
        headers = {'Content-Type': 'application/json'}
        
        # KST 시간을 ISO 형식으로 변환
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        
        # 날짜 범위 파라미터만
        params = {
            'after': start_iso,
            'before': end_iso,
            'status': status,
            'per_page': 100,
            'page': 1
        }
    
    all_orders = []
    
    try:
        while True:
            response = requests.get(orders_url, auth=auth, headers=headers, params=params)
            
            print(f"🔍 API 응답 상태: {response.status_code}")
            if response.status_code != 200:
                print(f"❌ 주문 데이터 수집 실패: {response.status_code}")
                print(f"❌ 응답 내용: {response.text[:500]}")
                break
                
            orders = response.json()
            if not orders:
                break
                
            all_orders.extend(orders)
            params['page'] += 1
            
            # 페이지네이션 확인
            if len(orders) < params['per_page']:
                break
                
        print(f"✅ 주문 데이터 수집 완료: {len(all_orders)}개")
        return all_orders
        
    except Exception as e:
        print(f"❌ 주문 데이터 수집 오류: {e}")
        return []

def convert_orders_to_dataframe(orders, site_label):
    """주문 데이터를 DataFrame으로 변환 (WooCommerce 데이터 직접 사용)"""
    order_items = []
    
    for order in orders:
        order_id = order.get('id', '')
        order_status = order.get('status', '')
        
        # 배송 정보
        shipping = order.get('shipping', {})
        billing = order.get('billing', {})
        
        # 고객 정보
        customer_note = order.get('customer_note', '')
        
        # 주문 상품들
        line_items = order.get('line_items', [])
        
        for item in line_items:
            # SKU에서 하이픈 이하 정보 제거 (관리자용 정보 삭제)
            raw_sku = item.get('sku', '')
            clean_sku = raw_sku.split('-')[0] if raw_sku else ''  # 하이픈 이하 삭제
            
            order_items.append({
                '주문번호': str(order_id),
                '주문상태': '완료됨' if order_status == 'completed' else order_status,
                'SKU': clean_sku,
                '상품명': item.get('name', ''),
                '품번코드': clean_sku,
                '수량': str(item.get('quantity', 1)),
                '수령인명': (shipping.get('first_name', '') + ' ' + shipping.get('last_name', '')).strip(),
                '수령인 연락처': billing.get('phone', ''),
                '우편번호': shipping.get('postcode', ''),
                '배송지주소': f"{shipping.get('address_1', '')} {shipping.get('address_2', '')} {shipping.get('city', '')} {shipping.get('state', '')} {shipping.get('country', '')}".strip(),
                '배송 메모': customer_note
            })
    
    df = pd.DataFrame(order_items)
    print(f"✅ {site_label} 주문 데이터 변환 완료: {len(df)}개 항목 (SKU 매핑 불필요)")
    return df

def is_korean_address(addr):
    """한국어 주소 확인 (한글 포함 또는 KR 국가코드)"""
    if pd.isna(addr):
        return False
    
    addr_str = str(addr)
    
    # 1. 한글이 포함된 경우
    if re.search(r'[가-힣]', addr_str):
        return True
    
    # 2. 국가코드가 KR인 경우
    if re.search(r'\bKR\b', addr_str, re.IGNORECASE):
        return True
    
    # 3. 한국 관련 키워드가 포함된 경우
    korean_keywords = ['KOREA', 'SOUTH KOREA', '대한민국', '한국']
    for keyword in korean_keywords:
        if keyword in addr_str.upper():
            return True
    
    return False

def is_pure_digital_product(sku):
    """순수 디지털 상품인지 판별 (실물 패키지 + 디지털 보너스와 구분)"""
    if pd.isna(sku):
        return False
    
    sku_str = str(sku)
    
    # SKU가 [디지털]로 끝나는지 확인
    if not sku_str.endswith('[디지털]'):
        return False
    
    # SKU 구성요소 분석
    if '/' in sku_str:
        # 복합 상품인 경우 - 실물 구성요소가 많으면 실물 패키지로 판단
        components = sku_str.split('/')
        physical_components = [c for c in components if not c.endswith('[디지털]')]
        
        # 실물 구성요소가 3개 이상이면 실물 패키지로 판단
        if len(physical_components) >= 3:
            print(f"🎯 실물 패키지 + 디지털 보너스로 판단: {sku_str[:50]}...")
            return False
    
    # 단일 상품이거나 실물 구성요소가 적으면 순수 디지털
    return True

def clean_korean_address(addr):
    """한국 주소에서 불필요한 'KR' 제거"""
    if pd.isna(addr):
        return addr
    
    # 'KR', 'KOREA', 'South Korea' 등 제거
    cleaned = str(addr)
    patterns_to_remove = [
        r'\bKR\b',
        r'\bKOREA\b', 
        r'\bSOUTH KOREA\b',
        r'\b대한민국\b',
        r'\b한국\b'
    ]
    
    for pattern in patterns_to_remove:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # 연속된 공백과 콤마 정리
    cleaned = re.sub(r'\s*,\s*,\s*', ', ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip(' ,')
    
    return cleaned

def apply_string_format(filepath, columns):
    """Excel 파일의 특정 컬럼을 문자열 형식으로 변경"""
    try:
        from openpyxl import load_workbook
    except ImportError:
        print("❌ openpyxl 패키지가 설치되지 않았습니다. pip install openpyxl을 실행해주세요.")
        return
    
    try:
        wb = load_workbook(filepath)
        ws = wb.active
        col_idx = {cell.value: idx + 1 for idx, cell in enumerate(ws[1])}
        
        for col in columns:
            if col in col_idx:
                for row in ws.iter_rows(min_row=2, min_col=col_idx[col], max_col=col_idx[col]):
                    for cell in row:
                        cell.number_format = '@'
        
        wb.save(filepath)
        print(f"✅ Excel 형식 적용 완료: {filepath}")
    except Exception as e:
        print(f"❌ Excel 형식 적용 실패: {e}")
