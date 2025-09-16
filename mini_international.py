# -*- coding: utf-8 -*-
"""
미니학습지 국외(EMS) 주문서 작성 모듈
"""
import pandas as pd
from datetime import datetime
import requests
import re
import time
import os
from common_utils import DOWNLOAD_DIR, is_korean_address, apply_string_format, is_pure_digital_product

def normalize_address_with_google_maps(address, api_key):
    """Google Maps API를 사용한 주소 정규화 및 국가코드 추출"""
    if not api_key or not address:
        return address, None
    
    cleaned_address = ' '.join(address.split())
    
    try:
        geocoding_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': cleaned_address,
            'key': api_key,
            'language': 'en'
        }
        
        response = requests.get(geocoding_url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if data['status'] == 'OK' and data['results']:
                formatted_address = data['results'][0]['formatted_address']
                components = data['results'][0].get('address_components', [])
                
                country_code = None
                for comp in components:
                    if 'country' in comp['types']:
                        country_code = comp['short_name']
                        break
                
                if country_code == 'KR':
                    return cleaned_address, country_code
                else:
                    reconstructed_address = reconstruct_address_from_components(components, cleaned_address)
                    print(f"🌍 주소 정규화: {cleaned_address}")
                    print(f"🌍 재구성 결과: {reconstructed_address}")
                    print(f"🌍 국가코드 추출: {country_code}")
                    return reconstructed_address, country_code
            
            else:
                print(f"⚠️ Google Maps에서 주소를 찾을 수 없음: {cleaned_address}")
                return cleaned_address, None
        
        else:
            print(f"⚠️ Google Maps API 호출 실패: {response.status_code}")
            return cleaned_address, None
    
    except Exception as e:
        print(f"⚠️ 주소 정규화 오류: {e}")
        return cleaned_address, None

def reconstruct_address_from_components(components, original_address):
    """Google Maps 구성요소를 사용해서 더 정확한 주소 재구성"""
    
    # 주소 구성요소 추출
    street_number = ""
    route = ""
    subpremise = ""
    locality = ""
    postal_code = ""
    country = ""
    
    for comp in components:
        types = comp['types']
        long_name = comp['long_name']
        
        if 'street_number' in types:
            street_number = long_name
        elif 'route' in types:
            route = long_name
        elif 'subpremise' in types:
            subpremise = long_name
        elif 'locality' in types:
            locality = long_name
        elif 'postal_code' in types:
            postal_code = long_name
        elif 'country' in types:
            country = long_name
    
    # 원본 주소에서 호수 번호 추출
    apt_match = re.search(r'\b(\d{3,})\b(?=\s+\w+\s+\w+$)', original_address)
    if apt_match and not subpremise:
        subpremise = apt_match.group(1)
    
    # 주소 재구성 - 국제 배송 표준 형식
    address_parts = []
    
    # 1. 거리 주소 (거리명 + 번호)
    if route and street_number:
        street_address = f"{route} {street_number}"
        address_parts.append(street_address)
    elif route:
        street_address = route
        if street_number:
            street_address += f" {street_number}"
        address_parts.append(street_address)
    
    # 2. 호수/방 번호 (별도 라인)
    if subpremise:
        address_parts.append(f"Room {subpremise}")
    
    # 3. 우편번호 + 도시 (국제 표준)
    if postal_code and locality:
        address_parts.append(f"{postal_code} {locality}")
    elif locality:
        address_parts.append(locality)
    
    # 4. 국가 (대문자)
    if country:
        address_parts.append(country.upper())
    
    if not address_parts:
        return original_address
    
    return ", ".join(address_parts)

def process_overseas_addresses(df, api_key):
    """해외 배송 주소들을 Google Maps로 정규화 및 국가코드 추출"""
    if not api_key:
        print("⚠️ Google Maps API 키가 설정되지 않아 주소 정규화를 건너뜁니다.")
        return df
    
    print("🌍 해외 배송 주소 정규화 시작...")
    
    overseas_mask = ~df["배송지주소"].apply(is_korean_address)
    overseas_df = df[overseas_mask].copy()
    
    if overseas_df.empty:
        print("✅ 해외 배송 주소 없음")
        return df
    
    print(f"🌍 {len(overseas_df)}개의 해외 주소 정규화 중...")
    
    for idx, row in overseas_df.iterrows():
        original_address = row["배송지주소"]
        normalized_address, country_code = normalize_address_with_google_maps(original_address, api_key)
        
        df.at[idx, "배송지주소"] = normalized_address
        
        if country_code:
            df.at[idx, "국가코드"] = country_code
        
        time.sleep(0.1)
    
    print("✅ 해외 배송 주소 정규화 완료")
    return df

def process_mini_international_orders(df):
    """미니학습지 국외(EMS) 주문서 처리"""
    if df.empty:
        print("✅ 미니학습지: 국외 주문 없음")
        return
    
    print("--- 미니학습지 국외(EMS) 주문서 작성 시작 ---")
    
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
        print("✅ 미니학습지: 유효한 해외 배송 주소 없음")
        return
    
    # B2B 상품 및 디지털 상품 제외 (해외배송은 실물만)
    overseas = overseas[
        (~overseas["SKU"].str.contains("B2B", na=False)) &
        (~overseas["SKU"].str.endswith("[디지털]", na=False))
    ].copy()
    
    if overseas.empty:
        print("✅ 미니학습지: 해외 실물 배송 주문 없음 (디지털/B2B 제외)")
        return
    
    # 데이터 처리
    overseas["쇼핑몰상품코드"] = overseas["품번코드"]
    overseas["수령인연락처1"] = overseas["수령인 연락처"]
    overseas["수령인연락처2"] = overseas["수령인 연락처"]
    overseas["송장번호"] = ""
    overseas["국가코드"] = ""
    
    # 주문번호 접두사 추가
    overseas["주문번호"] = overseas["주문번호"].apply(lambda x: "S" + str(x))
    
    # Google Maps 주소 정규화
    google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    overseas = process_overseas_addresses(overseas, google_api_key)
    
    # EMS용 표준 컬럼 선택 (배송 메모는 빈 값)
    overseas["배송 메모"] = ""  # EMS는 배송 메모 비움
    
    ems_columns = [
        "주문번호", "상품명", "품번코드", "쇼핑몰상품코드", "수량",
        "수령인명", "수령인연락처1", "수령인연락처2", "우편번호",
        "배송지주소", "배송 메모", "송장번호", "국가코드"
    ]
    overseas_ems = overseas[ems_columns].copy()
    overseas_ems = overseas_ems.sort_values(by="주문번호")
    
    # Excel 파일 저장
    today_str = datetime.today().strftime('%y%m%d')
    ems_path = f"{DOWNLOAD_DIR}/{today_str} 노이지콘텐츠주문서(EMS).xlsx"
    
    # 기존 EMS 파일과 통합
    if os.path.exists(ems_path):
        old = pd.read_excel(ems_path)
        overseas_ems = pd.concat([old, overseas_ems], ignore_index=True)
    
    overseas_ems.to_excel(ems_path, index=False)
    apply_string_format(ems_path, ["수량", "수령인연락처1", "수령인연락처2", "우편번호"])
    print("📦 미니학습지 EMS 주문서 저장 완료:", ems_path)
    
    return ems_path
