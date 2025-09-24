# -*- coding: utf-8 -*-
"""
물류창고 송장번호 업데이트 시스템
1. Google Sheets에서 오늘 날짜로 시작하는 송장 데이터 가져오기
2. WooCommerce 주문에 송장번호 업데이트 (엠샵 플러그인 연동)
"""
import os
import pandas as pd
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from common_utils import should_skip_today
from email_sender import send_processing_result_email

load_dotenv()

# Google Drive API 스코프
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]

def authenticate_google_services():
    """Google Drive 및 Sheets API 인증"""
    
    try:
        # Google Service Account 환경변수 로드
        project_id = os.getenv('GOOGLE_PROJECT_ID')
        private_key = os.getenv('GOOGLE_PRIVATE_KEY')
        client_email = os.getenv('GOOGLE_CLIENT_EMAIL')
        
        if private_key:
            # Private Key 줄바꿈 처리 (GitHub Secrets 형식 대응)
            private_key = private_key.replace('\\n', '\n').replace('\\\\n', '\n')
        
        service_account_info = {
            "type": "service_account",
            "project_id": project_id,
            "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
            "private_key": private_key,
            "client_email": client_email,
            "client_id": os.getenv('GOOGLE_CLIENT_ID'),
            "auth_uri": os.getenv('GOOGLE_AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
            "token_uri": os.getenv('GOOGLE_TOKEN_URI', 'https://oauth2.googleapis.com/token'),
            "auth_provider_x509_cert_url": os.getenv('GOOGLE_AUTH_PROVIDER_X509_CERT_URL', 'https://www.googleapis.com/oauth2/v1/certs'),
            "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL'),
            "universe_domain": "googleapis.com"
        }
        
        required_fields = ['project_id', 'private_key', 'client_email']
        missing_fields = [field for field in required_fields if not service_account_info.get(field)]
        
        if missing_fields:
            print(f"❌ Google Service Account 정보 누락: {missing_fields}")
            return None, None
    
    except Exception as e:
        print(f"❌ Google API 환경변수 처리 실패: {e}")
        return None, None
    
    try:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        
        drive_service = build('drive', 'v3', credentials=credentials)
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        print("✅ Google API 인증 성공")
        return drive_service, sheets_service
        
    except Exception as e:
        print(f"❌ Google API 인증 실패: {e}")
        return None, None


def find_today_tracking_sheets(drive_service, folder_id, shared_drive_id, date_prefix=None):
    """오늘 날짜로 시작하는 송장 시트 찾기"""
    
    if not date_prefix:
        # 오늘 날짜를 YYMMDD 형식으로 생성
        today = datetime.now()
        date_prefix = today.strftime('%y%m%d')  # 250915 형태
    
    print(f"🔍 {date_prefix}로 시작하는 송장 시트 검색...")
    
    try:
        query = f"'{folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.spreadsheet' and name contains '{date_prefix}'"
        
        results = drive_service.files().list(
            q=query,
            corpora="drive",
            driveId=shared_drive_id,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=50,
            fields="files(id, name, mimeType, modifiedTime)"
        ).execute()
        
        files = results.get('files', [])
        
        print(f"📁 {date_prefix} 관련 파일 수: {len(files)}개")
        
        # Google Sheets 파일만 필터링 (b2b 파일 제외)
        tracking_sheets = []
        for file in files:
            file_name = file['name']
            mime_type = file['mimeType']
            
            # B2B/본사 파일 필터링 개선
            import re
            file_name_lower = file_name.lower()
            is_b2b = re.search(r'(?:^|[ _-])b2b(?:$|[ ._-])', file_name_lower)
            is_office = '본사' in file_name
            
            if ('spreadsheet' in mime_type and 
                file_name.startswith(date_prefix) and 
                not is_b2b and not is_office):
                
                tracking_sheets.append(file)
                print(f"🎯 송장 시트 발견: {file_name}")
                print(f"   ID: {file['id']}")
                print(f"   수정일: {file['modifiedTime']}")
            elif is_b2b:
                print(f"⚠️ B2B 파일 제외: {file_name}")
            elif is_office:
                print(f"⚠️ 본사 파일 제외: {file_name}")
        
        return tracking_sheets
        
    except Exception as e:
        print(f"❌ 송장 시트 검색 실패: {e}")
        return []


def download_tracking_data(sheets_service, spreadsheet_id, max_retries=3):
    """송장 시트에서 데이터 다운로드 (재시도 로직 포함)"""
    
    import time
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt  # 지수적 백오프: 2초, 4초, 8초
                print(f"🔄 재시도 {attempt + 1}/{max_retries} (대기: {wait_time}초)")
                time.sleep(wait_time)
            
            # 스프레드시트 정보 조회 (타임아웃 설정)
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            sheet_title = spreadsheet['properties']['title']
            print(f"📊 송장 시트: {sheet_title}")
            
            # 첫 번째 시트 데이터 조회
            sheets = spreadsheet.get('sheets', [])
            if not sheets:
                print("❌ 시트가 없습니다")
                return None
            
            target_sheet = sheets[0]['properties']['title']
            print(f"🎯 데이터 조회 시트: {target_sheet}")
            
            # 시트 데이터 조회 (A:Z 범위, 작은 범위로 제한)
            range_name = f"{target_sheet}!A1:Z1000"  # 1000행으로 제한
            
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values or len(values) <= 1:
                print("📭 송장 데이터가 없습니다")
                return None
            
            print(f"✅ 송장 데이터 조회 성공: {len(values)}행")
            
            # DataFrame으로 변환
            headers = values[0]
            data_rows = values[1:]
            
            # 행 길이 정규화
            normalized_rows = []
            for row in data_rows:
                while len(row) < len(headers):
                    row.append('')
                normalized_rows.append(row[:len(headers)])
            
            df = pd.DataFrame(normalized_rows, columns=headers)
            
            print(f"📊 DataFrame 생성: {df.shape[0]}행 x {df.shape[1]}열")
            print(f"📋 컬럼: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ 시도 {attempt + 1} 실패: {error_msg}")
            
            # Broken pipe, 연결 오류, 타임아웃 등은 재시도 가능
            if any(keyword in error_msg.lower() for keyword in 
                   ['broken pipe', 'connection', 'timeout', 'network', 'errno 32']):
                if attempt < max_retries - 1:
                    print("🔄 네트워크 오류 감지 - 재시도 예정")
                    continue
            
            # 권한 오류, 파일 없음 등은 재시도 불가
            if any(keyword in error_msg.lower() for keyword in 
                   ['permission', 'not found', '404', '403', '401']):
                print("❌ 복구 불가능한 오류 - 재시도 중단")
                break
    
    print(f"❌ 모든 재시도 실패: {spreadsheet_id}")
    return None


def update_woocommerce_tracking(order_id, tracking_number, carrier_code="HANJIN", carrier_name="한진택배", site="mini"):
    """WooCommerce 주문에 송장번호 업데이트 (엠샵 플러그인 연동)"""
    
    # 사이트별 환경변수
    if site == "mini":
        base_url = os.getenv('WP_BASE_URL')
        consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
        consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    elif site == "dok":
        base_url = os.getenv('DOK_WP_BASE_URL')
        consumer_key = os.getenv('DOK_WP_WOO_CONSUMER_KEY')
        consumer_secret = os.getenv('DOK_WP_WOO_CONSUMER_SECRET')
    else:
        print(f"❌ 지원하지 않는 사이트: {site}")
        return False
    
    if not all([base_url, consumer_key, consumer_secret]):
        print(f"❌ {site} 사이트 환경변수가 설정되지 않았습니다")
        return False
    
    print(f"🔄 {site} 주문 {order_id} 송장번호 업데이트...")
    
    order_url = f"{base_url}/wp-json/wc/v3/orders/{order_id}"
    
    # 현재 날짜
    register_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 엠샵 플러그인 메타 키들
    meta_data = [
        {
            "key": "_msex_dlv_code",
            "value": carrier_code
        },
        {
            "key": "_msex_dlv_name", 
            "value": carrier_name
        },
        {
            "key": "_msex_sheet_no",
            "value": tracking_number
        },
        {
            "key": "_msex_register_date",
            "value": register_date
        }
    ]
    
    update_data = {"meta_data": meta_data}
    
    try:
        if base_url.startswith('https://'):
            params = {
                'consumer_key': consumer_key,
                'consumer_secret': consumer_secret
            }
            response = requests.put(
                order_url,
                params=params,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(update_data),
                timeout=15
            )
        else:
            auth = (consumer_key, consumer_secret)
            response = requests.put(
                order_url,
                auth=auth,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(update_data),
                timeout=15
            )
        
        if response.status_code == 200:
            print(f"✅ 주문 {order_id} 송장번호 업데이트 성공!")
            print(f"   택배사: {carrier_name}")
            print(f"   송장번호: {tracking_number}")
            return True
        else:
            print(f"❌ 주문 {order_id} 업데이트 실패: {response.status_code}")
            print(f"❌ 응답: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ 주문 {order_id} 업데이트 오류: {e}")
        return False


# 중복 함수 제거됨 - 하단의 get_carrier_info_from_tracking 사용


def process_tracking_updates(date_prefix=None):
    """송장번호 업데이트 전체 프로세스"""
    
    if not date_prefix:
        today = datetime.now()
        date_prefix = today.strftime('%y%m%d')  # 250915 형태
    
    print("=== 물류창고 송장번호 업데이트 시스템 ===")
    print(f"🎯 처리 날짜: {date_prefix}")
    
    # 결과 수집 변수
    total_updated = 0
    total_failed = 0
    processed_sheets = 0
    errors = []
    
    # 공휴일/주말 체크
    if should_skip_today():
        print("\n🚫 오늘은 작업을 건너뜁니다. (물류창고 휴무)")
        return False
    
    # Google Drive 설정 (환경변수에서 가져오기)
    shared_drive_id = os.getenv('GOOGLE_SHARED_DRIVE_ID')
    folder_id = os.getenv('GOOGLE_FOLDER_ID')
    
    if not shared_drive_id or not folder_id:
        print("❌ Google Drive 환경변수가 설정되지 않았습니다.")
        print("   필요한 환경변수: GOOGLE_SHARED_DRIVE_ID, GOOGLE_FOLDER_ID")
        return False
    
    # 1단계: Google API 인증
    print("\n1️⃣ Google API 인증...")
    drive_service, sheets_service = authenticate_google_services()
    
    if not drive_service or not sheets_service:
        print("❌ Google API 인증 실패")
        return False
    
    # 2단계: 오늘 날짜 송장 시트 찾기
    print(f"\n2️⃣ {date_prefix} 송장 시트 검색...")
    tracking_sheets = find_today_tracking_sheets(drive_service, folder_id, shared_drive_id, date_prefix)
    
    if not tracking_sheets:
        print(f"📭 {date_prefix}로 시작하는 송장 시트를 찾을 수 없습니다")
        return False
    
    print(f"✅ {len(tracking_sheets)}개 송장 시트 발견")
    
    # 3단계: 각 시트 처리
    for sheet_info in tracking_sheets:
        processed_sheets += 1
        sheet_name = sheet_info['name']
        sheet_id = sheet_info['id']
        
        print(f"\n3️⃣ 송장 시트 처리: {sheet_name}")
        
        # 송장 데이터 다운로드
        df = download_tracking_data(sheets_service, sheet_id)
        
        if df is None:
            error_msg = f"{sheet_name} 데이터 다운로드 실패"
            print(f"❌ {error_msg}")
            errors.append(error_msg)
            continue
        
        # EMS 해외 시트의 경우 컬럼 매핑 필요
        if '해외' in sheet_name.lower() or 'ems' in sheet_name.lower():
            print("🌍 EMS 해외 시트 감지 - 컬럼 매핑 적용")
            
            # EMS 시트 컬럼 매핑
            column_mapping = {
                '고객주문번호': '주문번호',
                '등기번호': '송장번호',
                '수취인명': '수령인명',
                '수취인 전화번호': '수령인연락처1',
                '수취인 우편번호': '우편번호', 
                '수취인 주소': '배송지주소',
                '수취인 국가코드': '국가코드'
            }
            
            # 컬럼명 변경
            df = df.rename(columns=column_mapping)
            
            # 누락된 컬럼 추가
            required_columns_ems = [
                '주문번호', '상품명', '품번코드', '쇼핑몰상품코드', '수량',
                '수령인명', '수령인연락처1', '수령인연락처2', '우편번호',
                '배송지주소', '송장번호', '국가코드'
            ]
            
            for col in required_columns_ems:
                if col not in df.columns:
                    if col == '수령인연락처2':
                        df[col] = df.get('수령인연락처1', '')  # 연락처1과 동일
                    elif col in ['상품명', '품번코드', '쇼핑몰상품코드', '수량']:
                        df[col] = ''  # 빈 값으로 설정
                    else:
                        df[col] = ''
            
            # 컬럼 순서 정리
            df = df[required_columns_ems]
            
            print(f"✅ EMS 시트 컬럼 매핑 완료: {len(df)}개 주문")
        
        # 일반 국내 시트의 필수 컬럼 확인
        required_columns = ['주문번호', '송장번호']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ 필수 컬럼 누락: {missing_columns}")
            print(f"📋 사용 가능한 컬럼: {list(df.columns)}")
            continue
        
        print(f"✅ 송장 데이터 검증 완료: {len(df)}개 주문")
        
        # 4단계: 중복 주문 제거 및 배치 처리 준비
        print(f"\n4️⃣ 송장번호 업데이트 준비...")
        
        # 중복 제거를 위한 딕셔너리 (주문번호 → 송장번호)
        order_tracking_map = {}
        
        for idx, row in df.iterrows():
            order_number = str(row['주문번호']).strip()
            tracking_number = str(row['송장번호']).strip()
            
            # 빈 데이터 건너뛰기
            if not order_number or not tracking_number or tracking_number.lower() in ['nan', 'none', '']:
                continue
            
            # 주문번호 정제 및 사이트 구분
            clean_order_id, site = parse_order_number(order_number)
            if not clean_order_id:
                continue
            
            # 배송 유형 및 택배사 정보
            is_international = determine_shipping_type(row)
            carrier_code, carrier_name = get_carrier_info_from_tracking(tracking_number, is_international)
            
            # 중복 제거: 같은 주문번호는 마지막 송장번호 사용
            order_key = f"{site}_{clean_order_id}"
            order_tracking_map[order_key] = {
                'order_id': clean_order_id,
                'tracking_number': tracking_number,
                'carrier_code': carrier_code,
                'carrier_name': carrier_name,
                'site': site,
                'original_order': order_number,
                'shipping_type': "해외" if is_international else "국내"
            }
        
        print(f"📊 중복 제거 후 처리할 주문: {len(order_tracking_map)}개")
        
        # 5단계: 배치 처리로 업데이트
        print(f"\n5️⃣ 배치 처리 시작...")
        batch_updates = list(order_tracking_map.values())
        
        for i in range(0, len(batch_updates), 20):  # 20개씩 배치
            batch = batch_updates[i:i+20]
            print(f"📦 배치 {i//20 + 1}/{(len(batch_updates)-1)//20 + 1}: {len(batch)}개 주문 처리 중...")
            
            batch_success, batch_failed = update_woocommerce_batch(batch)
            total_updated += batch_success
            total_failed += batch_failed
            
            if batch_failed > 0:
                errors.append(f"배치 {i//20 + 1}: {batch_failed}개 실패")
    
    print(f"\n🎉 배치 처리 완료!")
    print(f"✅ 성공: {total_updated}개")
    print(f"❌ 실패: {total_failed}개")
    
    # 처리 결과 이메일 발송
    print(f"\n📧 송장 업데이트 결과 이메일 발송...")
    
    # 결과 요약 생성
    result_summary = f"""=== 송장번호 업데이트 결과 ({date_prefix}) ===

📊 처리 결과:
   📦 처리된 시트: {processed_sheets}개
   ✅ 송장 업데이트 성공: {total_updated}건
   ❌ 송장 업데이트 실패: {total_failed}건

📈 성공률: {(total_updated/(total_updated+total_failed)*100):.1f}% """ if (total_updated + total_failed) > 0 else "📈 성공률: 0%"

    if errors:
        result_summary += "\n\n❌ 발생한 오류:"
        for error in errors:
            result_summary += f"\n   • {error}"
    
    if total_failed == 0 and not errors:
        result_summary += "\n\n✅ 모든 송장번호 업데이트가 성공적으로 완료되었습니다."
    
    # 결과 이메일 발송
    email_success = send_processing_result_email(result_summary)
    if email_success:
        print("✅ 송장 업데이트 결과 이메일 발송 완료!")
    else:
        print("❌ 송장 업데이트 결과 이메일 발송 실패")
    
    return total_updated > 0


def update_woocommerce_tracking_DEPRECATED(order_id, tracking_number, carrier_code="CJGLS", carrier_name="대한통운", site="mini"):
    """DEPRECATED: 개별 처리 함수 - 배치 처리로 대체됨"""
    
    # 사이트별 환경변수
    if site == "mini":
        base_url = os.getenv('WP_BASE_URL')
        consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
        consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    elif site == "dok":
        base_url = os.getenv('DOK_WP_BASE_URL')
        consumer_key = os.getenv('DOK_WP_WOO_CONSUMER_KEY')
        consumer_secret = os.getenv('DOK_WP_WOO_CONSUMER_SECRET')
    else:
        print(f"❌ 지원하지 않는 사이트: {site}")
        return False
    
    if not all([base_url, consumer_key, consumer_secret]):
        print(f"❌ {site} 사이트 환경변수가 설정되지 않았습니다")
        return False
    
    order_url = f"{base_url}/wp-json/wc/v3/orders/{order_id}"
    
    # 송장번호 업데이트
    register_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 엠샵 플러그인 메타 키들 (UPSERT)
    meta_data = [
        {
            "key": "_msex_dlv_code",
            "value": carrier_code
        },
        {
            "key": "_msex_dlv_name", 
            "value": carrier_name
        },
        {
            "key": "_msex_sheet_no",
            "value": tracking_number
        },
        {
            "key": "_msex_register_date",
            "value": register_date
        }
    ]
    
    # 송장번호 + 상태를 shipping으로 업데이트
    print(f"   📝 송장번호: {tracking_number} → shipping 상태")
    
    update_data = {
        "meta_data": meta_data,
        "status": "shipping"  # 송장 등록 시 주문상태를 shipping으로 변경
    }
    
    try:
        if base_url.startswith('https://'):
            params = {
                'consumer_key': consumer_key,
                'consumer_secret': consumer_secret
            }
            response = requests.put(
                order_url,
                params=params,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(update_data),
                timeout=15
            )
        else:
            auth = (consumer_key, consumer_secret)
            response = requests.put(
                order_url,
                auth=auth,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(update_data),
                timeout=15
            )
        
        if response.status_code == 200:
            return True
        else:
            print(f"❌ API 오류: {response.status_code}")
            print(f"❌ 응답: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ 업데이트 오류: {e}")
        return False


def parse_order_number(order_number):
    """주문번호를 파싱하여 clean_order_id와 site 반환"""
    if order_number.startswith('S'):
        site = "mini"
        order_without_prefix = order_number[1:]
        if '-' in order_without_prefix:
            clean_order_id = order_without_prefix.split('-')[0]
        else:
            clean_order_id = order_without_prefix
        return clean_order_id, site
        
    elif order_number.startswith('D'):
        site = "dok"
        order_without_prefix = order_number[1:]
        if '-' in order_without_prefix:
            clean_order_id = order_without_prefix.split('-')[0]
        else:
            clean_order_id = order_without_prefix
        return clean_order_id, site
    else:
        print(f"⚠️ 접두사 없는 주문번호 건너뛰기: {order_number}")
        return None, None

def get_carrier_info_from_tracking(tracking_number, is_international=False):
    """송장번호 패턴과 배송 유형으로 택배사 결정"""
    
    # 해외 배송은 무조건 EMS
    if is_international:
        return ("EMS", "EMS")
    
    # 국내 배송은 기본적으로 대한통운
    return ("CJGLS", "대한통운")


def determine_shipping_type(row):
    """배송 유형 결정 (국내/해외) - 개선된 로직"""
    
    address = str(row.get('배송지주소', '')).strip()
    country_code = str(row.get('국가코드', '')).strip()
    
    # 1순위: 국가코드 우선 확인
    if country_code:
        return country_code.upper() != 'KR'
    
    # 2순위: 주소에 해외 국가명 포함 확인
    if address:
        import re
        address_upper = address.upper()
        overseas_keywords = [
            'USA', 'UNITED STATES', 'AMERICA', 'US',
            'JAPAN', 'TOKYO', 'OSAKA', 'JP', 
            'CHINA', 'BEIJING', 'SHANGHAI', 'CN',
            'SINGAPORE', 'SG', 'TAIWAN', 'TW',
            'HONG KONG', 'HK', 'VIETNAM', 'VN'
        ]
        
        for keyword in overseas_keywords:
            if keyword in address_upper:
                return True  # 해외
        
        # 3순위: 한글 포함 여부 (보조 판단)
        has_korean = bool(re.search(r'[가-힣]', address))
        has_english = bool(re.search(r'[A-Za-z]', address))
        
        # 영문만 있고 한글이 없으면 해외 가능성 높음 (하지만 확실하지 않음)
        if has_english and not has_korean and len(address) > 10:
            return True  # 해외 (낮은 확신도)
    
    return False  # 국내 (기본값)


def update_woocommerce_batch(batch_data):
    """WooCommerce Batch API를 사용한 대량 업데이트"""
    if not batch_data:
        return 0, 0
    
    # 사이트별로 분리
    mini_orders = [item for item in batch_data if item['site'] == 'mini']
    dok_orders = [item for item in batch_data if item['site'] == 'dok']
    
    total_success = 0
    total_failed = 0
    
    # 미니학습지 배치 처리
    if mini_orders:
        success, failed = process_batch_for_site(mini_orders, 'mini')
        total_success += success
        total_failed += failed
    
    # 독독독 배치 처리
    if dok_orders:
        success, failed = process_batch_for_site(dok_orders, 'dok')
        total_success += success
        total_failed += failed
    
    return total_success, total_failed

def process_batch_for_site(orders, site):
    """특정 사이트의 주문들을 배치로 처리"""
    if site == "mini":
        base_url = os.getenv('WP_BASE_URL')
        consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
        consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    elif site == "dok":
        base_url = os.getenv('DOK_WP_BASE_URL')
        consumer_key = os.getenv('DOK_WP_WOO_CONSUMER_KEY')
        consumer_secret = os.getenv('DOK_WP_WOO_CONSUMER_SECRET')
    else:
        return 0, len(orders)
    
    if not all([base_url, consumer_key, consumer_secret]):
        print(f"❌ {site} 환경변수 누락")
        return 0, len(orders)
    
    # 배치 데이터 생성
    batch_update = {
        "update": []
    }
    
    register_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for order in orders:
        meta_data = [
            {"key": "_msex_dlv_code", "value": order['carrier_code']},
            {"key": "_msex_dlv_name", "value": order['carrier_name']},
            {"key": "_msex_sheet_no", "value": order['tracking_number']},
            {"key": "_msex_register_date", "value": register_date}
        ]
        
        batch_update["update"].append({
            "id": int(order['order_id']),
            "meta_data": meta_data,
            "status": "shipping"
        })
    
    # API 호출
    try:
        batch_url = f"{base_url}/wp-json/wc/v3/orders/batch"
        
        if base_url.startswith('https://'):
            params = {'consumer_key': consumer_key, 'consumer_secret': consumer_secret}
            response = requests.post(batch_url, params=params, json=batch_update, timeout=30)
        else:
            auth = (consumer_key, consumer_secret)
            response = requests.post(batch_url, auth=auth, json=batch_update, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            update_results = result.get('update', [])
            
            # 실제 성공/실패 개수 계산
            success_count = 0
            failed_count = 0
            
            for item in update_results:
                if 'error' in item:
                    failed_count += 1
                    error_msg = item.get('error', {}).get('message', 'Unknown error')
                    print(f"   ⚠️ 주문 {item.get('id', '?')} 실패: {error_msg}")
                elif 'id' in item:
                    success_count += 1
                else:
                    failed_count += 1
                    print(f"   ⚠️ 알 수 없는 응답: {item}")
            
            print(f"   ✅ {site} 배치 완료: {success_count}개 성공, {failed_count}개 실패")
            return success_count, failed_count
        else:
            print(f"   ❌ {site} 배치 실패: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"   상세: {error_detail}")
            except:
                print(f"   응답: {response.text[:200]}")
            return 0, len(orders)
            
    except Exception as e:
        print(f"   ❌ {site} 배치 오류: {e}")
        return 0, len(orders)


if __name__ == "__main__":
    import sys
    
    # 명령행 인수로 날짜 지정 가능
    if len(sys.argv) > 1:
        date_prefix = sys.argv[1]
    else:
        # 기본값: 오늘 날짜
        date_prefix = datetime.now().strftime('%y%m%d')
    
    print(f"🚀 송장번호 업데이트 시작: {date_prefix}")
    
    success = process_tracking_updates(date_prefix)
    
    if success:
        print(f"\n🎉 {date_prefix} 송장번호 업데이트 완료!")
    else:
        print(f"\n❌ {date_prefix} 송장번호 업데이트 실패")
    
