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
        query = f"'{folder_id}' in parents and trashed=false and name contains '{date_prefix}'"
        
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
            
            if ('spreadsheet' in mime_type and 
                file_name.startswith(date_prefix) and 
                not file_name.lower().endswith('b2b') and
                not file_name.endswith('본사')):
                
                tracking_sheets.append(file)
                print(f"🎯 송장 시트 발견: {file_name}")
                print(f"   ID: {file['id']}")
                print(f"   수정일: {file['modifiedTime']}")
            elif file_name.lower().endswith('b2b'):
                print(f"⚠️ B2B 파일 제외: {file_name}")
            elif file_name.endswith('본사'):
                print(f"⚠️ 본사 파일 제외: {file_name}")
        
        return tracking_sheets
        
    except Exception as e:
        print(f"❌ 송장 시트 검색 실패: {e}")
        return []


def download_tracking_data(sheets_service, spreadsheet_id):
    """송장 시트에서 데이터 다운로드"""
    
    try:
        # 스프레드시트 정보 조회
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
        
        # 시트 데이터 조회 (A:Z 범위)
        range_name = f"{target_sheet}!A:Z"
        
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
        print(f"❌ 송장 데이터 다운로드 실패: {e}")
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


def get_carrier_info_from_tracking(tracking_number):
    """송장번호 패턴으로 택배사 추정"""
    
    # 일반적인 송장번호 패턴 (앞자리 기준)
    carrier_patterns = {
        "1": ("HANJIN", "한진택배"),      # 1로 시작
        "2": ("HANJIN", "한진택배"),      # 2로 시작  
        "3": ("CJGLS", "CJ대한통운"),     # 3으로 시작
        "4": ("LOTTE", "롯데택배"),       # 4로 시작
        "5": ("CJGLS", "CJ대한통운"),     # 5로 시작
        "6": ("LOGEN", "로젠택배"),       # 6으로 시작
    }
    
    if tracking_number and len(tracking_number) > 0:
        first_digit = tracking_number[0]
        if first_digit in carrier_patterns:
            return carrier_patterns[first_digit]
    
    # 기본값: 한진택배
    return ("HANJIN", "한진택배")


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
        
        # 4단계: 각 주문의 송장번호 업데이트
        print(f"\n4️⃣ 송장번호 업데이트 시작...")
        
        for idx, row in df.iterrows():
            order_number = str(row['주문번호']).strip()
            tracking_number = str(row['송장번호']).strip()
            
            # 빈 데이터 건너뛰기
            if not order_number or not tracking_number or tracking_number.lower() in ['nan', 'none', '']:
                continue
            
            # 주문번호에서 사이트 구분 (S = 미니학습지, D = 독독독)
            if order_number.startswith('S'):
                site = "mini"
                # S 접두사 제거
                order_without_prefix = order_number[1:]
                # 하이픈이 있는 경우 첫 번째 부분만 사용 (예: 12234-1 → 12234)
                if '-' in order_without_prefix:
                    clean_order_id = order_without_prefix.split('-')[0]
                    print(f"   📝 추가 발송 감지: {order_number} → 기본 주문 {clean_order_id}")
                else:
                    clean_order_id = order_without_prefix
                    
            elif order_number.startswith('D'):
                site = "dok"
                # D 접두사 제거
                order_without_prefix = order_number[1:]
                # 하이픈이 있는 경우 첫 번째 부분만 사용 (예: 12234-1 → 12234)
                if '-' in order_without_prefix:
                    clean_order_id = order_without_prefix.split('-')[0]
                    print(f"   📝 추가 발송 감지: {order_number} → 기본 주문 {clean_order_id}")
                else:
                    clean_order_id = order_without_prefix
                    
            else:
                # 접두사 없는 경우는 건너뛰기
                print(f"⚠️ 접두사 없는 주문번호 건너뛰기: {order_number}")
                continue
            
            print(f"\n📦 주문 처리: {order_number} → {clean_order_id} ({site})")
            print(f"   송장번호: {tracking_number}")
            
            # 배송 유형 결정 (국내/해외)
            is_international = determine_shipping_type(row)
            shipping_type = "해외" if is_international else "국내"
            
            # 택배사 정보 결정 (해외=EMS, 국내=대한통운)
            carrier_code, carrier_name = get_carrier_info_from_tracking(tracking_number, is_international)
            
            print(f"   배송 유형: {shipping_type}")
            print(f"   택배사: {carrier_name} ({carrier_code})")
            
            # WooCommerce 업데이트
            success = update_woocommerce_tracking(
                order_id=clean_order_id,
                tracking_number=tracking_number,
                carrier_code=carrier_code,
                carrier_name=carrier_name,
                site=site
            )
            
            if success:
                total_updated += 1
                print(f"   ✅ 업데이트 성공")
            else:
                total_failed += 1
                print(f"   ❌ 업데이트 실패")
    
    print(f"\n🎉 송장번호 업데이트 완료!")
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

📈 성공률: {(total_updated/(total_updated+total_failed)*100):.1f}%" if (total_updated + total_failed) > 0 else "📈 성공률: 0%"""

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


def update_order_status(order_url, update_data, base_url, consumer_key, consumer_secret):
    """주문 상태만 업데이트하는 헬퍼 함수"""
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
        
        return response.status_code == 200
        
    except Exception as e:
        print(f"   ❌ 상태 업데이트 오류: {e}")
        return False

def update_woocommerce_tracking(order_id, tracking_number, carrier_code="CJGLS", carrier_name="대한통운", site="mini"):
    """WooCommerce 주문에 송장번호 업데이트 (UPSERT 방식으로 추가 발송 지원)"""
    
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
    
    # 1단계: 기존 주문 정보 조회 (기존 송장번호 확인)
    # 기본값으로 새 송장번호 설정
    final_tracking = tracking_number
    
    try:
        if base_url.startswith('https://'):
            params = {
                'consumer_key': consumer_key,
                'consumer_secret': consumer_secret
            }
            get_response = requests.get(order_url, params=params, timeout=15)
        else:
            auth = (consumer_key, consumer_secret)
            get_response = requests.get(order_url, auth=auth, timeout=15)
        
        existing_tracking = ""
        if get_response.status_code == 200:
            order_data = get_response.json()
            # 기존 송장번호 확인
            for meta in order_data.get('meta_data', []):
                if meta.get('key') == '_msex_sheet_no':
                    existing_tracking = meta.get('value', '')
                    break
            
            if existing_tracking:
                print(f"   📋 기존 송장번호 발견: {existing_tracking}")
                if existing_tracking != tracking_number:
                    print(f"   🔄 송장번호 교체: {existing_tracking} → {tracking_number}")
                    # 1단계: 주문상태를 "completed"으로 변경
                    print(f"   📝 1단계: 주문상태를 completed으로 변경")
                    temp_update_data = {"status": "completed"}
                    temp_success = update_order_status(order_url, temp_update_data, base_url, consumer_key, consumer_secret)
                    if not temp_success:
                        print(f"   ⚠️ 주문상태 변경 실패 (completed)")
                else:
                    print(f"   ⚠️ 동일한 송장번호: {tracking_number}")
            else:
                print(f"   📝 새 송장번호 등록: {tracking_number}")
        else:
            print(f"   ⚠️ 기존 주문 조회 실패: {get_response.status_code}")
            
    except Exception as e:
        print(f"   ⚠️ 기존 주문 조회 오류: {e}")
    
    # 2단계: 송장번호 업데이트
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
            "value": final_tracking  # 새 송장번호로 대체
        },
        {
            "key": "_msex_register_date",
            "value": register_date
        }
    ]
    
    # 2단계: 송장번호 + 상태를 shipping으로 업데이트
    print(f"   📝 2단계: 송장번호 등록 + 주문상태를 shipping으로 변경")
    
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


def get_carrier_info_from_tracking(tracking_number, is_international=False):
    """송장번호 패턴과 배송 유형으로 택배사 결정"""
    
    # 해외 배송은 무조건 EMS
    if is_international:
        return ("EMS", "EMS")
    
    # 국내 배송은 기본적으로 대한통운
    return ("CJGLS", "대한통운")


def determine_shipping_type(row):
    """배송 유형 결정 (국내/해외)"""
    
    address = str(row.get('배송지주소', '')).strip()
    country_code = str(row.get('국가코드', '')).strip()
    
    # 국가코드가 있고 KR이 아니면 해외
    if country_code and country_code.upper() != 'KR':
        return True  # 해외
    
    # 주소에 한글이 없으면 해외
    import re
    if address and not re.search(r'[가-힣]', address):
        return True  # 해외
    
    return False  # 국내


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
    
    print("\n사용법:")
    print("  python3 tracking_updater.py          # 오늘 날짜 자동")
    print("  python3 tracking_updater.py 250915   # 특정 날짜 지정")
