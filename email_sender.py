# -*- coding: utf-8 -*-
"""
배송 주문서 이메일 발송 모듈
"""
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.header import Header
from datetime import datetime
from dotenv import load_dotenv
import urllib.parse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

# Google Drive API 스코프 (Shared Drive 접근 포함)
DRIVE_SCOPES = [
    'https://www.googleapis.com/auth/drive'
]

def authenticate_google_drive():
    """Google Drive API 인증"""
    try:
        # Google Service Account 환경변수 로드 (tracking_updater.py와 동일)
        project_id = os.getenv('GOOGLE_PROJECT_ID')
        private_key = os.getenv('GOOGLE_PRIVATE_KEY')
        client_email = os.getenv('GOOGLE_CLIENT_EMAIL')
        
        if not all([project_id, private_key, client_email]):
            print("⚠️ Google Drive 환경변수가 설정되지 않아 백업을 건너뜁니다.")
            return None
        
        # Service Account 정보 구성
        service_account_info = {
            "type": "service_account",
            "project_id": project_id,
            "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
            "private_key": private_key.replace('\\n', '\n'),
            "client_email": client_email,
            "client_id": os.getenv('GOOGLE_CLIENT_ID'),
            "auth_uri": os.getenv('GOOGLE_AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
            "token_uri": os.getenv('GOOGLE_TOKEN_URI', 'https://oauth2.googleapis.com/token'),
            "auth_provider_x509_cert_url": os.getenv('GOOGLE_AUTH_PROVIDER_X509_CERT_URL', 'https://www.googleapis.com/oauth2/v1/certs'),
            "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL'),
            "universe_domain": "googleapis.com"
        }
        
        # 인증 정보 생성
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=DRIVE_SCOPES
        )
        
        # Drive 서비스 생성
        drive_service = build('drive', 'v3', credentials=credentials)
        
        return drive_service
        
    except Exception as e:
        print(f"⚠️ Google Drive 인증 실패: {e}")
        return None

def upload_to_google_drive(file_path, folder_id):
    """파일을 Google Drive 폴더에 업로드 (Shared Drive 지원, 같은 이름 파일 있으면 덮어쓰기)"""
    drive_service = authenticate_google_drive()
    
    if not drive_service:
        return False
    
    try:
        filename = os.path.basename(file_path)
        
        # 기존 파일 검색 (Shared Drive 방식)
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        existing_files = drive_service.files().list(
            q=query, 
            fields='files(id,name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        # 미디어 업로드 설정 (Excel 파일)
        media = MediaFileUpload(
            file_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        if existing_files['files']:
            # 기존 파일이 있으면 업데이트 (덮어쓰기)
            existing_file_id = existing_files['files'][0]['id']
            file = drive_service.files().update(
                fileId=existing_file_id,
                media_body=media,
                supportsAllDrives=True,
                fields='id,name,webViewLink'
            ).execute()
            print(f"☁️ Google Drive 파일 업데이트 완료: {filename}")
        else:
            # 새 파일 생성 (Shared Drive 방식)
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True,
                fields='id,name,webViewLink'
            ).execute()
            print(f"☁️ Google Drive 새 파일 업로드 완료: {filename}")
        
        print(f"   파일 ID: {file.get('id')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Google Drive 업로드 실패 ({filename}): {e}")
        return False

def backup_files_to_drive(file_paths):
    """배송 주문서 파일들을 Google Drive에 백업"""
    # 백업 폴더 ID (제공받은 URL에서 추출)
    backup_folder_id = "1-Ena544f3kuTeiQaSOtwrSB3CeM6iS9y"
    
    if not file_paths:
        print("📁 백업할 파일이 없습니다.")
        return False
    
    print(f"☁️ Google Drive 백업 시작... ({len(file_paths)}개 파일)")
    
    success_count = 0
    for file_path in file_paths:
        if os.path.exists(file_path):
            if upload_to_google_drive(file_path, backup_folder_id):
                success_count += 1
        else:
            print(f"⚠️ 파일 없음: {file_path}")
    
    print(f"☁️ Google Drive 백업 완료: {success_count}/{len(file_paths)}개 성공")
    return success_count > 0

def send_shipping_files_email(file_paths, recipient_email=None):
    """배송 주문서 파일들을 이메일로 발송"""
    
    if not file_paths:
        print("📧 발송할 파일이 없습니다.")
        return False
    
    # 수신자 이메일 설정
    if not recipient_email:
        recipient_email = os.getenv('EMAIL_RECIPIENT')
        if not recipient_email:
            print("❌ EMAIL_RECIPIENT 환경변수가 설정되지 않았습니다.")
            return False
    
    # 이메일 설정 (환경변수에서 가져오기)
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port_str = os.getenv('SMTP_PORT')
    sender_email = os.getenv('SMTP_USERNAME')  # GitHub Actions와 일관성
    sender_password = os.getenv('SMTP_PASSWORD')
    
    if not all([smtp_server, smtp_port_str, sender_email, sender_password]):
        print("❌ 이메일 발송 설정이 없습니다.")
        print("   필요한 환경변수: SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD")
        return False
    
    try:
        smtp_port = int(smtp_port_str)
    except ValueError:
        print("❌ SMTP_PORT는 숫자여야 합니다.")
        return False
    
    try:
        # 이메일 메시지 생성
        today_str = datetime.today().strftime('%y%m%d')
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"{today_str} 발송"
        
        # 메일 본문
        body = """배송지를 하기와 같이 첨부합니다. 감사합니다.

첨부 파일:"""
        
        # 첨부 파일 목록 추가
        for file_path in file_paths:
            filename = os.path.basename(file_path)
            body += f"\n- {filename}"
        
        body += f"""

발송 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
처리 시스템: 3PL 자동화 시스템
"""
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # 파일 첨부
        attached_count = 0
        for file_path in file_paths:
            if os.path.exists(file_path):
                filename = os.path.basename(file_path)
                
                with open(file_path, "rb") as attachment:
                    # Excel 파일의 올바른 MIME 타입 설정
                    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                
                # 파일명 인코딩 (한글 파일명 지원)
                encoded_filename = urllib.parse.quote(filename)
                
                # 올바른 파일명과 확장자 설정 (RFC 2231 방식)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename*=UTF-8\'\'{encoded_filename}'
                )
                
                # 추가 헤더로 파일명 명시 (호환성)
                part.add_header(
                    'Content-Type', 
                    f'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; name*=UTF-8\'\'{encoded_filename}'
                )
                
                msg.attach(part)
                attached_count += 1
                print(f"📎 첨부 파일 추가: {filename}")
            else:
                print(f"⚠️ 파일 없음: {file_path}")
        
        if attached_count == 0:
            print("❌ 첨부할 파일이 없습니다.")
            return False
        
        # SMTP 서버 연결 및 발송
        print(f"📧 이메일 발송 중... ({recipient_email})")
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print(f"✅ 이메일 발송 완료!")
        print(f"📧 수신자: {recipient_email}")
        print(f"📧 제목: {today_str} 발송")
        print(f"📧 첨부 파일: {attached_count}개")
        
        return True
        
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
        return False

def collect_shipping_files():
    """오늘 생성된 배송 관련 파일들 수집"""
    from common_utils import DOWNLOAD_DIR
    
    today_str = datetime.today().strftime('%y%m%d')
    
    # 수집할 파일 패턴들
    file_patterns = [
        f"{today_str} 노이지콘텐츠주문서(독독독_국내).xlsx",
        f"{today_str} 노이지콘텐츠주문서(미니학습지_국내).xlsx", 
        f"{today_str} 노이지콘텐츠주문서(EMS).xlsx"
    ]
    
    existing_files = []
    
    for pattern in file_patterns:
        file_path = os.path.join(DOWNLOAD_DIR, pattern)
        if os.path.exists(file_path):
            existing_files.append(file_path)
            print(f"📦 발송 대상 파일: {pattern}")
        else:
            print(f"⚠️ 파일 없음: {pattern}")
    
    return existing_files

def send_processing_result_email(result_summary, po_box_file_path=None):
    """3PL 처리 결과를 이메일로 발송 (별도 수신자, 사서함 주문 파일 첨부 가능)"""
    
    # 처리 결과 전용 수신자 이메일 설정
    recipient_email = os.getenv('LOGIS_EMAIL_RECIPIENT')
    if not recipient_email:
        print("❌ LOGIS_EMAIL_RECIPIENT 환경변수가 설정되지 않았습니다.")
        print("💡 처리 결과 이메일은 물류 담당자용 별도 주소입니다.")
        return False
    
    # 이메일 설정 (환경변수에서 가져오기)
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port_str = os.getenv('SMTP_PORT')
    sender_email = os.getenv('SMTP_USERNAME')
    sender_password = os.getenv('SMTP_PASSWORD')
    
    if not all([smtp_server, smtp_port_str, sender_email, sender_password]):
        print("❌ 이메일 발송 설정이 없습니다.")
        return False
    
    try:
        smtp_port = int(smtp_port_str)
    except ValueError:
        print("❌ SMTP_PORT는 숫자여야 합니다.")
        return False
    
    try:
        # 이메일 메시지 생성
        today_str = datetime.today().strftime('%y%m%d')
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"[3PL] {today_str} 처리 결과 보고"
        
        # 메일 본문 (처리 결과 요약)
        body = f"""3PL 자동화 시스템 처리 결과를 보고드립니다.

{result_summary}

발송 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
처리 시스템: 3PL 자동화 시스템 (GitHub Actions)
"""
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # 사서함 주문 파일 첨부 (있는 경우)
        if po_box_file_path and os.path.exists(po_box_file_path):
            try:
                with open(po_box_file_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    
                    filename = os.path.basename(po_box_file_path)
                    encoded_filename = urllib.parse.quote(filename)
                    
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename*=UTF-8\'\'{encoded_filename}'
                    )
                    msg.attach(part)
                    print(f"📎 사서함 주문 파일 첨부: {filename}")
            except Exception as e:
                print(f"⚠️ 사서함 파일 첨부 실패: {e}")
        
        # SMTP 서버 연결 및 발송
        print(f"📧 처리 결과 이메일 발송 중... ({recipient_email})")
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print(f"✅ 처리 결과 이메일 발송 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 처리 결과 이메일 발송 실패: {e}")
        return False

if __name__ == "__main__":
    print("=== 배송 주문서 이메일 발송 ===")
    
    # 배송 파일들 수집
    shipping_files = collect_shipping_files()
    
    if shipping_files:
        # 이메일 발송
        success = send_shipping_files_email(shipping_files)
        
        if success:
            print("\n🎉 배송 주문서 이메일 발송 완료!")
        else:
            print("\n❌ 이메일 발송 실패")
    else:
        print("\n⚠️ 발송할 배송 주문서가 없습니다.")
    
    print("\n=== 이메일 발송 완료 ===")
