# -*- coding: utf-8 -*-
"""
해피투게더 스타터팩 자동 주문 생성 시스템
"""
import os
import requests
import re
import json
from dotenv import load_dotenv

load_dotenv()

def extract_email_from_customer_note(note):
    """고객 메모에서 이메일 추출"""
    if not note:
        return None
    
    # 이메일 정규표현식
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, note)
    
    if emails:
        email = emails[0]  # 첫 번째 이메일 사용
        print(f"📧 고객 메모에서 이메일 추출: {email}")
        return email
    else:
        print("⚠️ 고객 메모에서 이메일을 찾을 수 없습니다")
        return None

def analyze_product_options(line_item):
    """상품 옵션 분석"""
    meta_data = line_item.get('meta_data', [])
    
    options = {}
    for meta in meta_data:
        key = meta['key']
        value = meta['value']
        
        # 정확한 옵션 키 매칭
        if key == '첫-번째-언어':
            options['first_language'] = value
        elif key == '두-번째-언어':
            options['second_language'] = value
        elif key == '원하는 학습지 유형을 선택하세요!' or key == 'pa_paper-type':
            options['paper_type'] = value
    
    return options

def determine_product_variation(second_language, paper_type):
    """상품 옵션에 따른 상품명 결정"""
    
    # 실제 학습지 유형에 따른 상품명 결정
    if paper_type == 'digital' or paper_type == 'digitalonly':
        product_name = f"1&1-{second_language} 스타터팩[디지털학습지]"
    else:  # paperdigital 또는 기타
        product_name = f"1&1-{second_language} 스타터팩"
    
    print(f"📋 상품명 결정: {paper_type} → {product_name}")
    return product_name

def get_wp_user_id_by_email(email):
    """WP Users API로 이메일 기반 user_id 찾기 (관리자 Application Password 인증)"""
    
    base_url = os.getenv('WP_BASE_URL')
    admin_user = os.getenv('WP_APP_USER')
    app_password = os.getenv('WP_APP_PASSWORD')
    
    if not all([base_url, admin_user, app_password]):
        print("❌ WordPress Application Password 환경변수가 설정되지 않았습니다")
        return None
    
    print(f"🔍 WP Users API로 이메일 검색: {email}")
    
    try:
        response = requests.get(
            f"{base_url}/wp-json/wp/v2/users",
            params={"search": email, "per_page": 100, "context": "edit"},
            auth=(admin_user, app_password), 
            timeout=15
        )
        
        print(f"WP Users API 응답 상태: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ WP Users API 실패: {response.status_code}")
            print(f"❌ 응답: {response.text[:200]}")
            return None
            
        users = response.json()
        print(f"검색된 사용자 수: {len(users)}")
        
        # 이메일 정확 매칭
        for user in users:
            user_email = user.get("email", "")
            if user_email.lower() == email.lower():
                user_id = user.get("id")
                user_name = user.get("name", "")
                print(f"✅ WordPress 사용자 발견!")
                print(f"  User ID: {user_id}")
                print(f"  이메일: {user_email}")
                print(f"  이름: {user_name}")
                return user_id
        
        print(f"📭 해당 이메일의 WordPress 사용자를 찾을 수 없습니다: {email}")
        return None
        
    except Exception as e:
        print(f"❌ WP Users API 오류: {e}")
        return None


def ensure_wc_customer_by_user_id(user_id, email, first_name=""):
    """WooCommerce 고객 레코드 보장 (없으면 초기화)"""
    
    base_url = os.getenv('WP_BASE_URL')
    consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
    consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    
    print(f"🔄 WooCommerce 고객 레코드 확인/초기화: User ID {user_id}")
    
    # 1. 우선 GET /customers/{id} 시도
    customer_url = f"{base_url}/wp-json/wc/v3/customers/{user_id}"
    params = {"consumer_key": consumer_key, "consumer_secret": consumer_secret}
    
    try:
        response = requests.get(customer_url, params=params, timeout=15)
        
        if response.status_code == 200:
            customer = response.json()
            print(f"✅ 기존 WooCommerce 고객 발견: ID {user_id}")
            return user_id
        
        print(f"📭 WooCommerce 고객 레코드 없음 (상태: {response.status_code}) - 초기화 시도")
        
        # 2. 없으면 최소 정보로 초기화 (PUT 권장)
        payload = {"email": email}
        if first_name:
            payload["first_name"] = first_name
        
        put_response = requests.put(customer_url, params=params, json=payload, timeout=15)
        
        if put_response.status_code in (200, 201):
            print(f"✅ WooCommerce 고객 초기화 성공 (PUT): ID {user_id}")
            return user_id
        
        print(f"⚠️ PUT 실패 ({put_response.status_code}) - POST로 재시도")
        
        # 3. PUT가 막혀있으면 POST로도 시도
        customers_url = f"{base_url}/wp-json/wc/v3/customers"
        post_response = requests.post(
            customers_url, 
            params=params, 
            json={"email": email, "first_name": first_name}, 
            timeout=15
        )
        
        if post_response.status_code in (200, 201):
            created_id = post_response.json().get("id", user_id)
            print(f"✅ WooCommerce 고객 생성 성공 (POST): ID {created_id}")
            return created_id
        
        print(f"❌ WooCommerce 고객 초기화 완전 실패:")
        print(f"  PUT: {put_response.status_code} - {put_response.text[:200]}")
        print(f"  POST: {post_response.status_code} - {post_response.text[:200]}")
        return None
        
    except Exception as e:
        print(f"❌ WooCommerce 고객 초기화 오류: {e}")
        return None


def find_user_by_stable_flow(email):
    """안정적인 사용자 찾기 플로우 (WP Users → WooCommerce 고객 보장)"""
    
    print(f"🚀 안정적인 플로우로 사용자 검색: {email}")
    
    # 1단계: WP Users API로 user_id 찾기
    wp_user_id = get_wp_user_id_by_email(email)
    
    if not wp_user_id:
        print(f"📭 WordPress 사용자 없음 - 신규 사용자로 처리")
        return None
    
    # 2단계: WooCommerce 고객 레코드 보장
    first_name = email.split('@')[0]  # 기본 이름
    customer_id = ensure_wc_customer_by_user_id(wp_user_id, email, first_name)
    
    if customer_id:
        print(f"🎉 안정적 플로우 성공: User ID {customer_id}")
        return {
            "id": customer_id,
            "name": first_name,
            "email": email
        }
    else:
        print(f"❌ WooCommerce 고객 초기화 실패 - 게스트로 폴백")
        return None


def check_if_friend_order_exists(original_order_id, friend_email):
    """meta_data의 원본_주문번호로 중복 주문 확인"""
    
    base_url = os.getenv('WP_BASE_URL')
    consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
    consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    
    # 최근 주문들에서 원본 주문번호가 메타데이터에 있는지 확인
    orders_url = f"{base_url}/wp-json/wc/v3/orders"
    params = {
        'consumer_key': consumer_key,
        'consumer_secret': consumer_secret,
        'per_page': 50,  # 최근 50개 주문 확인
        'orderby': 'date',
        'order': 'desc'
    }
    
    try:
        if base_url.startswith('https://'):
            full_params = {**params}
            response = requests.get(orders_url, params=full_params, timeout=15)
        else:
            auth = (consumer_key, consumer_secret)
            response = requests.get(orders_url, params={'per_page': 50, 'orderby': 'date', 'order': 'desc'}, auth=auth, timeout=15)
        
        if response.status_code == 200:
            orders = response.json()
            
            for order in orders:
                # 고객 이메일 확인
                billing_email = order.get('billing', {}).get('email', '')
                
                if billing_email.lower() == friend_email.lower():
                    # 해당 주문의 line_items에서 원본_주문번호 메타데이터 확인
                    for item in order.get('line_items', []):
                        for meta in item.get('meta_data', []):
                            if (meta.get('key') == '원본_주문번호' and 
                                str(meta.get('value')) == str(original_order_id)):
                                print(f"⚠️ 이미 생성된 친구 주문 발견: {order['id']}")
                                return order['id']
            
            return None
        else:
            print(f"❌ 주문 검색 실패: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ 중복 확인 오류: {e}")
        return None


def get_original_customer_info(order_id):
    """원본 주문의 고객 정보 가져오기"""
    
    base_url = os.getenv('WP_BASE_URL')
    consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
    consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    
    order_url = f'{base_url}/wp-json/wc/v3/orders/{order_id}'
    params = {
        'consumer_key': consumer_key,
        'consumer_secret': consumer_secret
    }
    
    try:
        response = requests.get(order_url, params=params, timeout=10)
        if response.status_code == 200:
            order = response.json()
            return {
                'customer_id': order.get('customer_id', 0),
                'billing': order.get('billing', {}),
                'shipping': order.get('shipping', {})
            }
        else:
            print(f'❌ 원본 주문 조회 실패: {response.status_code}')
            return None
    except Exception as e:
        print(f'❌ 원본 주문 조회 오류: {e}')
        return None

def create_new_order_for_friend(friend_email, product_name, original_order_id, original_customer_info):
    """친구를 위한 새 주문 생성 (조건별 상태 설정)"""
    
    # 미니학습지 환경변수
    base_url = os.getenv('WP_BASE_URL')
    consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
    consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    happy_together_product_id = os.getenv('HAPPY_TOGETHER_PRODUCT_ID')
    
    if not all([base_url, consumer_key, consumer_secret, happy_together_product_id]):
        print("❌ 해피투게더 환경변수가 설정되지 않았습니다")
        print("   필요한 환경변수: WP_BASE_URL, WP_WOO_CONSUMER_KEY, WP_WOO_CONSUMER_SECRET, HAPPY_TOGETHER_PRODUCT_ID")
        return False
    
    # 주문 생성 시나리오 결정 (안정적 플로우 사용)
    if friend_email:
        # 1단계: WP Users로 user_id 확보
        wp_user_id = get_wp_user_id_by_email(friend_email)
        
        # 2단계: WooCommerce 고객 초기화/확인 (Customers API 이메일 검색 우회)
        customer_id = None
        if wp_user_id:
            customer_id = ensure_wc_customer_by_user_id(
                wp_user_id, friend_email, first_name=friend_email.split("@")[0]
            )
        
        if customer_id:
            # ✅ 회원 주문
            order_status = "shipped"  # 기존 사용자 = 배송완료
            billing_info = {
                "first_name": friend_email.split('@')[0],
                "last_name": "",
                "email": friend_email,
                "phone": ""
            }
            print(f"📋 기존 사용자 ID {customer_id}로 회원 주문 생성")
        else:
            # 게스트로 폴백
            customer_id = 0
            order_status = "processing"  # 신규 사용자 = 진행중
            billing_info = {
                "first_name": friend_email.split('@')[0],
                "last_name": "",
                "email": friend_email,
                "phone": ""
            }
            print("📋 신규 사용자로 게스트 주문 생성")
    else:
        # 원 주문자로 주문 생성
        customer_id = original_customer_info.get('customer_id', 0)
        order_status = "shipped"
        billing_info = original_customer_info.get('billing', {})
        print(f"📋 원 주문자로 주문 생성")
    
    # 새 주문 데이터
    new_order_data = {
        "payment_method": "",
        "payment_method_title": "1&1 친구언어 자동 추가",
        "set_paid": True,
        "status": order_status,
        "customer_id": customer_id,
        "billing": billing_info,
        "shipping": billing_info,
        "meta_data": [
            {
                "key": "_customer_user_agent",
                "value": "해피투게더 자동 시스템"
            },
            {
                "key": "_created_via",
                "value": "해피투게더"
            }
        ],
        "line_items": [
            {
                "product_id": int(happy_together_product_id),  # 미니학습지 [친구언어 추가] 상품 ID
                "quantity": 1,
                "meta_data": [
                    {
                        "key": "상품명",
                        "value": product_name
                    },
                    {
                        "key": "원본_주문번호", 
                        "value": str(original_order_id)
                    }
                ]
            }
        ]
    }
    
    # API 호출
    orders_url = f"{base_url}/wp-json/wc/v3/orders"
    
    try:
        if base_url.startswith('https://'):
            params = {
                'consumer_key': consumer_key,
                'consumer_secret': consumer_secret
            }
            response = requests.post(orders_url, json=new_order_data, params=params, timeout=15)
        else:
            auth = (consumer_key, consumer_secret)
            response = requests.post(orders_url, json=new_order_data, auth=auth, timeout=15)
        
        if response.status_code == 201:
            new_order = response.json()
            new_order_id = new_order['id']
            print(f"✅ 친구 주문 생성 성공!")
            print(f"📦 새 주문번호: {new_order_id}")
            print(f"📧 친구 이메일: {friend_email}")
            print(f"🎁 상품명: {product_name}")
            return new_order_id
        else:
            print(f"❌ 친구 주문 생성 실패: {response.status_code}")
            print(f"❌ 응답: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ 친구 주문 생성 오류: {e}")
        return False

def get_order_details_with_options(order_id):
    """주문 상세 정보 및 옵션 조회"""
    
    base_url = os.getenv('WP_BASE_URL')
    consumer_key = os.getenv('WP_WOO_CONSUMER_KEY')
    consumer_secret = os.getenv('WP_WOO_CONSUMER_SECRET')
    
    order_url = f'{base_url}/wp-json/wc/v3/orders/{order_id}'
    params = {
        'consumer_key': consumer_key,
        'consumer_secret': consumer_secret
    }
    
    try:
        response = requests.get(order_url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'❌ 주문 상세 조회 실패: {response.status_code}')
            return None
    except Exception as e:
        print(f'❌ 주문 상세 조회 오류: {e}')
        return None

def determine_product_variation(second_language, paper_type):
    """상품 옵션에 따른 상품명 결정"""
    
    # 실제 학습지 유형에 따른 상품명 결정
    if paper_type == 'digital' or paper_type == 'digitalonly':
        product_name = f"1&1-{second_language} 스타터팩[디지털학습지]"
    else:  # paperdigital 또는 기타
        product_name = f"1&1-{second_language} 스타터팩"
    
    print(f"📋 상품명 결정: {paper_type} → {product_name}")
    return product_name

def process_single_order(order_id):
    """단일 주문에 대한 해피투게더 처리"""
    
    print(f"🔍 주문 {order_id} 해피투게더 처리 시작")
    
    # 주문 상세 정보 조회
    order_details = get_order_details_with_options(order_id)
    
    if not order_details:
        print(f"❌ 주문 {order_id}을 찾을 수 없습니다")
        return False
    
    print(f"✅ 주문 조회 성공")
    print(f"📧 고객 이메일: {order_details['billing']['email']}")
    print(f"📝 고객 노트: {order_details.get('customer_note', 'N/A')}")
    
    # 스타터팩 상품 찾기
    starter_pack_item = None
    for item in order_details.get('line_items', []):
        if "스타터팩" in item.get('name', ''):
            starter_pack_item = item
            break
    
    if not starter_pack_item:
        print("📭 스타터팩 상품이 아님 - 해피투게더 대상 아님")
        return False
    
    print(f"✅ 스타터팩 상품 발견: {starter_pack_item['name']}")
    
    # 고객 메모에서 친구 이메일 추출
    customer_note = order_details.get('customer_note', '')
    friend_email = extract_email_from_customer_note(customer_note)
    
    # 중복 주문 확인 (친구 이메일이 있는 경우에만)
    if friend_email:
        existing_order = check_if_friend_order_exists(order_id, friend_email)
        if existing_order:
            print(f"⚠️ 이미 처리된 주문입니다: 기존 친구 주문 {existing_order}")
            return False
    
    # 상품 옵션 분석
    options = analyze_product_options(starter_pack_item)
    second_language = options.get('second_language', '')
    paper_type = options.get('paper_type', '')
    
    print(f"📋 상품 옵션:")
    print(f"  - 두 번째 언어: {second_language}")
    print(f"  - 학습지 유형: {paper_type}")
    
    if not second_language:
        print("❌ 두 번째 언어 옵션을 찾을 수 없습니다")
        return False
    
    # 새 상품명 결정
    new_product_name = determine_product_variation(second_language, paper_type)
    print(f"🎁 생성할 상품명: {new_product_name}")
    
    # 원본 주문 고객 정보
    original_customer_info = {
        'customer_id': order_details.get('customer_id', 0),
        'billing': order_details.get('billing', {}),
        'shipping': order_details.get('shipping', {})
    }
    
    # 친구 주문 생성
    print(f"\n🚀 친구 주문 생성 시작...")
    new_order_id = create_new_order_for_friend(
        friend_email,
        new_product_name,
        order_id,
        original_customer_info
    )
    
    if new_order_id:
        print(f"\n🎉 성공!")
        print(f"📦 원본 주문: {order_id}")
        print(f"📦 새 주문: {new_order_id}")
        print(f"📧 친구 이메일: {friend_email if friend_email else '원 주문자'}")
        print(f"🎁 상품명: {new_product_name}")
        return True
    else:
        print(f"\n❌ 친구 주문 생성 실패")
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 명령행 인수로 주문 ID 전달
        order_id = int(sys.argv[1])
        process_single_order(order_id)
    else:
        print("사용법: python3 happy_together_processor.py <주문번호>")
        print("예시: python3 happy_together_processor.py 654371")