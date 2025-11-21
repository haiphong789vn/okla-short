"""
TranscriptAPI Key Manager
==========================
Manages TranscriptAPI.com API keys with automatic registration and rotation.

Features:
- Get active API key from database
- Auto-register new accounts when quota exceeded
- Rotate keys automatically
- Update key status in database
"""

import os
import sys
import time
import random
import string
import logging
import requests
import cloudscraper
import re
import json
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_db, ApiKey

# ============================================================================
# CONFIGURATION
# ============================================================================

AUTH_REGISTER_ENDPOINT = "https://transcriptapi.com/api/auth/register"
AUTH_LOGIN_ENDPOINT = "https://transcriptapi.com/api/auth/login"
AUTH_SEND_OTP_ENDPOINT = "https://transcriptapi.com/api/auth/send-verification-otp"
AUTH_VERIFY_EMAIL_ENDPOINT = "https://transcriptapi.com/api/auth/verify-email"
API_KEYS_ENDPOINT = "https://transcriptapi.com/api/auth/api-keys"
TRANSCRIPT_API_ENDPOINT = "https://transcriptapi.com/api/v2/youtube/transcript"

# Tempmail API
TEMPMAIL_API_BASE = "https://tempmail.id.vn/api"
TEMPMAIL_API_TOKEN = "6176|49y9Xuny631gRZekQf5I8CxmP9r6Fshtb2FhRZwH152f0315"
TEMPMAIL_DOMAINS = [
    "tempmail.id.vn",
    "1trick.net",
    "hathitrannhien.edu.vn",
    "nghienplus.io.vn",
    "tempmail.ckvn.edu.vn"
]

# Testmail.app API (Backup)
TESTMAIL_API_KEY = "de9972d6-6446-46e8-9f69-0ac5f8a92fb8"
TESTMAIL_NAMESPACE = "sg7ph"
TESTMAIL_API_ENDPOINT = "https://api.testmail.app/api/json"

logger = logging.getLogger(__name__)

# Create cloudscraper instance for bypassing Cloudflare (tempmail.id.vn)
scraper = cloudscraper.create_scraper()

# ============================================================================
# KEY MANAGEMENT
# ============================================================================

def get_active_key(db_session) -> Optional[ApiKey]:
    """
    Get active TranscriptAPI key from database

    Args:
        db_session: Database session

    Returns:
        ApiKey object or None
    """
    try:
        # Get least recently used active key
        key = db_session.query(ApiKey).filter_by(
            service='transcript_api',
            status='active'
        ).order_by(ApiKey.last_used.asc().nullsfirst()).first()

        return key

    except Exception as e:
        logger.error(f"Error getting active key: {str(e)}")
        return None

def mark_key_used(db_session, key_id: int):
    """
    Mark key as used (update usage count and last_used)

    Args:
        db_session: Database session
        key_id: API key ID
    """
    try:
        key = db_session.query(ApiKey).filter_by(id=key_id).first()
        if key:
            key.usage_count = (key.usage_count or 0) + 1
            key.last_used = datetime.utcnow()
            db_session.commit()
            logger.debug(f"Marked key {key_id} as used (count: {key.usage_count})")

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error marking key as used: {str(e)}")

def disable_key(db_session, key_id: int, reason: str):
    """
    Disable an API key

    Args:
        db_session: Database session
        key_id: API key ID
        reason: Reason for disabling
    """
    try:
        key = db_session.query(ApiKey).filter_by(id=key_id).first()
        if key:
            key.status = 'disabled'
            key.disabled_reason = reason
            key.updated_at = datetime.utcnow()
            db_session.commit()
            logger.warning(f"⚠ Disabled key {key_id}: {reason}")

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error disabling key: {str(e)}")

def get_active_keys_count(db_session) -> int:
    """
    Count active TranscriptAPI keys

    Args:
        db_session: Database session

    Returns:
        Number of active keys
    """
    try:
        count = db_session.query(ApiKey).filter_by(
            service='transcript_api',
            status='active'
        ).count()
        return count

    except Exception as e:
        logger.error(f"Error counting active keys: {str(e)}")
        return 0

# ============================================================================
# AUTO REGISTRATION
# ============================================================================

# ============================================================================
# TEMPMAIL API FUNCTIONS
# ============================================================================

def create_temp_email() -> Optional[Tuple[str, int]]:
    """
    Create temporary email using tempmail.id.vn API

    Returns:
        Tuple of (email, email_id) or None
    """
    try:
        # Generate random username
        username_length = random.randint(10, 15)
        username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=username_length))

        # Random domain
        domain = random.choice(TEMPMAIL_DOMAINS)

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {TEMPMAIL_API_TOKEN}'
        }

        payload = {
            "user": username,
            "domain": domain
        }

        logger.info(f"[TEMPMAIL] Creating temp email: {username}@{domain}")

        # Log detailed request info
        request_url = f"{TEMPMAIL_API_BASE}/email/create"
        logger.info(f"[TEMPMAIL] Request URL: {request_url}")
        logger.info(f"[TEMPMAIL] Request Headers: {json.dumps({k: v for k, v in headers.items() if k != 'Authorization'}, indent=2)}")
        logger.info(f"[TEMPMAIL] Authorization: Bearer {TEMPMAIL_API_TOKEN[:20]}...{TEMPMAIL_API_TOKEN[-10:]}")
        logger.info(f"[TEMPMAIL] Request Payload: {json.dumps(payload, indent=2)}")

        # Use cloudscraper to bypass Cloudflare
        logger.info(f"[TEMPMAIL] Using cloudscraper to bypass Cloudflare...")
        response = scraper.post(
            request_url,
            headers=headers,
            json=payload,
            timeout=30
        )

        # Log response details for debugging
        logger.info(f"[TEMPMAIL] Response Status: {response.status_code}")
        logger.debug(f"[TEMPMAIL] Response Headers: {dict(response.headers)}")

        if response.status_code != 200:
            try:
                error_data = response.json()
                logger.error(f"[TEMPMAIL] ✗ API Error ({response.status_code}): {error_data}")
            except:
                logger.error(f"[TEMPMAIL] ✗ HTTP {response.status_code}: {response.text[:200]}")
            return None

        data = response.json()

        if data.get('success') and 'data' in data:
            email_data = data['data']
            email = email_data.get('email')
            email_id = email_data.get('id')

            if email and email_id:
                logger.info(f"[TEMPMAIL] ✓ Created: {email} (ID: {email_id})")
                return (email, email_id)

        logger.error(f"[TEMPMAIL] ✗ Invalid response: {data}")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"[TEMPMAIL] ✗ Request error: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"[TEMPMAIL] Response body: {e.response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"[TEMPMAIL] ✗ Unexpected error: {str(e)}")
        return None

def get_email_list() -> Optional[list]:
    """
    Get list of emails from tempmail

    Returns:
        List of email data or None
    """
    try:
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {TEMPMAIL_API_TOKEN}'
        }

        logger.info(f"[TEMPMAIL] Fetching email list...")

        # Use cloudscraper to bypass Cloudflare
        response = scraper.get(
            f"{TEMPMAIL_API_BASE}/email",
            headers=headers,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if data.get('success') and 'data' in data:
            emails = data['data']
            logger.info(f"[TEMPMAIL] ✓ Found {len(emails)} email(s)")
            return emails

        logger.error(f"[TEMPMAIL] ✗ Invalid response: {data}")
        return None

    except Exception as e:
        logger.error(f"[TEMPMAIL] ✗ Error getting email list: {str(e)}")
        return None

def get_email_detail(email_id: int) -> Optional[dict]:
    """
    Get email detail (inbox messages)

    Args:
        email_id: Email ID from tempmail

    Returns:
        Email detail data or None
    """
    try:
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {TEMPMAIL_API_TOKEN}'
        }

        logger.info(f"[TEMPMAIL] Fetching inbox for email ID: {email_id}")

        # Use cloudscraper to bypass Cloudflare
        response = scraper.get(
            f"{TEMPMAIL_API_BASE}/email/{email_id}",
            headers=headers,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if data.get('success') and 'data' in data:
            inbox_data = data['data']
            items = inbox_data.get('items', [])
            logger.info(f"[TEMPMAIL] ✓ Found {len(items)} message(s)")
            return inbox_data

        logger.error(f"[TEMPMAIL] ✗ Invalid response: {data}")
        return None

    except Exception as e:
        logger.error(f"[TEMPMAIL] ✗ Error getting email detail: {str(e)}")
        return None

def get_email_message(message_id: int) -> Optional[str]:
    """
    Get email message content

    Args:
        message_id: Message ID from tempmail

    Returns:
        Email HTML body or None
    """
    try:
        headers = {
            'Authorization': f'Bearer {TEMPMAIL_API_TOKEN}'
        }

        logger.info(f"[TEMPMAIL] Fetching message ID: {message_id}")

        # Use cloudscraper to bypass Cloudflare
        response = scraper.get(
            f"{TEMPMAIL_API_BASE}/message/{message_id}",
            headers=headers,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if data.get('success') and 'data' in data:
            message_data = data['data']
            body = message_data.get('body', '')
            logger.info(f"[TEMPMAIL] ✓ Got message content")
            return body

        logger.error(f"[TEMPMAIL] ✗ Invalid response: {data}")
        return None

    except Exception as e:
        logger.error(f"[TEMPMAIL] ✗ Error getting message: {str(e)}")
        return None

def extract_otp_from_html(html_content: str) -> Optional[str]:
    """
    Extract OTP code from email HTML content

    Args:
        html_content: HTML content of email

    Returns:
        OTP code or None
    """
    try:
        # Pattern to match 6-digit OTP code in HTML
        # Looking for pattern like: <div style="...font-size:32px...">123456</div>
        pattern = r'font-size:\s*32px[^>]*>(\d{6})<'

        match = re.search(pattern, html_content)
        if match:
            otp = match.group(1)
            logger.info(f"[OTP] ✓ Extracted OTP: {otp}")
            return otp

        # Fallback: try to find any 6-digit number
        pattern2 = r'\b(\d{6})\b'
        matches = re.findall(pattern2, html_content)
        if matches:
            otp = matches[0]
            logger.info(f"[OTP] ✓ Extracted OTP (fallback): {otp}")
            return otp

        logger.error(f"[OTP] ✗ Could not extract OTP from HTML")
        return None

    except Exception as e:
        logger.error(f"[OTP] ✗ Error extracting OTP: {str(e)}")
        return None

# ============================================================================
# TESTMAIL.APP API FUNCTIONS (BACKUP)
# ============================================================================

def create_testmail_email() -> Optional[Tuple[str, str]]:
    """
    Create temporary email using testmail.app (backup method)

    Returns:
        Tuple of (email, tag) or None
    """
    try:
        # Generate random tag
        tag_length = random.randint(8, 12)
        tag = ''.join(random.choices(string.ascii_lowercase + string.digits, k=tag_length))

        # Email format: {tag}.{namespace}@inbox.testmail.app
        email = f"{tag}.{TESTMAIL_NAMESPACE}@inbox.testmail.app"

        logger.info(f"[TESTMAIL] Creating temp email: {email}")
        logger.info(f"[TESTMAIL] Tag: {tag}, Namespace: {TESTMAIL_NAMESPACE}")

        # No API call needed to create email - just return it
        return (email, tag)

    except Exception as e:
        logger.error(f"[TESTMAIL] ✗ Error creating email: {str(e)}")
        return None

def wait_for_testmail_message(tag: str, max_wait: int = 60, poll_interval: int = 10) -> Optional[dict]:
    """
    Wait for email message using testmail.app API with polling

    Args:
        tag: Email tag (prefix)
        max_wait: Maximum wait time in seconds (default 60s)
        poll_interval: Interval between polls in seconds (default 10s)

    Returns:
        Email message data or None
    """
    logger.info(f"[TESTMAIL] Waiting for email with tag: {tag}")
    logger.info(f"[TESTMAIL] API Endpoint: {TESTMAIL_API_ENDPOINT}")
    logger.info(f"[TESTMAIL] Max wait: {max_wait}s, Poll interval: {poll_interval}s")

    max_attempts = max_wait // poll_interval

    for attempt in range(1, max_attempts + 1):
        try:
            params = {
                'apikey': TESTMAIL_API_KEY,
                'namespace': TESTMAIL_NAMESPACE,
                'tag': tag
            }

            logger.info(f"[TESTMAIL] Poll attempt {attempt}/{max_attempts}")

            response = requests.get(
                TESTMAIL_API_ENDPOINT,
                params=params,
                timeout=15  # Short timeout per request
            )

            response.raise_for_status()
            data = response.json()

            logger.debug(f"[TESTMAIL] Response: {json.dumps(data, indent=2)[:500]}...")

            if data.get('result') == 'success':
                emails = data.get('emails', [])
                if emails:
                    logger.info(f"[TESTMAIL] ✓ Found {len(emails)} email(s) on attempt {attempt}")
                    return emails[0]  # Return first email
                else:
                    logger.info(f"[TESTMAIL] No emails yet (attempt {attempt}/{max_attempts})")
                    if attempt < max_attempts:
                        logger.info(f"[TESTMAIL] Waiting {poll_interval}s before next poll...")
                        time.sleep(poll_interval)
                        continue
            else:
                error_msg = data.get('message', 'Unknown error')
                logger.error(f"[TESTMAIL] ✗ API Error: {error_msg}")
                return None

        except requests.exceptions.Timeout:
            logger.warning(f"[TESTMAIL] Request timeout on attempt {attempt}")
            if attempt < max_attempts:
                time.sleep(poll_interval)
                continue
            return None
        except Exception as e:
            logger.error(f"[TESTMAIL] ✗ Error on attempt {attempt}: {str(e)}")
            if attempt < max_attempts:
                time.sleep(poll_interval)
                continue
            return None

    logger.warning(f"[TESTMAIL] No email received after {max_wait}s")
    return None

def extract_otp_from_testmail(email_data: dict) -> Optional[str]:
    """
    Extract OTP from testmail.app email data

    Args:
        email_data: Email data from testmail.app API

    Returns:
        OTP code or None
    """
    try:
        # Get email HTML content
        html_content = email_data.get('html', '')
        text_content = email_data.get('text', '')

        logger.info(f"[TESTMAIL-OTP] Extracting OTP from email")
        logger.debug(f"[TESTMAIL-OTP] Subject: {email_data.get('subject', 'N/A')}")

        # Try HTML first
        if html_content:
            otp = extract_otp_from_html(html_content)
            if otp:
                return otp

        # Try text content
        if text_content:
            pattern = r'\b(\d{6})\b'
            matches = re.findall(pattern, text_content)
            if matches:
                otp = matches[0]
                logger.info(f"[TESTMAIL-OTP] ✓ Extracted OTP from text: {otp}")
                return otp

        logger.error(f"[TESTMAIL-OTP] ✗ Could not extract OTP")
        return None

    except Exception as e:
        logger.error(f"[TESTMAIL-OTP] ✗ Error: {str(e)}")
        return None

# ============================================================================
# TRANSCRIPTAPI OTP VERIFICATION
# ============================================================================

def send_verification_otp(access_token: str) -> bool:
    """
    Send verification OTP to email

    Args:
        access_token: Access token from login

    Returns:
        True if successful
    """
    try:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'authorization': f'Bearer {access_token}',
            'content-length': '0',
            'origin': 'https://transcriptapi.com',
            'referer': 'https://transcriptapi.com/verify-email',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        cookies = {
            'access_token': access_token
        }

        logger.info(f"[OTP] Sending verification OTP...")

        response = requests.post(
            AUTH_SEND_OTP_ENDPOINT,
            headers=headers,
            cookies=cookies,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if 'message' in data and 'sent_at' in data:
            logger.info(f"[OTP] ✓ OTP sent: {data['message']}")
            return True

        logger.error(f"[OTP] ✗ Unexpected response: {data}")
        return False

    except Exception as e:
        logger.error(f"[OTP] ✗ Error sending OTP: {str(e)}")
        return False

def verify_email_with_otp(access_token: str, otp: str) -> bool:
    """
    Verify email with OTP code

    Args:
        access_token: Access token from login
        otp: OTP code from email

    Returns:
        True if successful
    """
    try:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'authorization': f'Bearer {access_token}',
            'content-type': 'application/json',
            'origin': 'https://transcriptapi.com',
            'referer': 'https://transcriptapi.com/verify-email',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        cookies = {
            'access_token': access_token
        }

        payload = {
            "otp": otp
        }

        logger.info(f"[VERIFY] Verifying email with OTP: {otp}")

        response = requests.post(
            AUTH_VERIFY_EMAIL_ENDPOINT,
            headers=headers,
            cookies=cookies,
            json=payload,
            timeout=30
        )

        # HTTP 200 with no response means success
        if response.status_code == 200:
            logger.info(f"[VERIFY] ✓ Email verified successfully!")
            return True

        logger.error(f"[VERIFY] ✗ Verification failed: HTTP {response.status_code}")
        return False

    except Exception as e:
        logger.error(f"[VERIFY] ✗ Error verifying email: {str(e)}")
        return False

def generate_random_password() -> str:
    """Generate random strong password"""
    length = random.randint(12, 16)
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=length))

def register_account(email: str, password: str) -> bool:
    """
    Register new account on TranscriptAPI.com

    Args:
        email: Email for registration
        password: Password

    Returns:
        True if successful
    """
    try:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'origin': 'https://transcriptapi.com',
            'referer': 'https://transcriptapi.com/signup',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        payload = {
            "email": email,
            "password": password,
            "name": "",
            "agreeToTerms": True
        }

        logger.info(f"[REGISTER] Registering account: {email}")

        response = requests.post(
            AUTH_REGISTER_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if 'id' in data and 'email' in data:
            logger.info(f"[REGISTER] ✓ Registration successful!")
            return True
        else:
            logger.error(f"[REGISTER] ✗ Invalid response: {data}")
            return False

    except Exception as e:
        logger.error(f"[REGISTER] ✗ Error: {str(e)}")
        return False

def login_account(email: str, password: str) -> Optional[str]:
    """
    Login and get access token

    Args:
        email: Email
        password: Password

    Returns:
        Access token or None
    """
    try:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://transcriptapi.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        payload = f"username={email}&password={password}"

        logger.info(f"[LOGIN] Logging in: {email}")

        response = requests.post(
            AUTH_LOGIN_ENDPOINT,
            headers=headers,
            data=payload,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if 'access_token' in data:
            logger.info(f"[LOGIN] ✓ Login successful!")
            return data['access_token']
        else:
            logger.error(f"[LOGIN] ✗ No access_token in response")
            return None

    except Exception as e:
        logger.error(f"[LOGIN] ✗ Error: {str(e)}")
        return None

def get_api_key_from_account(access_token: str) -> Optional[str]:
    """
    Get API key from account using access token

    Args:
        access_token: Access token from login

    Returns:
        API key or None
    """
    try:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'authorization': f'Bearer {access_token}',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        cookies = {
            'access_token': access_token
        }

        logger.info(f"[API-KEY] Fetching API key...")

        response = requests.get(
            API_KEYS_ENDPOINT,
            headers=headers,
            cookies=cookies,
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        if isinstance(data, list) and len(data) > 0:
            first_key = data[0]
            api_key = first_key.get('key')

            if api_key:
                logger.info(f"[API-KEY] ✓ Got API key!")
                return api_key
            else:
                logger.error(f"[API-KEY] ✗ No key in response")
                return None
        else:
            logger.error(f"[API-KEY] ✗ Invalid response")
            return None

    except Exception as e:
        logger.error(f"[API-KEY] ✗ Error: {str(e)}")
        return None

def auto_register_and_save_key(db_session) -> Optional[ApiKey]:
    """
    Auto-register new account and save API key to database
    Now with email OTP verification support

    Args:
        db_session: Database session

    Returns:
        ApiKey object or None
    """
    logger.info(f"\n{'='*80}")
    logger.info("AUTO-REGISTERING NEW TRANSCRIPTAPI ACCOUNT WITH OTP VERIFICATION")
    logger.info(f"{'='*80}")

    # Step 1: Create temporary email (try tempmail.id.vn first, then testmail.app)
    logger.info("[STEP 1] Creating temporary email...")

    use_testmail = False
    temp_email_result = create_temp_email()

    if not temp_email_result:
        logger.warning("[FALLBACK] tempmail.id.vn failed, trying testmail.app backup...")
        testmail_result = create_testmail_email()
        if testmail_result:
            email, email_tag = testmail_result
            use_testmail = True
            logger.info(f"[FALLBACK] ✓ Using testmail.app")
        else:
            logger.error("[FAILED] Both tempmail.id.vn and testmail.app failed")
            return None
    else:
        email, temp_email_id = temp_email_result
        logger.info(f"[INFO] Using tempmail.id.vn")

    password = generate_random_password()
    logger.info(f"[INFO] Email: {email}")

    if use_testmail:
        logger.info(f"[INFO] Email Tag: {email_tag}")
    else:
        logger.info(f"[INFO] Temp Email ID: {temp_email_id}")

    # Step 2: Register account
    logger.info("[STEP 2] Registering account...")
    if not register_account(email, password):
        logger.error("[FAILED] Registration failed")
        return None

    time.sleep(2)

    # Step 3: Login
    logger.info("[STEP 3] Logging in...")
    access_token = login_account(email, password)
    if not access_token:
        logger.error("[FAILED] Login failed")
        return None

    time.sleep(2)

    # Step 4: Send verification OTP
    logger.info("[STEP 4] Sending verification OTP...")
    if not send_verification_otp(access_token):
        logger.error("[FAILED] Could not send OTP")
        return None

    # Step 5-9: Get OTP code (different logic for tempmail vs testmail)
    if use_testmail:
        # Using testmail.app - polling with 10s intervals
        logger.info("[STEP 5-9] Waiting for OTP email (testmail.app with polling)...")
        email_data = wait_for_testmail_message(email_tag, max_wait=60, poll_interval=10)
        if not email_data:
            logger.error("[FAILED] Could not receive email via testmail.app after 60s")
            return None

        logger.info("[TESTMAIL] ✓ Email received, extracting OTP...")
        otp_code = extract_otp_from_testmail(email_data)
        if not otp_code:
            logger.error("[FAILED] Could not extract OTP code from testmail")
            return None

    else:
        # Using tempmail.id.vn - manual polling
        logger.info("[STEP 5] Waiting 30 seconds for OTP email to arrive...")
        time.sleep(30)

        logger.info("[STEP 6] Fetching email list...")
        email_list = get_email_list()
        if not email_list:
            logger.error("[FAILED] Could not get email list")
            return None

        # Find our email by ID
        our_email = None
        for e in email_list:
            if e.get('id') == temp_email_id:
                our_email = e
                break

        if not our_email:
            logger.error(f"[FAILED] Could not find email with ID {temp_email_id}")
            return None

        logger.info("[STEP 7] Fetching inbox messages...")
        inbox_data = get_email_detail(temp_email_id)
        if not inbox_data:
            logger.error("[FAILED] Could not get inbox")
            return None

        items = inbox_data.get('items', [])
        if not items:
            logger.error("[FAILED] No messages in inbox")
            return None

        # Get first message (should be OTP email)
        first_message = items[0]
        message_id = first_message.get('id')

        if not message_id:
            logger.error("[FAILED] No message ID found")
            return None

        logger.info("[STEP 8] Fetching message content...")
        message_body = get_email_message(message_id)
        if not message_body:
            logger.error("[FAILED] Could not get message content")
            return None

        logger.info("[STEP 9] Extracting OTP from email...")
        otp_code = extract_otp_from_html(message_body)
        if not otp_code:
            logger.error("[FAILED] Could not extract OTP code")
            return None

    # Step 10: Verify email with OTP
    logger.info("[STEP 10] Verifying email with OTP...")
    if not verify_email_with_otp(access_token, otp_code):
        logger.error("[FAILED] Email verification failed")
        return None

    time.sleep(2)

    # Step 11: Get API key
    logger.info("[STEP 11] Fetching API key...")
    api_key = get_api_key_from_account(access_token)
    if not api_key:
        logger.error("[FAILED] Could not get API key")
        return None

    # Step 12: Save to database
    logger.info("[STEP 12] Saving to database...")
    try:
        new_key = ApiKey(
            service='transcript_api',
            api_key=api_key,
            email=email,
            password=password,  # Consider encrypting in production
            status='active',
            usage_count=0
        )

        db_session.add(new_key)
        db_session.commit()

        logger.info(f"\n{'='*80}")
        logger.info("✓ AUTO-REGISTRATION WITH OTP VERIFICATION SUCCESSFUL!")
        logger.info(f"{'='*80}")
        logger.info(f"API Key: {api_key[:20]}...")
        logger.info(f"Saved to database with ID: {new_key.id}")

        return new_key

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error saving key to database: {str(e)}")
        return None

# ============================================================================
# TRANSCRIPT FETCHING WITH AUTO-RETRY
# ============================================================================

def fetch_transcript_with_retry(
    video_id: str,
    db_session,
    max_attempts: int = 3
) -> Optional[list]:
    """
    Fetch transcript with automatic key rotation and renewal

    Args:
        video_id: YouTube video ID
        db_session: Database session
        max_attempts: Maximum number of auto-registration attempts

    Returns:
        Transcript data or None
    """
    for attempt in range(max_attempts):
        # Get active key
        key_obj = get_active_key(db_session)

        if not key_obj:
            logger.warning("No active key available, registering new account...")
            key_obj = auto_register_and_save_key(db_session)

            if not key_obj:
                logger.error("Failed to get/register API key")
                continue

        api_key = key_obj.api_key

        # Try to fetch transcript
        try:
            headers = {
                "Authorization": f"Bearer {api_key}"
            }

            params = {
                "video_url": video_id,
                "format": "json"
            }

            logger.info(f"[API] Fetching transcript for: {video_id}")
            logger.info(f"[API] Using key ID: {key_obj.id} (usage: {key_obj.usage_count})")

            response = requests.get(
                TRANSCRIPT_API_ENDPOINT,
                headers=headers,
                params=params,
                timeout=30
            )

            # Check status code before parsing
            status_code = response.status_code

            # Handle error status codes
            if status_code in [401, 402, 403, 429]:
                error_msg = {
                    401: "Unauthorized",
                    402: "Quota exceeded",
                    403: "Forbidden",
                    429: "Rate limit"
                }.get(status_code, f"HTTP {status_code}")

                logger.warning(f"[API] ⚠ {error_msg} (status {status_code}) for key {key_obj.id}")

                # Disable current key
                disable_key(db_session, key_obj.id, error_msg)

                # Try to register new account
                if attempt < max_attempts - 1:
                    logger.info(f"[AUTO-RETRY] Attempt {attempt + 1}/{max_attempts} - will get new key...")
                    time.sleep(1)  # Brief delay before retry
                    continue
                else:
                    logger.error(f"[AUTO-RETRY] Max attempts reached")
                    return None

            elif status_code == 404:
                logger.warning(f"[API] Transcript not found for: {video_id}")
                # Mark key as used even for 404
                mark_key_used(db_session, key_obj.id)
                return None

            elif status_code >= 400:
                logger.error(f"[API] HTTP {status_code}: {response.text[:200]}")
                return None

            # Parse successful response
            data = response.json()

            # Parse transcript
            if 'transcript' in data:
                transcript_data = data['transcript']

                transcript = []
                if isinstance(transcript_data, list):
                    for entry in transcript_data:
                        if isinstance(entry, dict):
                            transcript_entry = {
                                'start': float(entry.get('start', entry.get('offset', 0))),
                                'duration': float(entry.get('duration', entry.get('dur', 0))),
                                'text': entry.get('text', entry.get('content', ''))
                            }
                            transcript.append(transcript_entry)

                if transcript:
                    logger.info(f"[API] ✓ Got {len(transcript)} transcript entries")
                    # Mark key as used
                    mark_key_used(db_session, key_obj.id)
                    return transcript
                else:
                    logger.warning(f"[API] Empty transcript")
                    return None

            elif 'error' in data:
                logger.error(f"[API] Error: {data.get('error')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"[API] Request error: {str(e)}")
            # For network errors, try next attempt without disabling key
            if attempt < max_attempts - 1:
                logger.info(f"[RETRY] Network error, retrying (attempt {attempt + 1}/{max_attempts})...")
                time.sleep(2)
                continue
            return None

        except Exception as e:
            logger.error(f"[API] Unexpected error: {str(e)}")
            return None

    logger.error(f"[API] Failed after {max_attempts} attempts")
    return None
