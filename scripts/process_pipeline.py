"""
Video Processing Pipeline
===========================
Download videos, extract transcripts, analyze with AI, cut shorts, upload to R2.

This script:
1. Fetches pending videos from database
2. Downloads videos with yt-dlp
3. CHECKS video availability first (quick Gemini analysis)
4. Gets transcripts from TranscriptAPI (with auto-renewal)
5. Analyzes with Gemini AI to find interesting segments
6. Cuts video shorts with FFmpeg (9:16 vertical format)
7. Uploads shorts to Cloudflare R2
8. Updates database with results

Usage:
    python scripts/process_pipeline.py --limit 5 --cookies youtube_cookies.txt
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yt_dlp
import requests
from database import get_db, init_db, PlaylistVideo, Video, NoTranscriptVideo, ApiKey
from scripts.transcript_manager import fetch_transcript_with_retry, get_active_keys_count
from scripts.r2_uploader import create_r2_uploader_from_config

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ============================================================================
# GEMINI API CONFIGURATION
# ============================================================================

GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent"
MAX_GEMINI_RETRIES = 10
GEMINI_KEYS_JSON = os.getenv('GEMINI_KEYS_JSON', '[]')

# Load Gemini keys from environment
try:
    GEMINI_KEYS = json.loads(GEMINI_KEYS_JSON)
except:
    logger.warning("Failed to load Gemini keys from GEMINI_KEYS_JSON")
    GEMINI_KEYS = []

# ============================================================================
# HUGGING FACE API CONFIGURATION (BACKUP)
# ============================================================================

HUGGINGFACE_ENDPOINT = "https://router.huggingface.co/v1/chat/completions"
HUGGINGFACE_MODEL = "deepseek-ai/DeepSeek-V3.2-Exp"
HUGGINGFACE_TOKEN = os.getenv('HUGGINGFACE_API_TOKEN', '')

logger.info(f"Hugging Face API token configured: {'Yes' if HUGGINGFACE_TOKEN else 'No'}")

# ============================================================================
# VIDEO DOWNLOAD
# ============================================================================

def download_video(
    video_url: str,
    output_dir: str,
    cookies_file: Optional[str] = None
) -> Optional[str]:
    """
    Download video with yt-dlp

    Args:
        video_url: YouTube video URL
        output_dir: Output directory
        cookies_file: Path to cookies file

    Returns:
        Downloaded file path or None
    """
    try:
        os.makedirs(output_dir, exist_ok=True)

        ydl_opts = {
            'format': 'best[ext=mp4]/best',
            'outtmpl': os.path.join(output_dir, '%(id)s.%(ext)s'),
            'quiet': False,
            'no_warnings': False,
        }

        if cookies_file and os.path.exists(cookies_file):
            ydl_opts['cookiefile'] = cookies_file

        logger.info(f"Downloading video: {video_url}")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=True)
            video_id = info['id']
            ext = info['ext']
            output_path = os.path.join(output_dir, f"{video_id}.{ext}")

            if os.path.exists(output_path):
                file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                logger.info(f"✓ Downloaded: {output_path} ({file_size_mb:.2f} MB)")
                return output_path
            else:
                logger.error("Download completed but file not found")
                return None

    except Exception as e:
        logger.error(f"Error downloading video: {str(e)}")
        return None

# ============================================================================
# GEMINI AI ANALYSIS
# ============================================================================

class GeminiKeyManager:
    """Simple Gemini key manager with rotation"""

    def __init__(self, keys: List):
        import random
        # Handle both formats: list of strings or list of dicts
        self.keys = []
        for k in keys:
            if isinstance(k, str):
                # Plain string key - convert to dict format
                self.keys.append({'key': k, 'status': 'active'})
            elif isinstance(k, dict):
                # Dictionary format - only include active keys
                if k.get('status') == 'active':
                    self.keys.append(k)
            else:
                logger.warning(f"Skipping invalid key format: {type(k)}")
        
        self.current_index = random.randint(0, len(self.keys) - 1) if self.keys else 0
        logger.info(f"Initialized with {len(self.keys)} keys, starting at index {self.current_index}")

    def get_current_key(self) -> Optional[str]:
        if not self.keys:
            return None
        if self.current_index >= len(self.keys):
            self.current_index = 0
        key = self.keys[self.current_index].get('key')
        logger.debug(f"Using key index {self.current_index}/{len(self.keys)}")
        return key

    def rotate_key(self):
        if self.keys:
            old_index = self.current_index
            self.current_index = (self.current_index + 1) % len(self.keys)
            logger.info(f"Rotated key: {old_index} → {self.current_index} (total: {len(self.keys)} keys)")

    def disable_current_key(self, reason: str):
        if self.keys and self.current_index < len(self.keys):
            disabled_index = self.current_index
            self.keys[self.current_index]['status'] = 'disabled'
            logger.warning(f"Disabled key index {disabled_index}: {reason}")
            # Remove from list
            self.keys.pop(self.current_index)
            if self.current_index >= len(self.keys):
                self.current_index = 0
            logger.info(f"Remaining active keys: {len(self.keys)}")

    def get_active_count(self) -> int:
        return len(self.keys)

def call_huggingface_api(prompt: str) -> Optional[Dict]:
    """
    Call Hugging Face API as backup when Gemini fails with 503

    Args:
        prompt: Prompt text

    Returns:
        API response in Gemini-compatible format or None
    """
    if not HUGGINGFACE_TOKEN:
        logger.error("Hugging Face API token not configured")
        return None

    try:
        headers = {
            'Authorization': f'Bearer {HUGGINGFACE_TOKEN}',
            'Content-Type': 'application/json'
        }

        payload = {
            'messages': [
                {'role': 'user', 'content': prompt}
            ],
            'model': HUGGINGFACE_MODEL
        }

        logger.info(f"[BACKUP] Calling Hugging Face API (model: {HUGGINGFACE_MODEL})...")

        response = requests.post(
            HUGGINGFACE_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=90
        )

        response.raise_for_status()
        data = response.json()

        # Convert Hugging Face response to Gemini-compatible format
        if 'choices' in data and len(data['choices']) > 0:
            content = data['choices'][0]['message']['content']

            # Convert to Gemini format
            gemini_format = {
                'candidates': [{
                    'content': {
                        'parts': [{'text': content}]
                    }
                }]
            }

            logger.info("[BACKUP] ✓ Hugging Face API call successful")
            return gemini_format
        else:
            logger.error(f"[BACKUP] Invalid Hugging Face response format: {data}")
            return None

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else None
        logger.error(f"[BACKUP] Hugging Face HTTP error {status_code}: {e}")
        return None
    except Exception as e:
        logger.error(f"[BACKUP] Hugging Face API error: {str(e)}")
        return None

def call_gemini_api(
    prompt: str,
    key_manager: GeminiKeyManager,
    retry_count: int = 0
) -> Optional[Dict]:
    """
    Call Gemini API with key rotation

    Args:
        prompt: Prompt text
        key_manager: Key manager instance
        retry_count: Current retry count

    Returns:
        API response or None
    """
    if retry_count >= MAX_GEMINI_RETRIES:
        logger.warning("Max Gemini retries reached, trying Hugging Face API as backup...")
        hf_response = call_huggingface_api(prompt)
        if hf_response:
            logger.info("✓ Successfully switched to Hugging Face API backup")
            return hf_response
        else:
            logger.error("Hugging Face backup also failed")
            return None

    api_key = key_manager.get_current_key()
    if not api_key:
        logger.warning("No active Gemini keys available, trying Hugging Face API as backup...")
        hf_response = call_huggingface_api(prompt)
        if hf_response:
            logger.info("✓ Successfully switched to Hugging Face API backup")
            return hf_response
        else:
            logger.error("Hugging Face backup also failed")
            return None

    # Log first 20 characters of API key for debugging
    logger.info(f"Using API key (first 20 chars): {api_key[:20]}...")

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key
    }

    # Simplified payload matching curl example (without generationConfig to avoid 400 errors)
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }

    try:
        logger.info(f"Calling Gemini API - Key {key_manager.current_index + 1}/{key_manager.get_active_count()} (retry {retry_count}/{MAX_GEMINI_RETRIES})")
        
        # Log request payload for debugging (first 500 chars of prompt)
        logger.debug(f"Request payload: {json.dumps(payload, ensure_ascii=False)[:500]}...")
        
        response = requests.post(GEMINI_ENDPOINT, headers=headers, json=payload, timeout=120)
        response.raise_for_status()

        return response.json()

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else None
        
        # Log detailed error response for debugging
        if e.response:
            try:
                error_body = e.response.json()
                logger.error(f"Gemini API error response: {json.dumps(error_body, indent=2)}")
            except:
                logger.error(f"Gemini API error response (raw): {e.response.text[:500]}")

        if status_code in [400, 403, 429, 503]:
            error_msg = f"HTTP {status_code}"
            logger.warning(f"Gemini API error: {error_msg} - Rotating to next key (retry {retry_count + 1}/{MAX_GEMINI_RETRIES})")
            
            # For 503 errors, just rotate key without disabling
            if status_code == 503:
                if key_manager.get_active_count() > 0:
                    key_manager.rotate_key()
                    time.sleep(2)  # Wait 2 seconds before retry
                    return call_gemini_api(prompt, key_manager, retry_count + 1)
                else:
                    # All Gemini keys exhausted with 503 error, fallback to Hugging Face
                    logger.warning("All Gemini keys exhausted with 503 error, trying Hugging Face API as backup...")
                    hf_response = call_huggingface_api(prompt)
                    if hf_response:
                        logger.info("✓ Successfully switched to Hugging Face API backup")
                        return hf_response
                    else:
                        logger.error("Hugging Face backup also failed")
                        return None
            else:
                # For other errors (400, 403, 429), disable the key
                key_manager.disable_current_key(error_msg)
                
                if key_manager.get_active_count() > 0:
                    key_manager.rotate_key()
                    time.sleep(1)
                    return call_gemini_api(prompt, key_manager, retry_count + 1)
                else:
                    logger.error("All Gemini keys exhausted")
                    return None
        else:
            logger.error(f"Gemini HTTP error {status_code}: {e}")
            return None

    except Exception as e:
        logger.error(f"Gemini API error: {str(e)} - Retrying with next key ({retry_count + 1}/{MAX_GEMINI_RETRIES})")
        
        # On any other exception, rotate and retry
        if key_manager.get_active_count() > 0:
            key_manager.rotate_key()
            time.sleep(1)
            return call_gemini_api(prompt, key_manager, retry_count + 1)
        else:
            logger.error("All Gemini keys exhausted")
            return None

def check_video_availability(video_id: str, key_manager: GeminiKeyManager) -> bool:
    """
    Quick check if video/transcript is available using Gemini
    
    Args:
        video_id: YouTube video ID
        key_manager: Gemini key manager
        
    Returns:
        True if video seems available, False otherwise
    """
    prompt = f"""Check if this YouTube video exists and is accessible: https://www.youtube.com/watch?v={video_id}

Return ONLY a JSON response in this format (no markdown, no code blocks):
{{
  "available": true,
  "reason": "Video is accessible"
}}

Or if not available:
{{
  "available": false,
  "reason": "Video is private/deleted/unavailable"
}}"""

    response = call_gemini_api(prompt, key_manager)
    if not response:
        logger.warning("Could not check video availability, assuming available")
        return True
    
    try:
        text = response['candidates'][0]['content']['parts'][0]['text']
        text = text.strip()
        if text.startswith('```json'):
            text = text[7:]
        elif text.startswith('```'):
            text = text[3:]
        if text.endswith('```'):
            text = text[:-3]
        text = text.strip()
        
        data = json.loads(text)
        is_available = data.get('available', True)
        reason = data.get('reason', 'Unknown')
        
        if not is_available:
            logger.warning(f"Video not available: {reason}")
        
        return is_available
        
    except Exception as e:
        logger.warning(f"Error parsing availability check: {e}, assuming available")
        return True

def parse_gemini_response(response: Dict) -> Optional[List[Dict]]:
    """Parse Gemini response to extract segments"""
    try:
        text = response['candidates'][0]['content']['parts'][0]['text']

        # Remove markdown code blocks
        text = text.strip()
        if text.startswith('```json'):
            text = text[7:]
        elif text.startswith('```'):
            text = text[3:]
        if text.endswith('```'):
            text = text[:-3]
        text = text.strip()

        # Parse JSON
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            logger.error(f"Response text (first 500 chars): {text[:500]}")
            return None

        if 'segments' not in data:
            logger.error("No 'segments' in response")
            return None

        segments = data['segments']

        # Validate segments
        valid_segments = []
        for i, seg in enumerate(segments, 1):
            if all(k in seg for k in ['start', 'end', 'title', 'description']):
                duration = seg['end'] - seg['start']
                if duration >= 120:  # At least 2 minutes
                    valid_segments.append(seg)
                    logger.info(f"Segment {i}: {seg['title']} ({duration}s)")
                else:
                    logger.warning(f"Segment {i} too short: {duration}s")

        return valid_segments if valid_segments else None

    except KeyError as e:
        logger.error(f"Missing key in response structure: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing Gemini response: {str(e)}")
        return None

def analyze_transcript(transcript: List[Dict], key_manager: GeminiKeyManager, max_retries: int = 3) -> Optional[List[Dict]]:
    """Analyze transcript with Gemini to find interesting segments"""
    # Format transcript
    transcript_text = "\n".join([
        f"[{e['start']:.1f}s - {e['start'] + e['duration']:.1f}s] {e['text']}"
        for e in transcript
    ])

    prompt = f"""Phân tích bản transcript video YouTube này và xác định 3 đến 5 đoạn video thú vị/hấp dẫn nhất phù hợp làm nội dung ngắn cho TikTok (mỗi đoạn 2-5 phút).

Đối với mỗi đoạn, hãy cung cấp (BẮT BUỘC TIẾNG VIỆT):
- Thời gian bắt đầu (tính bằng giây)
- Thời gian kết thúc (tính bằng giây)
- Tiêu đề hấp dẫn, ngắn gọn cho clip (tối đa 50 ký tự, phù hợp làm tên file - không có ký tự đặc biệt)
- Mô tả cho video ngắn (BẰNG TIẾNG VIỆT)

Transcript:
{transcript_text}

Trả về response CHỈ theo định dạng JSON chính xác này (không có markdown, không có code blocks):
{{
  "segments": [
    {{
      "start": 60,
      "end": 240,
      "title": "Cau_Chuyen_Cam_Dong",
      "description": "Mô tả chi tiết bằng tiếng Việt..."
    }}
  ]
}}

LƯU Ý:
- Trả về 3-5 segments
- Mỗi segment 2-5 phút (120-300 giây)
- Title không dấu (để làm tên file)
- Description bằng tiếng Việt
- QUAN TRỌNG: Đảm bảo JSON hợp lệ, không có chuỗi bị cắt ngang"""

    # Retry logic for parse errors
    for attempt in range(max_retries):
        response = call_gemini_api(prompt, key_manager)
        if not response:
            logger.warning(f"Gemini API call failed (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            else:
                # All Gemini attempts failed, try Hugging Face as fallback
                logger.warning("All Gemini retry attempts exhausted, trying Hugging Face API as backup...")
                hf_response = call_huggingface_api(prompt)
                if hf_response:
                    logger.info("✓ Successfully switched to Hugging Face API backup")
                    segments = parse_gemini_response(hf_response)
                    if segments:
                        return segments
                    else:
                        logger.error("Failed to parse Hugging Face response")
                        return None
                else:
                    logger.error("Hugging Face backup also failed")
                    return None

        segments = parse_gemini_response(response)
        
        if segments:
            return segments
        
        # If parse failed, retry with different key
        logger.warning(f"Parse failed, retrying with next key (attempt {attempt + 1}/{max_retries})")
        if attempt < max_retries - 1:
            key_manager.rotate_key()
            time.sleep(2)
    
    logger.error(f"Failed to analyze transcript after {max_retries} attempts")
    return None

# ============================================================================
# VIDEO CUTTING
# ============================================================================

def sanitize_filename(title: str, max_length: int = 50) -> str:
    """Sanitize filename"""
    import re
    title = re.sub(r'[<>:"/\\|?*]', '', title)
    title = title.replace(' ', '_')
    title = re.sub(r'_+', '_', title)
    return title[:max_length].strip('. _') or "clip"

def cut_video_clip(
    input_video: str,
    output_path: str,
    start_time: float,
    end_time: float
) -> bool:
    """Cut video clip with FFmpeg (9:16 vertical format)"""
    try:
        cmd = [
            'ffmpeg',
            '-i', input_video,
            '-ss', str(start_time),
            '-to', str(end_time),
            '-vf', 'crop=ih*9/16:ih,scale=1080:1920',
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-crf', '23',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-movflags', '+faststart',
            '-y',
            output_path
        ]

        logger.info(f"Cutting clip: {start_time}s to {end_time}s")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logger.info(f"✓ Created clip: {output_path}")
            return True
        else:
            logger.error(f"FFmpeg error: {result.stderr[:500]}")
            return False

    except Exception as e:
        logger.error(f"Error cutting video: {str(e)}")
        return False

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_single_video(
    playlist_video: PlaylistVideo,
    db_session,
    temp_dir: str,
    cookies_file: Optional[str],
    key_manager: GeminiKeyManager,
    r2_uploader
) -> Dict:
    """
    Process a single video through the entire pipeline

    Returns:
        Stats dict
    """
    stats = {
        'success': False,
        'shorts_created': 0,
        'error': None,
        'skipped': False
    }

    video_id = playlist_video.video_id
    video_url = playlist_video.video_url

    logger.info(f"\n{'='*80}")
    logger.info(f"Processing: {video_id} - {playlist_video.title}")
    logger.info(f"{'='*80}")

    try:
        # Step 1: Fetch transcript FIRST (fail fast if video unavailable)
        logger.info("[STEP 1] Fetching transcript...")
        playlist_video.transcript_status = 'fetching'
        db_session.commit()
        
        try:
            transcript = fetch_transcript_with_retry(video_id, db_session, max_attempts=3)
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                logger.warning(f"Transcript API returned 404, marking as skipped")
                stats['error'] = "Transcript not found (404)"
                stats['skipped'] = True
                
                playlist_video.download_status = 'skipped'
                playlist_video.transcript_status = 'skipped'
                playlist_video.analysis_status = 'skipped'
                playlist_video.conversion_status = 'skipped'
                playlist_video.download_error = "Transcript API 404 - video unavailable"
                
                # Add to no_transcript_videos
                no_transcript = NoTranscriptVideo(
                    video_id=video_id,
                    reason="Transcript API 404 error"
                )
                db_session.merge(no_transcript)
                db_session.commit()
                
                return stats
            else:
                raise

        if not transcript:
            stats['error'] = "No transcript"
            stats['skipped'] = True
            playlist_video.download_status = 'skipped'
            playlist_video.transcript_status = 'skipped'
            playlist_video.analysis_status = 'skipped'
            playlist_video.conversion_status = 'skipped'

            # Add to no_transcript_videos
            no_transcript = NoTranscriptVideo(
                video_id=video_id,
                reason="Transcript not available from API"
            )
            db_session.merge(no_transcript)
            db_session.commit()

            return stats

        playlist_video.transcript_status = 'completed'
        db_session.commit()
        logger.info(f"✓ Transcript fetched: {len(transcript)} entries")

        # Step 2: Download video (only after transcript confirmed available)
        logger.info("[STEP 2] Downloading video...")
        playlist_video.download_status = 'downloading'
        db_session.commit()

        video_path = download_video(video_url, temp_dir, cookies_file)

        if not video_path:
            stats['error'] = "Download failed"
            playlist_video.download_status = 'failed'
            playlist_video.download_error = "Download failed"
            db_session.commit()
            return stats

        playlist_video.download_status = 'completed'
        playlist_video.local_path = video_path
        playlist_video.file_size = os.path.getsize(video_path)
        db_session.commit()

        # Step 3: Check video availability (quick Gemini check)
        logger.info("[STEP 3] Checking video availability...")
        is_available = check_video_availability(video_id, key_manager)
        
        if not is_available:
            logger.warning("Video appears unavailable, skipping...")
            stats['error'] = "Video unavailable"
            stats['skipped'] = True
            playlist_video.download_status = 'skipped'
            playlist_video.analysis_status = 'skipped'
            playlist_video.conversion_status = 'skipped'
            playlist_video.download_error = "Video unavailable or inaccessible"
            db_session.commit()
            
            # Cleanup
            if os.path.exists(video_path):
                os.remove(video_path)
            
            return stats

        # Step 4: Analyze with Gemini
        logger.info("[STEP 4] Analyzing with Gemini AI...")
        segments = analyze_transcript(transcript, key_manager)

        if not segments:
            stats['error'] = "AI analysis failed"
            playlist_video.analysis_status = 'failed'
            db_session.commit()
            return stats

        playlist_video.analysis_status = 'completed'
        db_session.commit()

        logger.info(f"Found {len(segments)} interesting segments")

        # Step 5: Cut videos
        logger.info("[STEP 5] Cutting video clips...")
        shorts_dir = os.path.join(temp_dir, 'shorts')
        os.makedirs(shorts_dir, exist_ok=True)

        created_shorts = []

        for i, seg in enumerate(segments, 1):
            title = sanitize_filename(seg['title'])
            output_filename = f"{video_id}_{i}_{title}.mp4"
            output_path = os.path.join(shorts_dir, output_filename)

            success = cut_video_clip(
                video_path,
                output_path,
                seg['start'],
                seg['end']
            )

            if success:
                created_shorts.append({
                    'filename': output_filename,
                    'path': output_path,
                    'segment': seg
                })

        if not created_shorts:
            stats['error'] = "No shorts created"
            playlist_video.conversion_status = 'failed'
            db_session.commit()
            return stats

        playlist_video.conversion_status = 'completed'
        playlist_video.shorts_count = len(created_shorts)
        db_session.commit()

        # Step 6: Upload to R2
        logger.info("[STEP 6] Uploading to R2...")

        for short in created_shorts:
            r2_key = f"shorts/{video_id}/{short['filename']}"
            result = r2_uploader.upload_file(short['path'], r2_key)

            if result:
                r2_key, public_url = result

                # Save to database
                video_record = Video(
                    video_id=f"{video_id}_{short['filename']}",
                    filename=short['filename'],
                    title=short['segment']['title'],
                    description=short['segment']['description'],
                    duration=short['segment']['end'] - short['segment']['start'],
                    r2_url=public_url,
                    r2_key=r2_key,
                    tiktok_description=short['segment']['description'][:500],
                    uploaded_to_tiktok=False
                )

                db_session.add(video_record)
                stats['shorts_created'] += 1

        db_session.commit()

        # Cleanup
        if os.path.exists(video_path):
            os.remove(video_path)
            logger.info(f"✓ Deleted downloaded video")

        stats['success'] = True
        logger.info(f"✓ Processing complete: {stats['shorts_created']} shorts created")

        return stats

    except Exception as e:
        logger.error(f"Error processing video: {str(e)}")
        stats['error'] = str(e)
        return stats

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Process videos to shorts')
    parser.add_argument('--limit', type=int, default=5, help='Max videos to process')
    parser.add_argument('--cookies', default='youtube_cookies.txt', help='YouTube cookies file')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("VIDEO PROCESSING PIPELINE")
    logger.info("=" * 80)

    # Initialize database (create tables if not exist)
    try:
        init_db()
        logger.info("✓ Database initialized")
    except Exception as e:
        logger.warning(f"Database init warning: {e}")

    # Initialize
    db = get_db()
    key_manager = GeminiKeyManager(GEMINI_KEYS)
    r2_uploader = create_r2_uploader_from_config()

    logger.info(f"Gemini keys: {key_manager.get_active_count()}")
    logger.info(f"TranscriptAPI keys: {get_active_keys_count(db)}")

    # Get pending videos
    pending_videos = db.query(PlaylistVideo).filter_by(
        download_status='pending'
    ).limit(args.limit).all()

    if not pending_videos:
        logger.info("No pending videos to process")
        db.close()
        return

    logger.info(f"Found {len(pending_videos)} pending videos")

    # Process each video
    total_shorts = 0
    skipped_count = 0

    for i, pv in enumerate(pending_videos, 1):
        logger.info(f"\n[{i}/{len(pending_videos)}] Processing video...")

        with tempfile.TemporaryDirectory() as temp_dir:
            stats = process_single_video(
                pv, db, temp_dir, args.cookies, key_manager, r2_uploader
            )
            total_shorts += stats['shorts_created']
            if stats.get('skipped'):
                skipped_count += 1

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Videos processed: {len(pending_videos)}")
    logger.info(f"Videos skipped: {skipped_count}")
    logger.info(f"Total shorts created: {total_shorts}")
    logger.info("=" * 80)

    db.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\nFatal error: {str(e)}")
        sys.exit(1)
