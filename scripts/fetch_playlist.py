"""
Fetch YouTube Playlist/Channel Videos Script
==============================================
This script fetches video metadata from YouTube playlists, channels, or individual videos
and stores them in the PostgreSQL database for later processing.

Usage:
    python scripts/fetch_playlist.py --url "https://youtube.com/playlist?list=..." --limit 50
    python scripts/fetch_playlist.py --url "https://youtube.com/@channel" --limit 100
    python scripts/fetch_playlist.py --url "https://youtube.com/watch?v=..."
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yt_dlp
from sqlalchemy.exc import IntegrityError
from database import get_db, init_db, PlaylistVideo

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# YT-DLP CONFIGURATION
# ============================================================================

def get_yt_dlp_options(cookies_file: Optional[str] = None) -> Dict:
    """
    Get yt-dlp options for extracting video information

    Args:
        cookies_file: Path to YouTube cookies file

    Returns:
        Dict of yt-dlp options
    """
    options = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True,  # Only extract metadata, don't download
        'skip_download': True,
        'ignoreerrors': True,  # Continue on errors
        'no_color': True,
    }

    # Add cookies if provided
    if cookies_file and os.path.exists(cookies_file):
        options['cookiefile'] = cookies_file
        logger.info(f"Using cookies from: {cookies_file}")
    else:
        logger.warning("No cookies file provided or file not found. Some videos may be inaccessible.")

    return options

# ============================================================================
# VIDEO EXTRACTION
# ============================================================================

def extract_video_info(url: str, cookies_file: Optional[str] = None) -> List[Dict]:
    """
    Extract video information from YouTube URL (playlist, channel, or single video)

    Args:
        url: YouTube URL (playlist, channel, or video)
        cookies_file: Path to YouTube cookies file

    Returns:
        List of video information dicts
    """
    try:
        logger.info(f"Extracting video information from: {url}")

        ydl_opts = get_yt_dlp_options(cookies_file)

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract information
            info = ydl.extract_info(url, download=False)

            if not info:
                logger.error(f"No information extracted from URL: {url}")
                return []

            videos = []

            # Check if it's a playlist or channel
            if 'entries' in info:
                logger.info(f"Found playlist/channel with {len(info['entries'])} entries")

                for entry in info['entries']:
                    if entry is None:
                        continue

                    # Extract video details
                    video_info = {
                        'video_id': entry.get('id'),
                        'url': f"https://youtube.com/watch?v={entry.get('id')}",
                        'title': entry.get('title'),
                        'channel': entry.get('uploader') or entry.get('channel'),
                        'duration': entry.get('duration'),  # in seconds
                        'thumbnail_url': entry.get('thumbnail'),
                        'playlist_url': url
                    }

                    # Only add if we have a valid video ID
                    if video_info['video_id']:
                        videos.append(video_info)
                    else:
                        logger.warning(f"Skipping entry without video ID: {entry.get('title', 'Unknown')}")

            # Single video
            else:
                video_info = {
                    'video_id': info.get('id'),
                    'url': url,
                    'title': info.get('title'),
                    'channel': info.get('uploader') or info.get('channel'),
                    'duration': info.get('duration'),
                    'thumbnail_url': info.get('thumbnail'),
                    'playlist_url': url
                }

                if video_info['video_id']:
                    videos.append(video_info)
                    logger.info(f"Extracted single video: {video_info['title']}")
                else:
                    logger.error("Could not extract video ID from URL")

            logger.info(f"Successfully extracted {len(videos)} videos")
            return videos

    except Exception as e:
        logger.error(f"Error extracting video info: {str(e)}")
        return []

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def save_videos_to_db(videos: List[Dict], db_session) -> Dict[str, int]:
    """
    Save videos to database

    Args:
        videos: List of video information dicts
        db_session: Database session

    Returns:
        Dict with statistics (inserted, skipped, failed)
    """
    stats = {
        'inserted': 0,
        'skipped': 0,
        'failed': 0
    }

    for video in videos:
        try:
            # Check if video already exists
            existing = db_session.query(PlaylistVideo).filter_by(
                video_id=video['video_id']
            ).first()

            if existing:
                logger.debug(f"Video already exists: {video['video_id']} - {video['title']}")
                stats['skipped'] += 1
                continue

            # Create new record
            playlist_video = PlaylistVideo(
                video_id=video['video_id'],
                playlist_url=video['playlist_url'],
                video_url=video['url'],
                title=video['title'],
                channel=video['channel'],
                duration=video['duration'],
                thumbnail_url=video['thumbnail_url'],
                download_status='pending',
                transcript_status='pending',
                analysis_status='pending',
                conversion_status='pending'
            )

            db_session.add(playlist_video)
            db_session.commit()

            logger.info(f"✓ Inserted: {video['video_id']} - {video['title']}")
            stats['inserted'] += 1

        except IntegrityError as e:
            db_session.rollback()
            logger.warning(f"Duplicate video (IntegrityError): {video['video_id']}")
            stats['skipped'] += 1

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error saving video {video.get('video_id', 'unknown')}: {str(e)}")
            stats['failed'] += 1

    return stats

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Fetch YouTube videos from playlist/channel and save to database'
    )
    parser.add_argument(
        '--url',
        required=True,
        help='YouTube URL (playlist, channel, or video)'
    )
    parser.add_argument(
        '--cookies',
        default='youtube_cookies.txt',
        help='Path to YouTube cookies file (default: youtube_cookies.txt)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of videos to fetch (default: unlimited)'
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("FETCH YOUTUBE PLAYLIST/CHANNEL VIDEOS")
    logger.info("=" * 80)
    logger.info(f"URL: {args.url}")
    logger.info(f"Cookies: {args.cookies}")
    logger.info(f"Limit: {args.limit or 'unlimited'}")
    logger.info("")

    # Extract videos
    videos = extract_video_info(args.url, args.cookies)

    if not videos:
        logger.error("No videos extracted. Exiting.")
        sys.exit(1)

    # Apply limit if specified
    if args.limit and len(videos) > args.limit:
        logger.info(f"Limiting to {args.limit} videos (out of {len(videos)})")
        videos = videos[:args.limit]

    # Save to database
    logger.info(f"\nSaving {len(videos)} videos to database...")

    # Initialize database (create tables if not exist)
    try:
        init_db()
        logger.info("✓ Database initialized")
    except Exception as e:
        logger.warning(f"Database init warning: {e}")

    db = get_db()

    try:
        stats = save_videos_to_db(videos, db)

        # Print summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total videos processed: {len(videos)}")
        logger.info(f"✓ Inserted: {stats['inserted']}")
        logger.info(f"⊘ Skipped (already exists): {stats['skipped']}")
        logger.info(f"✗ Failed: {stats['failed']}")
        logger.info("=" * 80)

        # Exit code based on results
        if stats['inserted'] > 0:
            logger.info(f"SUCCESS: {stats['inserted']} new videos added to database")
            sys.exit(0)
        elif stats['skipped'] > 0:
            logger.info("INFO: All videos already exist in database")
            sys.exit(0)
        else:
            logger.error("ERROR: No videos were saved")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

    finally:
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
