"""
Cloudflare R2 Uploader
=======================
Upload video files to Cloudflare R2 storage.

Features:
- Upload files to R2 bucket
- Generate public URLs
- Progress tracking
- Retry logic
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, Tuple
import boto3
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

# ============================================================================
# R2 CLIENT
# ============================================================================

class R2Uploader:
    """Cloudflare R2 uploader class"""

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: str,
        bucket_name: str,
        public_url: str
    ):
        """
        Initialize R2 uploader

        Args:
            access_key_id: R2 access key ID
            secret_access_key: R2 secret access key
            endpoint_url: R2 endpoint URL
            bucket_name: R2 bucket name
            public_url: Public URL for accessing files
        """
        self.bucket_name = bucket_name
        self.public_url = public_url.rstrip('/')

        # Create S3 client (R2 is S3-compatible)
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name='auto'  # R2 uses 'auto' region
        )

        logger.info(f"R2 uploader initialized for bucket: {bucket_name}")

    def upload_file(
        self,
        local_path: str,
        r2_key: str,
        content_type: str = 'video/mp4',
        max_retries: int = 3
    ) -> Optional[Tuple[str, str]]:
        """
        Upload file to R2

        Args:
            local_path: Local file path
            r2_key: Object key in R2 (e.g., 'shorts/video1.mp4')
            content_type: Content type (default: video/mp4)
            max_retries: Maximum upload retries

        Returns:
            Tuple of (r2_key, public_url) or None if failed
        """
        if not os.path.exists(local_path):
            logger.error(f"File not found: {local_path}")
            return None

        file_size = os.path.getsize(local_path)
        file_size_mb = file_size / (1024 * 1024)

        logger.info(f"Uploading to R2: {local_path} ({file_size_mb:.2f} MB)")
        logger.info(f"R2 Key: {r2_key}")

        for attempt in range(max_retries):
            try:
                # Upload file
                with open(local_path, 'rb') as f:
                    self.client.put_object(
                        Bucket=self.bucket_name,
                        Key=r2_key,
                        Body=f,
                        ContentType=content_type,
                        # Optional: Add metadata
                        Metadata={
                            'original_filename': os.path.basename(local_path),
                            'uploaded_at': str(int(os.path.getmtime(local_path)))
                        }
                    )

                # Generate public URL
                public_url = f"{self.public_url}/{r2_key}"

                logger.info(f"✓ Upload successful!")
                logger.info(f"Public URL: {public_url}")

                return (r2_key, public_url)

            except (ClientError, BotoCoreError) as e:
                logger.error(f"Upload error (attempt {attempt + 1}/{max_retries}): {str(e)}")

                if attempt < max_retries - 1:
                    logger.info("Retrying...")
                    continue
                else:
                    logger.error("Max retries reached, upload failed")
                    return None

            except Exception as e:
                logger.error(f"Unexpected error during upload: {str(e)}")
                return None

        return None

    def delete_file(self, r2_key: str) -> bool:
        """
        Delete file from R2

        Args:
            r2_key: Object key in R2

        Returns:
            True if successful
        """
        try:
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=r2_key
            )
            logger.info(f"✓ Deleted from R2: {r2_key}")
            return True

        except Exception as e:
            logger.error(f"Error deleting from R2: {str(e)}")
            return False

    def file_exists(self, r2_key: str) -> bool:
        """
        Check if file exists in R2

        Args:
            r2_key: Object key in R2

        Returns:
            True if file exists
        """
        try:
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=r2_key
            )
            return True

        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking file existence: {str(e)}")
                return False

    def list_files(self, prefix: str = '') -> list:
        """
        List files in R2 bucket with optional prefix

        Args:
            prefix: Prefix to filter files (e.g., 'shorts/')

        Returns:
            List of object keys
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []

        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_r2_uploader_from_config(config_module=None) -> R2Uploader:
    """
    Create R2 uploader from config module or environment variables

    Args:
        config_module: Config module (default: imports config.py)

    Returns:
        R2Uploader instance
    """
    if config_module is None:
        # Try to import config
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            import config
            config_module = config
        except ImportError:
            config_module = None

    # Get config from module or environment
    if config_module:
        access_key_id = getattr(config_module, 'R2_SHORTS_ACCESS_KEY_ID', None)
        secret_access_key = getattr(config_module, 'R2_SHORTS_SECRET_ACCESS_KEY', None)
        endpoint_url = getattr(config_module, 'R2_SHORTS_ENDPOINT', None)
        bucket_name = getattr(config_module, 'R2_SHORTS_BUCKET', None)
        public_url = getattr(config_module, 'R2_SHORTS_PUBLIC_URL', None)
    else:
        access_key_id = os.getenv('R2_ACCESS_KEY_ID')
        secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
        endpoint_url = os.getenv('R2_ENDPOINT_URL')
        bucket_name = os.getenv('R2_BUCKET_NAME')
        public_url = os.getenv('R2_PUBLIC_URL')

    # Validate
    if not all([access_key_id, secret_access_key, endpoint_url, bucket_name, public_url]):
        raise ValueError("Missing R2 configuration. Check config.py or environment variables.")

    return R2Uploader(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        endpoint_url=endpoint_url,
        bucket_name=bucket_name,
        public_url=public_url
    )

def upload_shorts_batch(
    uploader: R2Uploader,
    shorts_dir: str,
    video_id: str
) -> list:
    """
    Upload all shorts from a directory to R2

    Args:
        uploader: R2Uploader instance
        shorts_dir: Directory containing shorts
        video_id: Original video ID (for organizing in R2)

    Returns:
        List of (filename, r2_key, public_url) tuples
    """
    if not os.path.exists(shorts_dir):
        logger.error(f"Shorts directory not found: {shorts_dir}")
        return []

    uploaded_shorts = []

    for filename in os.listdir(shorts_dir):
        if not filename.endswith('.mp4'):
            continue

        local_path = os.path.join(shorts_dir, filename)

        # Create R2 key: shorts/{video_id}/{filename}
        r2_key = f"shorts/{video_id}/{filename}"

        # Upload
        result = uploader.upload_file(local_path, r2_key)

        if result:
            r2_key, public_url = result
            uploaded_shorts.append((filename, r2_key, public_url))
        else:
            logger.error(f"Failed to upload: {filename}")

    return uploaded_shorts
