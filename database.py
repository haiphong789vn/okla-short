"""Database models and operations"""

import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Video(Base):
    """Video model for storing video metadata"""
    __tablename__ = 'videos'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Store composite identifiers that include the YouTube ID plus generated filename.
    # The previous 50-character limit was too restrictive once filenames were appended
    # (e.g. "<youtube_id>_<short_filename>.mp4"), which triggered INSERT errors.
    video_id = Column(String(255), unique=True, nullable=False, index=True)  # From analysis JSON
    filename = Column(String(255), nullable=False)  # Local filename
    title = Column(String(500), nullable=False)  # Vietnamese title
    description = Column(Text)  # Video description
    duration = Column(Float)  # Video duration in seconds
    r2_url = Column(String(1000))  # URL on R2 storage
    r2_key = Column(String(500))  # Object key in R2
    tiktok_description = Column(Text)  # Generated TikTok description with tags
    uploaded_to_tiktok = Column(Boolean, default=False)  # Flag for TikTok upload status
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'id': self.id,
            'video_id': self.video_id,
            'filename': self.filename,
            'title': self.title,
            'description': self.description,
            'duration': self.duration,
            'r2_url': self.r2_url,
            'r2_key': self.r2_key,
            'tiktok_description': self.tiktok_description,
            'uploaded_to_tiktok': self.uploaded_to_tiktok,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class PlaylistVideo(Base):
    """Model for storing playlist video information and download status"""
    __tablename__ = 'playlist_videos'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Allow longer identifiers so we can safely append suffixes without hitting the
    # database length limit used in production deployments.
    video_id = Column(String(255), unique=True, nullable=False, index=True)  # YouTube video ID
    playlist_url = Column(String(500), nullable=False)  # Playlist URL source
    video_url = Column(String(500), nullable=False)  # Individual video URL
    title = Column(String(500))  # Video title (fetched after download)
    channel = Column(String(255))  # Channel name
    duration = Column(Float)  # Duration in seconds
    thumbnail_url = Column(String(1000))  # Thumbnail URL

    # Download status
    download_status = Column(String(50), default='pending')  # pending, downloading, completed, failed, processing, converted
    download_progress = Column(Float, default=0.0)  # 0-100%
    download_error = Column(Text)  # Error message if failed
    local_path = Column(String(1000))  # Path to downloaded file
    file_size = Column(Float)  # File size in bytes

    # Processing status
    transcript_status = Column(String(50), default='pending')  # pending, completed, failed
    analysis_status = Column(String(50), default='pending')  # pending, completed, failed
    conversion_status = Column(String(50), default='pending')  # pending, completed, failed
    shorts_count = Column(Integer, default=0)  # Number of shorts created

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'id': self.id,
            'video_id': self.video_id,
            'playlist_url': self.playlist_url,
            'video_url': self.video_url,
            'title': self.title,
            'channel': self.channel,
            'duration': self.duration,
            'thumbnail_url': self.thumbnail_url,
            'download_status': self.download_status,
            'download_progress': self.download_progress,
            'download_error': self.download_error,
            'local_path': self.local_path,
            'file_size': self.file_size,
            'transcript_status': self.transcript_status,
            'analysis_status': self.analysis_status,
            'conversion_status': self.conversion_status,
            'shorts_count': self.shorts_count,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class ApiKey(Base):
    """Model for storing API keys for various services (TranscriptAPI, Gemini, etc.)"""
    __tablename__ = 'api_keys'

    id = Column(Integer, primary_key=True, autoincrement=True)
    service = Column(String(50), nullable=False, index=True)  # 'transcript_api', 'gemini'
    api_key = Column(Text, nullable=False)  # The actual API key
    email = Column(String(255))  # Email used for registration (if applicable)
    password = Column(Text)  # Password (consider encryption in production)
    status = Column(String(20), default='active', index=True)  # active, disabled, expired
    usage_count = Column(Integer, default=0)  # Number of times used
    quota_remaining = Column(Integer)  # Remaining quota (if applicable)
    disabled_reason = Column(Text)  # Reason for disabling
    last_used = Column(DateTime)  # Last time this key was used
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'id': self.id,
            'service': self.service,
            'api_key': self.api_key[:20] + '...' if self.api_key else None,  # Mask key for security
            'email': self.email,
            'status': self.status,
            'usage_count': self.usage_count,
            'quota_remaining': self.quota_remaining,
            'disabled_reason': self.disabled_reason,
            'last_used': self.last_used.isoformat() if self.last_used else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class NoTranscriptVideo(Base):
    """Model for tracking videos that don't have transcripts"""
    __tablename__ = 'no_transcript_videos'

    video_id = Column(String(255), primary_key=True)  # YouTube video ID
    reason = Column(Text)  # Reason for no transcript
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'video_id': self.video_id,
            'reason': self.reason,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

# Database engine and session with resilient pooling
engine = create_engine(
    os.getenv('DATABASE_URL'),
    echo=False,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_timeout=60,
    pool_size=15,
    max_overflow=30,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def _split_sql_statements(sql: str):
    """Split a SQL script into executable statements.

    Supports PostgreSQL dollar-quoted functions and ignores comments so we can
    safely execute migration files that contain multiple statements.
    """

    statements = []
    buffer = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    dollar_quote_tag = None
    i = 0

    while i < len(sql):
        char = sql[i]
        next_two = sql[i : i + 2]

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                buffer.append(char)
            i += 1
            continue

        if in_block_comment:
            if next_two == "*/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if dollar_quote_tag:
            if sql.startswith(dollar_quote_tag, i):
                buffer.append(dollar_quote_tag)
                i += len(dollar_quote_tag)
                dollar_quote_tag = None
            else:
                buffer.append(char)
                i += 1
            continue

        if not in_single_quote and not in_double_quote:
            if next_two == "--":
                in_line_comment = True
                i += 2
                continue
            if next_two == "/*":
                in_block_comment = True
                i += 2
                continue

            if char == "$":
                # Detect PostgreSQL dollar-quoted string delimiters such as $$ or $tag$
                tag_end = i + 1
                while tag_end < len(sql) and sql[tag_end] not in {"$", "\n", "\r", "\t", " "}:
                    tag_end += 1
                if tag_end < len(sql) and sql[tag_end] == "$":
                    dollar_quote_tag = sql[i : tag_end + 1]
                    buffer.append(dollar_quote_tag)
                    i = tag_end + 1
                    continue

        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            buffer.append(char)
            i += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            buffer.append(char)
            i += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)

    filtered = []
    for stmt in statements:
        upper_stmt = stmt.strip().upper()
        if upper_stmt in {"COMMIT", "BEGIN", "START TRANSACTION"}:
            continue
        filtered.append(stmt)

    return filtered


def run_pending_migrations():
    """Run unapplied SQL migrations from the migrations/ directory."""

    migrations_dir = Path(__file__).resolve().parent / "migrations"
    if not migrations_dir.exists():
        return

    migration_files = sorted(p for p in migrations_dir.glob("*.sql"))
    if not migration_files:
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                filename VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

    with engine.begin() as connection:
        applied = {
            row[0]
            for row in connection.execute(
                text("SELECT filename FROM schema_migrations")
            )
        }

    for migration in migration_files:
        if migration.name in applied:
            continue

        statements = _split_sql_statements(migration.read_text())
        if not statements:
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO schema_migrations (filename) VALUES (:filename)"
                    ),
                    {"filename": migration.name},
                )
            continue

        with engine.begin() as connection:
            for statement in statements:
                connection.exec_driver_sql(statement)
            connection.execute(
                text("INSERT INTO schema_migrations (filename) VALUES (:filename)"),
                {"filename": migration.name},
            )


def init_db():
    """Initialize database - create all tables and run migrations."""
    Base.metadata.create_all(engine)
    run_pending_migrations()
    print("Database initialized successfully!")

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        return db
    except Exception as e:
        db.close()
        raise e


def remove_videos_with_null_duration(db):
    """Remove video records that have null duration"""
    return db.query(Video).filter(Video.duration == None).delete(synchronize_session=False)
