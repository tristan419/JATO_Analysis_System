"""Upload Toolkit — reusable chunked upload, job engine, and file utilities.

Extracted from JATO monthly update service. Agnostic to any specific
data domain. Designed for scenarios that need:

- Chunked file upload with SHA-256 verification and resume
- Background job state machine (queued → running → success/failed)
- File hashing, validation, and atomic JSON persistence

Usage:

    from upload_toolkit.upload_engine import (
        create_upload_session,
        receive_chunk,
        complete_upload_session,
    )
    from upload_toolkit.job_engine import BaseJobRunner, append_log, utc_now
    from upload_toolkit.file_utils import sha256_hex_for_path, write_json
"""

__version__ = "0.1.0"
