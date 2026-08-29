from __future__ import annotations

import hashlib


def entity_bucket(entity_id: str, bucket_count: int) -> int:
    """Stable bucket independent of Python's randomized process hash."""

    if not isinstance(entity_id, str):
        raise TypeError("entity identifiers must be strings")
    if bucket_count < 1:
        raise ValueError("bucket_count must be positive")
    digest = hashlib.blake2b(entity_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % bucket_count
