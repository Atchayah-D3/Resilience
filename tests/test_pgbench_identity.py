"""Tests for transaction identity encoding and deterministic UUID derivation."""

import hashlib
import uuid
from resilience_tests.execution.workload.identity import identity_text, identity_uuid


def test_identity_text():
    assert identity_text(0, 0, 0) == "L0-C0-S0"
    assert identity_text(1, 2, 3) == "L1-C2-S3"
    assert identity_text(12, 3, 4567) == "L12-C3-S4567"


def test_identity_uuid_deterministic():
    text = "L1-C2-S3"
    u1 = identity_uuid(text)
    u2 = identity_uuid(text)
    assert u1 == u2
    # Verify standard UUID format
    assert len(u1) == 36
    assert u1.count("-") == 4
    # Parse back to UUID object
    parsed = uuid.UUID(u1)
    assert str(parsed) == u1


def test_identity_uuid_matches_postgres_md5_cast():
    # PostgreSQL `md5(text)::uuid` formats the 32-character hexadecimal MD5 hash as a UUID
    # 8-4-4-4-12 hex digits.
    samples = [
        "L0-C0-S0",
        "L1-C2-S3",
        "L12-C3-S4567",
    ]
    for text in samples:
        expected_raw_hex = hashlib.md5(text.encode("utf-8")).hexdigest()
        expected_uuid_str = f"{expected_raw_hex[0:8]}-{expected_raw_hex[8:12]}-{expected_raw_hex[12:16]}-{expected_raw_hex[16:20]}-{expected_raw_hex[20:32]}"
        assert identity_uuid(text) == expected_uuid_str
