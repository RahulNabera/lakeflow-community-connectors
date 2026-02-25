# pylint: disable=too-many-lines
"""
Security tests for Azure Service Bus connector.

Tests cover:
- Upper bound validation on numeric parameters
- Entity name validation
- Error message redaction (no infra IDs leaked)
- Expanded _safe_error_message regex patterns
- Mixed-credentials warning
"""

import logging
import re

import pytest

from databricks.labs.community_connector.sources.azure_servicebus.azure_servicebus import (
    AzureServicebusLakeflowConnect as Connector,
    _safe_error_message,
    _redact_sensitive_values,
)


# =================================================================
# Task #6: Upper bound validation on numeric parameters
# =================================================================


class TestNumericUpperBounds:
    """Verify that numeric options reject values above max."""

    def _make_options(self, **overrides):
        """Build minimal valid options dict with overrides."""
        base = {
            "connection_string": (
                "Endpoint=sb://fake.servicebus.windows.net/;"
                "SharedAccessKeyName=x;SharedAccessKey=fakekey"
            ),
        }
        base.update(overrides)
        return base

    def test_max_message_count_above_limit_rejected(self):
        """max_message_count > 10000 should raise ValueError."""
        with pytest.raises(ValueError, match="max_message_count"):
            Connector._parse_int_option(
                "10001",
                option_name="max_message_count",
                default=100,
                minimum=1,
                maximum=10000,
            )

    def test_max_message_count_at_limit_accepted(self):
        """max_message_count == 10000 should be accepted."""
        result = Connector._parse_int_option(
            "10000",
            option_name="max_message_count",
            default=100,
            minimum=1,
            maximum=10000,
        )
        assert result == 10000

    def test_max_retries_above_limit_rejected(self):
        """max_retries > 10 should raise ValueError."""
        with pytest.raises(ValueError, match="max_retries"):
            Connector._parse_int_option(
                "11",
                option_name="max_retries",
                default=3,
                minimum=0,
                maximum=10,
            )

    def test_operation_timeout_above_limit_rejected(self):
        """operation_timeout > 300 should raise ValueError."""
        with pytest.raises(ValueError, match="operation_timeout"):
            Connector._parse_int_option(
                "301",
                option_name="operation_timeout",
                default=60,
                minimum=1,
                maximum=300,
            )

    def test_max_wait_time_above_limit_rejected(self):
        """max_wait_time > 300 should raise ValueError."""
        with pytest.raises(ValueError, match="max_wait_time"):
            Connector._parse_int_option(
                "301",
                option_name="max_wait_time",
                default=5,
                minimum=0,
                maximum=300,
            )

    def test_max_body_size_above_limit_rejected(self):
        """max_body_size > 1GB should raise ValueError."""
        gb = 1073741824
        with pytest.raises(ValueError, match="max_body_size"):
            Connector._parse_int_option(
                str(gb + 1),
                option_name="max_body_size",
                default=0,
                minimum=0,
                maximum=gb,
            )

    def test_default_returned_when_none(self):
        """None value should return default without hitting max."""
        result = Connector._parse_int_option(
            None,
            option_name="test_opt",
            default=42,
            minimum=0,
            maximum=100,
        )
        assert result == 42

    def test_below_minimum_still_rejected(self):
        """Values below minimum should still be rejected."""
        with pytest.raises(ValueError, match="test_opt"):
            Connector._parse_int_option(
                "-1",
                option_name="test_opt",
                default=5,
                minimum=0,
                maximum=100,
            )

    def test_no_maximum_allows_large_values(self):
        """When maximum is None, large values should pass."""
        result = Connector._parse_int_option(
            "999999",
            option_name="test_opt",
            default=5,
            minimum=0,
        )
        assert result == 999999


# =================================================================
# Task #7: Entity name validation
# =================================================================


class TestEntityNameValidation:
    """Verify entity name validation rejects bad inputs."""

    def test_valid_queue_name(self):
        """Normal queue names should pass validation."""
        Connector._validate_entity_name(
            "my-test-queue", "queue_name"
        )

    def test_valid_name_with_dots_slashes(self):
        """Names with dots and slashes are valid."""
        Connector._validate_entity_name(
            "my.topic/sub", "topic_name"
        )

    def test_empty_name_rejected(self):
        """Empty string should raise ValueError."""
        with pytest.raises(ValueError, match="queue_name"):
            Connector._validate_entity_name("", "queue_name")

    def test_none_name_rejected(self):
        """None should raise ValueError."""
        with pytest.raises(ValueError, match="topic_name"):
            Connector._validate_entity_name(None, "topic_name")

    def test_name_exceeding_260_chars_rejected(self):
        """Names longer than 260 chars should be rejected."""
        long_name = "a" * 261
        with pytest.raises(ValueError, match="260"):
            Connector._validate_entity_name(
                long_name, "queue_name"
            )

    def test_name_with_260_chars_accepted(self):
        """Exactly 260 chars should be accepted."""
        name = "a" * 260
        Connector._validate_entity_name(name, "queue_name")

    def test_session_id_exceeding_128_chars_rejected(self):
        """Session IDs longer than 128 chars should fail."""
        long_id = "s" * 129
        with pytest.raises(ValueError, match="128"):
            Connector._validate_session_id(long_id)

    def test_session_id_at_128_chars_accepted(self):
        """Session ID at exactly 128 chars should pass."""
        sid = "s" * 128
        Connector._validate_session_id(sid)

    def test_session_id_none_accepted(self):
        """None session_id is valid (means no session)."""
        Connector._validate_session_id(None)


# =================================================================
# Task #8: Expanded _safe_error_message patterns
# =================================================================


class TestSafeErrorMessagePatterns:
    """Verify _safe_error_message redacts all sensitive values."""

    def test_redacts_shared_access_key(self):
        """SharedAccessKey values should be redacted."""
        msg = "SharedAccessKey=mysecretkey123;"
        result = _redact_sensitive_values(msg)
        assert "mysecretkey123" not in result
        assert "SharedAccessKey=***" in result

    def test_redacts_client_secret(self):
        """client_secret values should be redacted."""
        msg = "client_secret=abc123secret"
        result = _redact_sensitive_values(msg)
        assert "abc123secret" not in result

    def test_redacts_password(self):
        """password values should be redacted."""
        msg = "password=hunter2"
        result = _redact_sensitive_values(msg)
        assert "hunter2" not in result

    def test_redacts_sig(self):
        """sig (SAS signature) values should be redacted."""
        msg = "sig=abc%2Fdef%3D&other=val"
        result = _redact_sensitive_values(msg)
        assert "abc%2Fdef%3D" not in result

    def test_redacts_jwt_token(self):
        """JWT tokens (eyJ...) should be redacted."""
        jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
            ".eyJzdWIiOiIxMjM0NTY3ODkwIn0"
            ".dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
        )
        msg = f"Token '{jwt}' is invalid"
        result = _redact_sensitive_values(msg)
        assert "eyJhbGciOiJIUzI1NiI" not in result

    def test_redacts_bearer_token(self):
        """Bearer token values should be redacted."""
        msg = "Authorization: Bearer abc123tokenvalue"
        result = _redact_sensitive_values(msg)
        assert "abc123tokenvalue" not in result

    def test_redacts_access_token(self):
        """access_token values should be redacted."""
        msg = "access_token=secrettoken123"
        result = _redact_sensitive_values(msg)
        assert "secrettoken123" not in result

    def test_safe_error_message_wraps_type(self):
        """_safe_error_message should include exception type."""
        exc = ValueError("SharedAccessKey=leak;")
        result = _safe_error_message(exc)
        assert result.startswith("ValueError:")
        assert "leak" not in result

    def test_safe_error_message_empty_returns_type(self):
        """Empty exception message should return type name."""
        exc = RuntimeError("")
        result = _safe_error_message(exc)
        assert result == "RuntimeError"


# =================================================================
# Task #9: Redact infra identifiers from error messages
# =================================================================


class TestInfraIdentifierRedaction:
    """Verify infrastructure IDs are not leaked in errors."""

    def test_service_principal_error_no_tenant_id(self):
        """Service principal error should not expose tenant_id."""
        tenant = "9f37a392-f0ae-4280-9796-f1864a10effc"
        client = "abc12345-def6-7890-ghij-klmnopqrstuv"
        # We test the error message format; the actual auth
        # would fail, so we check the message pattern.
        # The error message should NOT contain tenant/client IDs
        error_pattern = re.compile(
            r"azure_tenant_id is correct: " + re.escape(tenant)
        )
        # After our fix, this pattern should NOT appear
        # in any error message from the connector.
        # (Tested indirectly via integration or by
        # inspecting the source code.)
        # For now, verify the source doesn't contain
        # literal f-string interpolation of these values.
        import inspect
        source = inspect.getsource(Connector)
        assert "{self.azure_tenant_id}" not in source, (
            "Source should not interpolate azure_tenant_id "
            "into error messages"
        )
        assert "{self.azure_client_id}" not in source, (
            "Source should not interpolate azure_client_id "
            "into error messages"
        )

    def test_namespace_not_in_auth_error(self):
        """Auth errors should not expose namespace URL."""
        import inspect
        source = inspect.getsource(Connector)
        # Check that fully_qualified_namespace is not
        # interpolated into ValueError raise statements.
        # We allow it in logger.info for connection success
        # but NOT in error/raise paths.
        # Count occurrences in raise ValueError contexts
        raise_blocks = re.findall(
            r"raise ValueError\([^)]*"
            r"\{self\.fully_qualified_namespace\}",
            source,
        )
        assert len(raise_blocks) == 0, (
            f"Found {len(raise_blocks)} ValueError(s) "
            "that leak fully_qualified_namespace"
        )

    def test_validate_connection_log_no_namespace(self):
        """Success log should not expose full namespace."""
        import inspect
        source = inspect.getsource(
            Connector._validate_connection
        )
        assert "{self.fully_qualified_namespace}" not in source, (
            "_validate_connection should not log "
            "fully_qualified_namespace"
        )


# =================================================================
# Task #10: Mixed-credentials warning
# =================================================================


class TestMixedCredentialsWarning:
    """Verify warning when multiple credential sets provided."""

    def test_mixed_creds_logs_warning(self, caplog):
        """Both connection_string and namespace should warn."""
        options = {
            "connection_string": (
                "Endpoint=sb://fake.servicebus.windows.net/;"
                "SharedAccessKeyName=x;"
                "SharedAccessKey=fakekey"
            ),
            "fully_qualified_namespace": (
                "fake.servicebus.windows.net"
            ),
        }
        with caplog.at_level(logging.WARNING):
            try:
                Connector(options)
            except Exception:
                pass  # Auth will fail; we only check logs
        assert any(
            "multiple credential" in r.message.lower()
            or "mixed" in r.message.lower()
            for r in caplog.records
        ), (
            "Expected warning about mixed/multiple "
            "credential parameters"
        )
