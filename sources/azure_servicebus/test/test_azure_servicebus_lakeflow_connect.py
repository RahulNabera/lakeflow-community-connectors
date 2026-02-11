"""
Tests for Azure Service Bus Lakeflow Community Connector

This file integrates with the generic LakeflowConnect test suite AND provides
comprehensive connector-specific tests.

Run generic tests (from repo root):
    pytest sources/azure_servicebus/test/
      test_azure_servicebus_lakeflow_connect.py
      ::test_azure_servicebus_connector -v

Run all tests:
    pytest sources/azure_servicebus/test/
      test_azure_servicebus_lakeflow_connect.py -v

Prerequisites:
    1. Copy configs/dev_config.example.json to configs/dev_config.json
    2. Copy configs/dev_table_config.example.json to configs/dev_table_config.json
    3. Fill in your Azure Service Bus credentials
    4. Ensure you have at least one queue, topic, and subscription in your namespace
       (use setup_test_resources.py to create them)
"""

import json
from pathlib import Path

import pytest
from pyspark.sql.types import StructType

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.azure_servicebus.azure_servicebus import LakeflowConnect


# =========================================================================
# Generic Test Suite Integration
# =========================================================================


def test_azure_servicebus_connector():
    """Test the Azure Service Bus connector using the shared LakeflowConnect test suite."""
    # Inject the Azure Service Bus LakeflowConnect class into the shared test_suite
    # namespace so that LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration (e.g. connection_string)
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(config, table_config)

    # Run all standard LakeflowConnect tests for this connector
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, "
        f"{report.error_tests} errors"
    )


# =========================================================================
# Helper: Load config for custom tests
# =========================================================================


def _load_custom_config() -> dict:
    """Load test configuration from dev_config.json."""
    config_path = Path(__file__).parent.parent / "configs" / "dev_config.json"
    if not config_path.exists():
        pytest.skip(
            "dev_config.json not found. Copy dev_config.example.json to dev_config.json "
            "and fill in your credentials."
        )
    with open(config_path) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def config():
    """Fixture to load configuration."""
    return _load_custom_config()


@pytest.fixture(scope="module")
def connector(config):
    """Fixture to create a connector instance."""
    options = {
        "connection_string": config.get("connection_string"),
        "fully_qualified_namespace": config.get("fully_qualified_namespace"),
        "credential_type": config.get("credential_type", "connection_string"),
    }
    # Remove None values
    options = {k: v for k, v in options.items() if v is not None}

    conn = LakeflowConnect(options)
    yield conn
    conn.close()


# =========================================================================
# Custom Tests: Interface Validation
# =========================================================================


class TestLakeflowConnectInterface:
    """Test the LakeflowConnect interface methods."""

    def test_list_tables(self, connector):
        """Test that list_tables returns expected tables."""
        tables = connector.list_tables()

        assert isinstance(tables, list)
        assert len(tables) == 6
        assert "queues" in tables
        assert "topics" in tables
        assert "subscriptions" in tables
        assert "queue_messages" in tables
        assert "subscription_messages" in tables
        assert "dead_letter_messages" in tables

    def test_get_table_schema_queues(self, connector):
        """Test schema for queues table."""
        schema = connector.get_table_schema("queues", {})

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "name" in field_names
        assert "max_size_in_megabytes" in field_names
        assert "message_count" in field_names

    def test_get_table_schema_topics(self, connector):
        """Test schema for topics table."""
        schema = connector.get_table_schema("topics", {})

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "name" in field_names
        assert "subscription_count" in field_names

    def test_get_table_schema_subscriptions(self, connector):
        """Test schema for subscriptions table."""
        schema = connector.get_table_schema("subscriptions", {})

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "name" in field_names
        assert "topic_name" in field_names

    def test_get_table_schema_queue_messages(self, connector):
        """Test schema for queue_messages table."""
        schema = connector.get_table_schema("queue_messages", {})

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "queue_name" in field_names
        assert "sequence_number" in field_names
        assert "body" in field_names
        assert "message_id" in field_names

    def test_get_table_schema_subscription_messages(self, connector):
        """Test schema for subscription_messages table."""
        schema = connector.get_table_schema("subscription_messages", {})

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "topic_name" in field_names
        assert "subscription_name" in field_names
        assert "sequence_number" in field_names

    def test_get_table_schema_dead_letter_messages(self, connector):
        """Test schema for dead_letter_messages table."""
        schema = connector.get_table_schema("dead_letter_messages", {})

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "source_type" in field_names
        assert "source_name" in field_names
        assert "dead_letter_reason" in field_names

    def test_get_table_schema_invalid_table(self, connector):
        """Test that invalid table name raises error."""
        with pytest.raises(ValueError, match="Unsupported table"):
            connector.get_table_schema("invalid_table", {})

    def test_read_table_metadata_queues(self, connector):
        """Test metadata for queues table."""
        metadata = connector.read_table_metadata("queues", {})

        assert metadata["primary_keys"] == ["name"]
        assert metadata["cursor_field"] is None
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_queue_messages(self, connector):
        """Test metadata for queue_messages table."""
        metadata = connector.read_table_metadata("queue_messages", {})

        assert metadata["primary_keys"] == ["queue_name", "sequence_number"]
        assert metadata["cursor_field"] == "sequence_number"
        assert metadata["ingestion_type"] == "append"

    def test_read_table_metadata_invalid_table(self, connector):
        """Test that invalid table name raises error."""
        with pytest.raises(ValueError, match="Unsupported table"):
            connector.read_table_metadata("invalid_table", {})


# =========================================================================
# Custom Tests: Read Operations
# =========================================================================


class TestReadQueues:  # pylint: disable=too-few-public-methods
    """Test reading queues table."""

    def test_read_queues(self, connector):
        """Test reading queues from the namespace."""
        records_iter, next_offset = connector.read_table("queues", {}, {})

        records = list(records_iter)

        assert isinstance(records, list)
        assert isinstance(next_offset, dict)

        if records:
            record = records[0]
            assert "name" in record
            assert "max_size_in_megabytes" in record


class TestReadTopics:  # pylint: disable=too-few-public-methods
    """Test reading topics table."""

    def test_read_topics(self, connector):
        """Test reading topics from the namespace."""
        records_iter, next_offset = connector.read_table("topics", {}, {})

        records = list(records_iter)

        assert isinstance(records, list)
        assert isinstance(next_offset, dict)

        if records:
            record = records[0]
            assert "name" in record
            assert "subscription_count" in record


class TestReadSubscriptions:
    """Test reading subscriptions table."""

    def test_read_subscriptions(self, connector):
        """Test reading subscriptions from all topics."""
        records_iter, next_offset = connector.read_table("subscriptions", {}, {})

        records = list(records_iter)

        assert isinstance(records, list)
        assert isinstance(next_offset, dict)

        if records:
            record = records[0]
            assert "name" in record
            assert "topic_name" in record

    def test_read_subscriptions_with_topic_filter(self, connector, config):
        """Test reading subscriptions for a specific topic."""
        topic_name = config.get("test_topic_name")
        if not topic_name:
            pytest.skip("test_topic_name not configured")

        records_iter, next_offset = connector.read_table(
            "subscriptions", {}, {"topic_name": topic_name}
        )

        records = list(records_iter)

        for record in records:
            assert record["topic_name"] == topic_name


class TestReadQueueMessages:
    """Test reading queue_messages table."""

    def test_read_queue_messages_requires_queue_name(self, connector):
        """Test that queue_name is required."""
        with pytest.raises(ValueError, match="queue_name is required"):
            records_iter, _ = connector.read_table("queue_messages", {}, {})
            list(records_iter)  # Force evaluation

    def test_read_queue_messages(self, connector, config):
        """Test reading messages from a queue."""
        queue_name = config.get("test_queue_name")
        if not queue_name:
            pytest.skip("test_queue_name not configured")

        records_iter, next_offset = connector.read_table(
            "queue_messages",
            {},
            {"queue_name": queue_name, "max_message_count": "10"},
        )

        records = list(records_iter)

        assert isinstance(records, list)
        assert isinstance(next_offset, dict)

        if records:
            record = records[0]
            assert record["queue_name"] == queue_name
            assert "sequence_number" in record
            assert "body" in record

    def test_read_queue_messages_incremental(self, connector, config):
        """Test incremental reading of queue messages."""
        queue_name = config.get("test_queue_name")
        if not queue_name:
            pytest.skip("test_queue_name not configured")

        # First read
        records_iter, offset1 = connector.read_table(
            "queue_messages",
            {},
            {"queue_name": queue_name, "max_message_count": "5"},
        )
        records1 = list(records_iter)

        if not records1:
            pytest.skip("No messages in queue to test incremental read")

        # Second read from offset
        records_iter, offset2 = connector.read_table(
            "queue_messages",
            offset1,
            {"queue_name": queue_name, "max_message_count": "5"},
        )
        records2 = list(records_iter)

        # Records should not overlap
        seq_nums_1 = {r["sequence_number"] for r in records1}
        seq_nums_2 = {r["sequence_number"] for r in records2}
        assert seq_nums_1.isdisjoint(seq_nums_2)


class TestReadSubscriptionMessages:
    """Test reading subscription_messages table."""

    def test_read_subscription_messages_requires_params(self, connector):
        """Test that topic_name and subscription_name are required."""
        with pytest.raises(
            ValueError, match="topic_name and subscription_name are required"
        ):
            records_iter, _ = connector.read_table(
                "subscription_messages", {}, {}
            )
            list(records_iter)

    def test_read_subscription_messages(self, connector, config):
        """Test reading messages from a subscription."""
        topic_name = config.get("test_topic_name")
        subscription_name = config.get("test_subscription_name")
        if not topic_name or not subscription_name:
            pytest.skip(
                "test_topic_name and test_subscription_name not configured"
            )

        records_iter, next_offset = connector.read_table(
            "subscription_messages",
            {},
            {
                "topic_name": topic_name,
                "subscription_name": subscription_name,
                "max_message_count": "10",
            },
        )

        records = list(records_iter)

        assert isinstance(records, list)

        if records:
            record = records[0]
            assert record["topic_name"] == topic_name
            assert record["subscription_name"] == subscription_name


class TestReadDeadLetterMessages:
    """Test reading dead_letter_messages table."""

    def test_read_dead_letter_messages_requires_source_type(self, connector):
        """Test that source_type is required."""
        with pytest.raises(ValueError, match="source_type must be"):
            records_iter, _ = connector.read_table(
                "dead_letter_messages", {}, {}
            )
            list(records_iter)

    def test_read_dead_letter_messages_queue(self, connector, config):
        """Test reading dead-letter messages from a queue."""
        queue_name = config.get("test_queue_name")
        if not queue_name:
            pytest.skip("test_queue_name not configured")

        records_iter, next_offset = connector.read_table(
            "dead_letter_messages",
            {},
            {"source_type": "queue", "queue_name": queue_name},
        )

        records = list(records_iter)

        assert isinstance(records, list)

        if records:
            record = records[0]
            assert record["source_type"] == "queue"
            assert "dead_letter_reason" in record


# =========================================================================
# Custom Tests: Authentication Methods
# =========================================================================


class TestAuthentication:
    """Test authentication methods."""

    def test_connection_string_auth(self, config):
        """Test connection string authentication."""
        conn_str = config.get("connection_string")
        if not conn_str:
            pytest.skip("connection_string not configured")

        connector = LakeflowConnect({"connection_string": conn_str})
        tables = connector.list_tables()
        connector.close()

        assert len(tables) == 6

    def test_missing_credentials_raises_error(self):
        """Test that missing credentials raises error."""
        with pytest.raises(ValueError, match="Either 'connection_string' or"):
            LakeflowConnect({})


class TestAzureADAuthentication:
    """Test Azure AD authentication.

    Requires Azure CLI to be logged in: az login
    """

    @pytest.fixture
    def azure_ad_connector(self, config):
        """Create a connector using Azure AD authentication."""
        namespace = config.get("fully_qualified_namespace")
        if not namespace:
            pytest.skip("fully_qualified_namespace not configured")

        try:
            connector = LakeflowConnect(
                {
                    "fully_qualified_namespace": namespace,
                    "credential_type": "azure_ad",
                }
            )
            yield connector
            connector.close()
        except ValueError as e:
            error_str = str(e).lower()
            if (
                "no azure credentials" in error_str
                or "azure ad authentication failed" in error_str
            ):
                pytest.skip("Azure AD credentials not available (run 'az login')")
            raise
        except Exception as e:
            if "DefaultAzureCredential" in str(e) or "No credential" in str(e):
                pytest.skip("Azure AD credentials not available (run 'az login')")
            raise

    def test_azure_ad_list_tables(self, azure_ad_connector):
        """Test that list_tables works with Azure AD."""
        tables = azure_ad_connector.list_tables()
        assert len(tables) == 6

    def test_azure_ad_read_queues(self, azure_ad_connector):
        """Test reading queues with Azure AD."""
        records_iter, offset = azure_ad_connector.read_table("queues", {}, {})
        records = list(records_iter)
        assert isinstance(records, list)

    def test_azure_ad_read_topics(self, azure_ad_connector):
        """Test reading topics with Azure AD."""
        records_iter, offset = azure_ad_connector.read_table("topics", {}, {})
        records = list(records_iter)
        assert isinstance(records, list)


class TestServicePrincipalAuthentication:
    """Test service principal (client credentials) authentication."""

    @pytest.fixture
    def service_principal_connector(self, config):
        """Create a connector using service principal authentication."""
        namespace = config.get("fully_qualified_namespace")
        tenant_id = config.get("azure_tenant_id")
        client_id = config.get("azure_client_id")
        client_secret = config.get("azure_client_secret")

        if not all([namespace, tenant_id, client_id, client_secret]):
            pytest.skip(
                "Service principal credentials not configured. "
                "Set fully_qualified_namespace, azure_tenant_id, azure_client_id, "
                "and azure_client_secret in dev_config.json"
            )

        try:
            connector = LakeflowConnect(
                {
                    "fully_qualified_namespace": namespace,
                    "credential_type": "service_principal",
                    "azure_tenant_id": tenant_id,
                    "azure_client_id": client_id,
                    "azure_client_secret": client_secret,
                }
            )
            yield connector
            connector.close()
        except ValueError as e:
            if "authentication failed" in str(e).lower():
                pytest.skip(f"Service principal authentication failed: {e}")
            raise

    def test_service_principal_list_tables(self, service_principal_connector):
        """Test that list_tables works with service principal."""
        tables = service_principal_connector.list_tables()
        assert len(tables) == 6

    def test_service_principal_read_queues(self, service_principal_connector):
        """Test reading queues with service principal."""
        records_iter, offset = service_principal_connector.read_table(
            "queues", {}, {}
        )
        records = list(records_iter)
        assert isinstance(records, list)

    def test_service_principal_missing_credentials(self, config):
        """Test that missing service principal credentials raises helpful error."""
        namespace = config.get("fully_qualified_namespace")
        if not namespace:
            pytest.skip("fully_qualified_namespace not configured")

        with pytest.raises(
            ValueError,
            match="azure_tenant_id.*azure_client_id.*azure_client_secret",
        ):
            LakeflowConnect(
                {
                    "fully_qualified_namespace": namespace,
                    "credential_type": "service_principal",
                    "azure_tenant_id": "some-tenant",
                }
            )


class TestAuthenticationErrorMessages:
    """Test that authentication errors provide helpful messages."""

    def test_missing_all_credentials_error(self):
        """Test error message when no credentials provided."""
        with pytest.raises(ValueError) as exc_info:
            LakeflowConnect({})

        error_msg = str(exc_info.value)
        assert "connection_string" in error_msg
        assert "fully_qualified_namespace" in error_msg

    def test_invalid_credential_type_error(self, config):
        """Test error message for invalid credential type."""
        namespace = config.get("fully_qualified_namespace")
        if not namespace:
            pytest.skip("fully_qualified_namespace not configured")

        with pytest.raises(ValueError, match="Unsupported credential_type"):
            LakeflowConnect(
                {
                    "fully_qualified_namespace": namespace,
                    "credential_type": "invalid_type",
                }
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
