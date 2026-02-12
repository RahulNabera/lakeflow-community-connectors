# pylint: disable=too-many-lines
# Azure Service Bus Lakeflow Community Connector
#
# This connector enables data ingestion from Azure Service Bus into Databricks
# using the Lakeflow Community Connector framework.
#
# Supported tables:
# - queues: List of queues in the namespace
# - topics: List of topics in the namespace
# - subscriptions: Subscriptions for topics
# - queue_messages: Messages from queues (peek without consuming)
# - subscription_messages: Messages from topic subscriptions (peek without consuming)
# - dead_letter_messages: Dead-lettered messages
#
# Authentication:
# - Connection string with SAS token
# - Azure AD (DefaultAzureCredential)
# - Service Principal (ClientSecretCredential)
# - Managed Identity (ManagedIdentityCredential)

import json
import base64
import logging
import time
from datetime import datetime
from typing import Iterator, Optional, Any, Callable, TypeVar

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    TimestampType,
)

# Azure Service Bus SDK
from azure.servicebus import ServiceBusClient, ServiceBusSubQueue, ServiceBusSessionFilter  # pylint: disable=import-error
from azure.servicebus.management import ServiceBusAdministrationClient  # pylint: disable=import-error
from azure.servicebus.exceptions import (  # pylint: disable=import-error
    ServiceBusError,
    ServiceBusConnectionError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    OperationTimeoutError,
    MessageSizeExceededError,
)
from azure.identity import (  # pylint: disable=import-error
    DefaultAzureCredential,
    ClientSecretCredential,
    ManagedIdentityCredential,
)
from azure.core.exceptions import (  # pylint: disable=import-error
    ClientAuthenticationError,
    HttpResponseError,
    ServiceRequestError,
    ServiceResponseError,
)

from databricks.labs.community_connector.interface.lakeflow_connect import (  # pylint: disable=import-error
    LakeflowConnect,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

_TRANSIENT_EXCEPTIONS = (
    ServiceBusConnectionError,
    OperationTimeoutError,
    ServiceRequestError,
    ServiceResponseError,
    ConnectionError,
    TimeoutError,
    OSError,
)


def _is_transient(exc: Exception) -> bool:
    """Return True if the exception is transient and the operation can be retried."""
    if isinstance(exc, _TRANSIENT_EXCEPTIONS):
        return True
    if isinstance(exc, HttpResponseError):
        return exc.status_code in (408, 429, 500, 502, 503, 504)
    if isinstance(exc, ServiceBusError):
        msg = str(exc).lower()
        return any(kw in msg for kw in ("timeout", "busy", "throttl", "temporary"))
    return False


def _retry_with_backoff(
    func: Callable[[], T],
    *,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    operation_name: str = "",
) -> T:
    """
    Execute *func* with retry + exponential backoff for transient failures.
    Non-transient exceptions are raised immediately.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if attempt == max_retries or not _is_transient(exc):
                raise
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.warning(
                "[Retry %d/%d] %s failed (%s: %s), retrying in %.1fs",
                attempt + 1,
                max_retries,
                operation_name or "Operation",
                type(exc).__name__,
                exc,
                delay,
            )
            time.sleep(delay)
    # Should not reach here, but satisfy the type checker
    raise last_exc  # type: ignore[misc]


class AzureServicebusLakeflowConnect(LakeflowConnect):  # pylint: disable=too-many-instance-attributes
    """
    Azure Service Bus connector implementing the Lakeflow Community Connector interface.
    """

    # Supported tables
    SUPPORTED_TABLES = [
        "queues",
        "topics",
        "subscriptions",
        "queue_messages",
        "subscription_messages",
        "dead_letter_messages",
    ]

    # Tables that require incremental ingestion
    INCREMENTAL_TABLES = [
        "queue_messages",
        "subscription_messages",
        "dead_letter_messages",
    ]

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Azure Service Bus connector.

        Args:
            options: Dictionary containing connection parameters:
                - connection_string: Full Service Bus connection string (option 1)
                - fully_qualified_namespace: Namespace URL for Azure AD auth (option 2)
                - credential_type: 'connection_string', 'azure_ad', 'service_principal',
                                   or 'managed_identity' (default: auto-detect)
                - operation_timeout: Timeout in seconds for SDK operations (default: 60)
                - max_wait_time: Max wait time in seconds for receivers (default: 5)
                - max_retries: Max retry attempts for transient failures (default: 3)
                - max_body_size: Max body size in bytes before truncation (default: 0 = unlimited)
                - debug_mode: 'true' to enable verbose per-message logging (default: 'false')

                For service_principal credential_type:
                - azure_tenant_id: Azure AD tenant ID
                - azure_client_id: Service principal client/application ID
                - azure_client_secret: Service principal client secret

                For managed_identity credential_type:
                - azure_client_id: (Optional) Client ID for user-assigned managed identity
        """
        self.options = options
        self._admin_client: Optional[ServiceBusAdministrationClient] = None
        self._client: Optional[ServiceBusClient] = None
        self._credential = None
        self._closed = False

        # Configurable timeouts and retry
        self.operation_timeout = int(options.get("operation_timeout", "60"))
        self.max_wait_time = int(options.get("max_wait_time", "5"))
        self.max_retries = int(options.get("max_retries", "3"))
        self.max_body_size = int(options.get("max_body_size", "0"))
        self.debug_mode = options.get("debug_mode", "false").lower() == "true"

        # Determine authentication method
        self.connection_string = options.get("connection_string")
        self.fully_qualified_namespace = options.get("fully_qualified_namespace")
        self.credential_type = options.get("credential_type", "auto")

        # Azure AD / Service Principal options
        self.azure_tenant_id = options.get("azure_tenant_id")
        self.azure_client_id = options.get("azure_client_id")
        self.azure_client_secret = options.get("azure_client_secret")

        if self.credential_type == "auto":
            if self.connection_string:
                self.credential_type = "connection_string"
            elif (
                self.azure_tenant_id
                and self.azure_client_id
                and self.azure_client_secret
            ):
                self.credential_type = "service_principal"
            elif self.fully_qualified_namespace:
                self.credential_type = "azure_ad"
            else:
                raise ValueError(
                    "Either 'connection_string' or 'fully_qualified_namespace' must be provided. "
                    "For Azure AD authentication, provide 'fully_qualified_namespace'. "
                    "For service principal, also provide 'azure_tenant_id', 'azure_client_id', "
                    "and 'azure_client_secret'."
                )

        # Initialize clients
        self._init_clients()

    def _init_clients(self) -> None:
        """Initialize Service Bus clients based on credential type."""
        if self.credential_type == "connection_string":
            self._init_connection_string_auth()
        elif self.credential_type == "service_principal":
            self._init_service_principal_auth()
        elif self.credential_type == "managed_identity":
            self._init_managed_identity_auth()
        elif self.credential_type == "azure_ad":
            self._init_azure_ad_auth()
        else:
            raise ValueError(f"Unsupported credential_type: {self.credential_type}")

    def _init_connection_string_auth(self) -> None:
        """Initialize clients using connection string authentication."""
        if not self.connection_string:
            raise ValueError(
                "connection_string is required for connection_string auth"
            )
        try:
            self._admin_client = (
                ServiceBusAdministrationClient.from_connection_string(
                    self.connection_string
                )
            )
            self._client = ServiceBusClient.from_connection_string(
                self.connection_string,
                socket_timeout=self.operation_timeout,
            )
        except Exception as e:
            raise ValueError(
                f"Failed to initialize Service Bus client with connection string. "
                f"Ensure the connection string is valid and includes the SharedAccessKey. "
                f"Error: {e}"
            ) from e

    def _init_service_principal_auth(self) -> None:
        """Initialize clients using service principal (client credentials) authentication."""
        if not self.fully_qualified_namespace:
            raise ValueError(
                "fully_qualified_namespace is required for service_principal auth"
            )
        if not all(
            [self.azure_tenant_id, self.azure_client_id, self.azure_client_secret]
        ):
            raise ValueError(
                "azure_tenant_id, azure_client_id, and azure_client_secret are all required "
                "for service_principal authentication"
            )

        try:
            self._credential = ClientSecretCredential(
                tenant_id=self.azure_tenant_id,
                client_id=self.azure_client_id,
                client_secret=self.azure_client_secret,
            )
            self._create_clients_with_credential(self._credential)
        except ClientAuthenticationError as e:
            raise ValueError(
                f"Service principal authentication failed. Please verify:\n"
                f"  1. azure_tenant_id is correct: {self.azure_tenant_id}\n"
                f"  2. azure_client_id is correct: {self.azure_client_id}\n"
                f"  3. azure_client_secret is valid and not expired\n"
                f"  4. The service principal has required RBAC roles on the Service Bus namespace:\n"
                f"     - 'Azure Service Bus Data Receiver' or 'Azure Service Bus Data Owner'\n"
                f"     - 'Reader' role for management operations\n"
                f"Error: {e}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"Failed to initialize Service Bus client with service principal. Error: {e}"
            ) from e

    def _init_managed_identity_auth(self) -> None:
        """Initialize clients using managed identity authentication."""
        if not self.fully_qualified_namespace:
            raise ValueError(
                "fully_qualified_namespace is required for managed_identity auth"
            )

        try:
            if self.azure_client_id:
                self._credential = ManagedIdentityCredential(
                    client_id=self.azure_client_id
                )
                logger.info(
                    f"Using user-assigned managed identity: {self.azure_client_id}"
                )
            else:
                self._credential = ManagedIdentityCredential()
                logger.info("Using system-assigned managed identity")

            self._create_clients_with_credential(self._credential)
        except ClientAuthenticationError as e:
            error_msg = (
                f"Managed identity authentication failed. Please verify:\n"
                f"  1. This code is running in an Azure environment with managed identity enabled\n"
                f"  2. The managed identity has required RBAC roles on the Service Bus namespace:\n"
                f"     - 'Azure Service Bus Data Receiver' or 'Azure Service Bus Data Owner'\n"
                f"     - 'Reader' role for management operations\n"
            )
            if self.azure_client_id:
                error_msg += (
                    f"  3. User-assigned managed identity client_id"
                    f" is correct: {self.azure_client_id}\n"
                )
            error_msg += f"Error: {e}"
            raise ValueError(error_msg) from e
        except Exception as e:
            raise ValueError(
                f"Failed to initialize Service Bus client with managed identity. "
                f"Ensure you are running in an Azure environment with managed identity enabled. "
                f"Error: {e}"
            ) from e

    def _init_azure_ad_auth(self) -> None:
        """Initialize clients using Azure AD DefaultAzureCredential authentication."""
        if not self.fully_qualified_namespace:
            raise ValueError(
                "fully_qualified_namespace is required for azure_ad auth"
            )

        try:
            self._credential = DefaultAzureCredential()
            self._create_clients_with_credential(self._credential)
        except ClientAuthenticationError as e:
            raise ValueError(
                f"Azure AD authentication failed. Please ensure one of the following:\n"
                f"  1. Run 'az login' to authenticate with Azure CLI\n"
                f"  2. Set environment variables: AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET\n"
                f"  3. Use managed identity in Azure environment\n"
                f"  4. Use service_principal credential_type with explicit credentials\n\n"
                f"After authenticating, ensure the identity has required RBAC roles:\n"
                f"  - 'Azure Service Bus Data Receiver' or 'Azure Service Bus Data Owner'\n"
                f"  - 'Reader' role for management operations\n"
                f"Error: {e}"
            ) from e
        except Exception as e:
            error_str = str(e).lower()
            if "no credential" in error_str or "defaultazurecredential" in error_str:
                raise ValueError(
                    f"No Azure credentials found. Please authenticate using one of:\n"
                    f"  1. Azure CLI: Run 'az login' and 'az account set -s <subscription-id>'\n"
                    f"  2. Environment variables: Set AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET\n"
                    f"  3. Managed Identity: Deploy to Azure with managed identity enabled\n"
                    f"  4. Service Principal: Use credential_type='service_principal' with explicit credentials\n"
                    f"Error: {e}"
                ) from e
            raise ValueError(
                f"Failed to initialize Service Bus client with Azure AD. Error: {e}"
            ) from e

    def _create_clients_with_credential(self, credential) -> None:
        """Create Service Bus clients with the given credential."""
        try:
            self._admin_client = ServiceBusAdministrationClient(
                self.fully_qualified_namespace, credential
            )
            self._client = ServiceBusClient(
                self.fully_qualified_namespace, credential,
                socket_timeout=self.operation_timeout,
            )

            # Validate the connection by making a simple API call
            self._validate_connection()
        except ClientAuthenticationError:
            raise
        except Exception as e:
            error_str = str(e).lower()
            if "unauthorized" in error_str or "403" in error_str:
                raise ValueError(
                    f"Authorization failed. The authenticated identity does not have required permissions.\n"
                    f"Assign these RBAC roles to the identity on the Service Bus namespace:\n"
                    f"  - 'Azure Service Bus Data Receiver' for reading messages\n"
                    f"  - 'Azure Service Bus Data Owner' for full access\n"
                    f"  - 'Reader' for listing queues/topics\n"
                    f"Namespace: {self.fully_qualified_namespace}\n"
                    f"Error: {e}"
                ) from e
            raise

    def _validate_connection(self) -> None:
        """Validate the connection by making a test API call."""
        try:
            next(iter(self._admin_client.list_queues()), None)
            logger.info(
                f"Successfully connected to Service Bus namespace: "
                f"{self.fully_qualified_namespace}"
            )
        except ClientAuthenticationError:
            raise
        except Exception as e:
            error_str = str(e).lower()
            if (
                "unauthorized" in error_str
                or "403" in error_str
                or "forbidden" in error_str
            ):
                raise ValueError(
                    f"Connected to Service Bus but authorization failed.\n"
                    f"The identity can authenticate but lacks permissions on namespace: "
                    f"{self.fully_qualified_namespace}\n"
                    f"Required RBAC roles:\n"
                    f"  - 'Reader' or 'Contributor' for management operations\n"
                    f"  - 'Azure Service Bus Data Receiver' for reading messages\n"
                    f"Error: {e}"
                ) from e
            logger.warning(f"Could not validate connection: {e}")
            raise RuntimeError(
                f"Failed to validate connection to Service Bus namespace "
                f"'{self.fully_qualified_namespace}': {e}"
            ) from e

    # =========================================================================
    # Health Check
    # =========================================================================

    def health_check(self) -> dict:
        """
        Check the health of the connection.

        Returns:
            Dictionary with 'healthy' (bool) and 'details' (str).
        """
        if self._closed:
            return {"healthy": False, "details": "Connector has been closed."}
        try:
            queues = list(self._admin_client.list_queues())
            return {
                "healthy": True,
                "details": f"Connected. {len(queues)} queue(s) accessible.",
            }
        except Exception as e:
            return {"healthy": False, "details": f"Connection error: {e}"}

    def list_tables(self) -> list[str]:
        """
        List all supported tables.

        Returns:
            List of table names supported by this connector.
        """
        return self.SUPPORTED_TABLES.copy()

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Get the schema for a table.

        Args:
            table_name: Name of the table
            table_options: Additional options for the table

        Returns:
            StructType representing the table schema
        """
        if table_name not in self.SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table_name}")

        schemas = {
            "queues": self._get_queues_schema(),
            "topics": self._get_topics_schema(),
            "subscriptions": self._get_subscriptions_schema(),
            "queue_messages": self._get_queue_messages_schema(),
            "subscription_messages": self._get_subscription_messages_schema(),
            "dead_letter_messages": self._get_dead_letter_messages_schema(),
        }

        return schemas[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Get metadata for a table including primary keys and ingestion type.

        Args:
            table_name: Name of the table
            table_options: Additional options for the table

        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        if table_name not in self.SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table_name}")

        metadata = {
            "queues": {
                "primary_keys": ["name"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
            },
            "topics": {
                "primary_keys": ["name"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
            },
            "subscriptions": {
                "primary_keys": ["topic_name", "name"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
            },
            "queue_messages": {
                "primary_keys": ["queue_name", "sequence_number"],
                "cursor_field": "sequence_number",
                "ingestion_type": "append",
            },
            "subscription_messages": {
                "primary_keys": [
                    "topic_name",
                    "subscription_name",
                    "sequence_number",
                ],
                "cursor_field": "sequence_number",
                "ingestion_type": "append",
            },
            "dead_letter_messages": {
                "primary_keys": ["source_type", "source_name", "sequence_number"],
                "cursor_field": "sequence_number",
                "ingestion_type": "append",
            },
        }

        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a table.

        Args:
            table_name: Name of the table to read
            start_offset: Starting offset for incremental reads
            table_options: Additional options for the table

        Returns:
            Tuple of (iterator of records, next offset)
        """
        if table_name not in self.SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table_name}")

        readers = {
            "queues": self._read_queues,
            "topics": self._read_topics,
            "subscriptions": self._read_subscriptions,
            "queue_messages": self._read_queue_messages,
            "subscription_messages": self._read_subscription_messages,
            "dead_letter_messages": self._read_dead_letter_messages,
        }

        return readers[table_name](start_offset, table_options)

    # =========================================================================
    # Schema Definitions
    # =========================================================================

    def _get_queues_schema(self) -> StructType:
        """Schema for queues table."""
        return StructType(
            [
                StructField("name", StringType(), False),
                StructField("id", StringType(), True),
                StructField("lock_duration", StringType(), True),
                StructField("max_size_in_megabytes", LongType(), True),
                StructField("requires_duplicate_detection", BooleanType(), True),
                StructField("requires_session", BooleanType(), True),
                StructField("default_message_time_to_live", StringType(), True),
                StructField(
                    "dead_lettering_on_message_expiration", BooleanType(), True
                ),
                StructField(
                    "duplicate_detection_history_time_window", StringType(), True
                ),
                StructField("max_delivery_count", LongType(), True),
                StructField("status", StringType(), True),
                StructField("enable_batched_operations", BooleanType(), True),
                StructField("auto_delete_on_idle", StringType(), True),
                StructField("enable_partitioning", BooleanType(), True),
                StructField("message_count", LongType(), True),
                StructField("size_in_bytes", LongType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("accessed_at", TimestampType(), True),
            ]
        )

    def _get_topics_schema(self) -> StructType:
        """Schema for topics table."""
        return StructType(
            [
                StructField("name", StringType(), False),
                StructField("id", StringType(), True),
                StructField("default_message_time_to_live", StringType(), True),
                StructField("max_size_in_megabytes", LongType(), True),
                StructField("requires_duplicate_detection", BooleanType(), True),
                StructField(
                    "duplicate_detection_history_time_window", StringType(), True
                ),
                StructField("enable_batched_operations", BooleanType(), True),
                StructField("status", StringType(), True),
                StructField("support_ordering", BooleanType(), True),
                StructField("auto_delete_on_idle", StringType(), True),
                StructField("enable_partitioning", BooleanType(), True),
                StructField("subscription_count", LongType(), True),
                StructField("size_in_bytes", LongType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("accessed_at", TimestampType(), True),
            ]
        )

    def _get_subscriptions_schema(self) -> StructType:
        """Schema for subscriptions table."""
        return StructType(
            [
                StructField("name", StringType(), False),
                StructField("topic_name", StringType(), False),
                StructField("id", StringType(), True),
                StructField("lock_duration", StringType(), True),
                StructField("requires_session", BooleanType(), True),
                StructField("default_message_time_to_live", StringType(), True),
                StructField(
                    "dead_lettering_on_message_expiration", BooleanType(), True
                ),
                StructField(
                    "dead_lettering_on_filter_evaluation_exceptions",
                    BooleanType(),
                    True,
                ),
                StructField("message_count", LongType(), True),
                StructField("max_delivery_count", LongType(), True),
                StructField("status", StringType(), True),
                StructField("enable_batched_operations", BooleanType(), True),
                StructField("auto_delete_on_idle", StringType(), True),
                StructField("forward_to", StringType(), True),
                StructField(
                    "forward_dead_lettered_messages_to", StringType(), True
                ),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("accessed_at", TimestampType(), True),
            ]
        )

    def _get_message_base_schema(self) -> list[StructField]:
        """Base schema fields for message tables."""
        return [
            StructField("sequence_number", LongType(), False),
            StructField("message_id", StringType(), True),
            StructField("body", StringType(), True),
            StructField("content_type", StringType(), True),
            StructField("correlation_id", StringType(), True),
            StructField("subject", StringType(), True),
            StructField("reply_to", StringType(), True),
            StructField("reply_to_session_id", StringType(), True),
            StructField("to", StringType(), True),
            StructField("time_to_live", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("partition_key", StringType(), True),
            StructField("scheduled_enqueue_time_utc", TimestampType(), True),
            StructField("enqueued_time_utc", TimestampType(), True),
            StructField("expires_at_utc", TimestampType(), True),
            StructField("enqueued_sequence_number", LongType(), True),
            StructField("dead_letter_source", StringType(), True),
            StructField("delivery_count", LongType(), True),
            StructField("lock_token", StringType(), True),
            StructField("locked_until_utc", TimestampType(), True),
            StructField("state", StringType(), True),
            StructField("application_properties", StringType(), True),
        ]

    def _get_queue_messages_schema(self) -> StructType:
        """Schema for queue_messages table."""
        fields = [StructField("queue_name", StringType(), False)]
        fields.extend(self._get_message_base_schema())
        return StructType(fields)

    def _get_subscription_messages_schema(self) -> StructType:
        """Schema for subscription_messages table."""
        fields = [
            StructField("topic_name", StringType(), False),
            StructField("subscription_name", StringType(), False),
        ]
        fields.extend(self._get_message_base_schema())
        return StructType(fields)

    def _get_dead_letter_messages_schema(self) -> StructType:
        """Schema for dead_letter_messages table."""
        fields = [
            StructField("source_type", StringType(), False),
            StructField("source_name", StringType(), False),
        ]
        fields.extend(self._get_message_base_schema())
        fields.extend(
            [
                StructField("dead_letter_reason", StringType(), True),
                StructField("dead_letter_error_description", StringType(), True),
            ]
        )
        return StructType(fields)

    # =========================================================================
    # Read Implementations
    # =========================================================================

    def _read_queues(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read queues from the namespace."""

        def generate_records():
            queues = _retry_with_backoff(
                lambda: list(self._admin_client.list_queues()),
                max_retries=self.max_retries,
                operation_name="list_queues",
            )
            for queue in queues:
                runtime_props = _retry_with_backoff(
                    lambda q=queue: self._admin_client.get_queue_runtime_properties(q.name),
                    max_retries=self.max_retries,
                    operation_name=f"get_queue_runtime_properties({queue.name})",
                )
                yield {
                    "name": queue.name,
                    "id": None,
                    "lock_duration": (
                        str(queue.lock_duration) if queue.lock_duration else None
                    ),
                    "max_size_in_megabytes": queue.max_size_in_megabytes,
                    "requires_duplicate_detection": queue.requires_duplicate_detection,
                    "requires_session": queue.requires_session,
                    "default_message_time_to_live": (
                        str(queue.default_message_time_to_live)
                        if queue.default_message_time_to_live
                        else None
                    ),
                    "dead_lettering_on_message_expiration": (
                        queue.dead_lettering_on_message_expiration
                    ),
                    "duplicate_detection_history_time_window": (
                        str(queue.duplicate_detection_history_time_window)
                        if queue.duplicate_detection_history_time_window
                        else None
                    ),
                    "max_delivery_count": queue.max_delivery_count,
                    "status": str(queue.status) if queue.status else None,
                    "enable_batched_operations": queue.enable_batched_operations,
                    "auto_delete_on_idle": (
                        str(queue.auto_delete_on_idle)
                        if queue.auto_delete_on_idle
                        else None
                    ),
                    "enable_partitioning": queue.enable_partitioning,
                    "message_count": runtime_props.total_message_count,
                    "size_in_bytes": runtime_props.size_in_bytes,
                    "created_at": runtime_props.created_at_utc,
                    "updated_at": runtime_props.updated_at_utc,
                    "accessed_at": runtime_props.accessed_at_utc,
                }

        return generate_records(), {}

    def _read_topics(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read topics from the namespace."""

        def generate_records():
            topics = _retry_with_backoff(
                lambda: list(self._admin_client.list_topics()),
                max_retries=self.max_retries,
                operation_name="list_topics",
            )
            for topic in topics:
                runtime_props = _retry_with_backoff(
                    lambda t=topic: self._admin_client.get_topic_runtime_properties(t.name),
                    max_retries=self.max_retries,
                    operation_name=f"get_topic_runtime_properties({topic.name})",
                )
                yield {
                    "name": topic.name,
                    "id": None,
                    "default_message_time_to_live": (
                        str(topic.default_message_time_to_live)
                        if topic.default_message_time_to_live
                        else None
                    ),
                    "max_size_in_megabytes": topic.max_size_in_megabytes,
                    "requires_duplicate_detection": topic.requires_duplicate_detection,
                    "duplicate_detection_history_time_window": (
                        str(topic.duplicate_detection_history_time_window)
                        if topic.duplicate_detection_history_time_window
                        else None
                    ),
                    "enable_batched_operations": topic.enable_batched_operations,
                    "status": str(topic.status) if topic.status else None,
                    "support_ordering": topic.support_ordering,
                    "auto_delete_on_idle": (
                        str(topic.auto_delete_on_idle)
                        if topic.auto_delete_on_idle
                        else None
                    ),
                    "enable_partitioning": topic.enable_partitioning,
                    "subscription_count": runtime_props.subscription_count,
                    "size_in_bytes": runtime_props.size_in_bytes,
                    "created_at": runtime_props.created_at_utc,
                    "updated_at": runtime_props.updated_at_utc,
                    "accessed_at": runtime_props.accessed_at_utc,
                }

        return generate_records(), {}

    def _read_subscriptions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read subscriptions from topics."""
        topic_name = table_options.get("topic_name")

        def generate_records():
            if topic_name:
                topics_to_read = [topic_name]
            else:
                topics_to_read = _retry_with_backoff(
                    lambda: [t.name for t in self._admin_client.list_topics()],
                    max_retries=self.max_retries,
                    operation_name="list_topics_for_subscriptions",
                )

            for t_name in topics_to_read:
                subs = _retry_with_backoff(
                    lambda tn=t_name: list(self._admin_client.list_subscriptions(tn)),
                    max_retries=self.max_retries,
                    operation_name=f"list_subscriptions({t_name})",
                )
                for sub in subs:
                    runtime_props = _retry_with_backoff(
                        lambda tn=t_name, sn=sub.name: (
                            self._admin_client.get_subscription_runtime_properties(tn, sn)
                        ),
                        max_retries=self.max_retries,
                        operation_name=f"get_subscription_runtime_properties({t_name}/{sub.name})",
                    )
                    yield {
                        "name": sub.name,
                        "topic_name": t_name,
                        "id": None,
                        "lock_duration": (
                            str(sub.lock_duration) if sub.lock_duration else None
                        ),
                        "requires_session": sub.requires_session,
                        "default_message_time_to_live": (
                            str(sub.default_message_time_to_live)
                            if sub.default_message_time_to_live
                            else None
                        ),
                        "dead_lettering_on_message_expiration": (
                            sub.dead_lettering_on_message_expiration
                        ),
                        "dead_lettering_on_filter_evaluation_exceptions": (
                            sub.dead_lettering_on_filter_evaluation_exceptions
                        ),
                        "message_count": runtime_props.total_message_count,
                        "max_delivery_count": sub.max_delivery_count,
                        "status": str(sub.status) if sub.status else None,
                        "enable_batched_operations": sub.enable_batched_operations,
                        "auto_delete_on_idle": (
                            str(sub.auto_delete_on_idle)
                            if sub.auto_delete_on_idle
                            else None
                        ),
                        "forward_to": sub.forward_to,
                        "forward_dead_lettered_messages_to": sub.forward_dead_lettered_messages_to,
                        "created_at": runtime_props.created_at_utc,
                        "updated_at": runtime_props.updated_at_utc,
                        "accessed_at": runtime_props.accessed_at_utc,
                    }

        return generate_records(), {}

    def _peek_messages_with_retry(self, receiver, max_message_count, sequence_number, label="peek"):
        """Peek messages from a receiver with retry logic."""
        return _retry_with_backoff(
            lambda: receiver.peek_messages(
                max_message_count=max_message_count,
                sequence_number=sequence_number,
            ),
            max_retries=self.max_retries,
            operation_name=f"{label}(seq={sequence_number})",
        )

    def _peek_all_from_receiver(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        receiver,
        max_message_count: int,
        start_sequence: int,
        queue_name: str,
        records: list,
        label: str,
    ) -> int:
        """Peek all messages from a single receiver, appending to *records*.

        Returns the last sequence number seen (or *start_sequence* if no
        messages were found).
        """
        last_sequence = start_sequence
        page_count = 0

        messages = self._peek_messages_with_retry(
            receiver,
            max_message_count,
            start_sequence + 1 if start_sequence else 0,
            label=label,
        )

        while messages:
            page_count += 1
            for msg in messages:
                record = self._message_to_record(msg)
                record["queue_name"] = queue_name
                last_sequence = msg.sequence_number
                records.append(record)
                if self.debug_mode:
                    logger.debug(
                        "queue_messages[%s] seq=%d msg_id=%s",
                        queue_name,
                        msg.sequence_number,
                        msg.message_id,
                    )

            # Get next batch
            messages = self._peek_messages_with_retry(
                receiver,
                max_message_count,
                last_sequence + 1,
                label=label,
            )

        return last_sequence

    def _read_queue_messages(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages from a queue using peek (non-destructive).

        Supports both regular and session-enabled queues.  For session
        queues the connector iterates through available sessions using
        ``NEXT_AVAILABLE`` and peeks messages from each one.  An explicit
        ``session_id`` table option can be provided to peek from a single
        session only.
        """
        queue_name = table_options.get("queue_name")
        if not queue_name:
            raise ValueError("queue_name is required for queue_messages table")

        max_message_count = int(table_options.get("max_message_count", "100"))
        start_sequence = start_offset.get("sequence_number", 0)
        session_id = table_options.get("session_id")  # optional

        last_sequence = start_sequence
        records: list[dict] = []
        t_start = time.perf_counter()

        try:
            if session_id:
                # Explicit session requested
                with self._client.get_queue_receiver(
                    queue_name, session_id=session_id
                ) as receiver:
                    last_sequence = self._peek_all_from_receiver(
                        receiver, max_message_count, start_sequence,
                        queue_name, records,
                        label=f"peek_queue({queue_name}/session={session_id})",
                    )
            else:
                # Try a regular (non-session) receiver first
                try:
                    with self._client.get_queue_receiver(queue_name) as receiver:
                        last_sequence = self._peek_all_from_receiver(
                            receiver, max_message_count, start_sequence,
                            queue_name, records,
                            label=f"peek_queue({queue_name})",
                        )
                except ServiceBusError as e:
                    if "not possible for an entity that requires sessions" in str(e):
                        # Queue is session-enabled: iterate available sessions
                        logger.info(
                            "Queue '%s' requires sessions; iterating sessions",
                            queue_name,
                        )
                        records.clear()
                        last_sequence = start_sequence
                        seen_sessions: set[str] = set()
                        max_empty_attempts = 3
                        empty_attempts = 0

                        while empty_attempts < max_empty_attempts:
                            try:
                                with self._client.get_queue_receiver(
                                    queue_name,
                                    session_id=ServiceBusSessionFilter.NEXT_AVAILABLE,
                                    max_wait_time=self.max_wait_time,
                                ) as session_receiver:
                                    sid = session_receiver.session.session_id
                                    if sid in seen_sessions:
                                        empty_attempts += 1
                                        continue
                                    seen_sessions.add(sid)
                                    empty_attempts = 0

                                    last_sequence = self._peek_all_from_receiver(
                                        session_receiver,
                                        max_message_count,
                                        start_sequence,
                                        queue_name,
                                        records,
                                        label=f"peek_queue({queue_name}/session={sid})",
                                    )
                            except OperationTimeoutError:
                                # No more sessions available
                                break
                            except ServiceBusError as inner_e:
                                if "timeout" in str(inner_e).lower():
                                    break
                                raise

                        # Re-sort by sequence number so records are ordered
                        records.sort(key=lambda r: r["sequence_number"])
                    else:
                        raise

        except (ServiceBusAuthenticationError, ServiceBusAuthorizationError) as e:
            raise ValueError(
                f"Authentication/authorization failed reading queue '{queue_name}': {e}"
            ) from e
        except ServiceBusError as e:
            raise RuntimeError(
                f"Service Bus error reading queue '{queue_name}': {e}"
            ) from e

        elapsed = time.perf_counter() - t_start
        logger.info(
            "queue_messages[%s]: read %d records (%.2fs, %.0f msgs/sec)",
            queue_name,
            len(records),
            elapsed,
            len(records) / elapsed if elapsed > 0 else 0,
        )

        # Recompute last_sequence from actual records if we have any
        if records:
            last_sequence = max(r["sequence_number"] for r in records)

        return iter(records), {"sequence_number": last_sequence}

    def _read_subscription_messages(  # pylint: disable=too-many-locals
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages from a topic subscription using peek (non-destructive)."""
        topic_name = table_options.get("topic_name")
        subscription_name = table_options.get("subscription_name")

        if not topic_name or not subscription_name:
            raise ValueError(
                "topic_name and subscription_name are required for subscription_messages table"
            )

        max_message_count = int(table_options.get("max_message_count", "100"))
        start_sequence = start_offset.get("sequence_number", 0)

        last_sequence = start_sequence
        records = []
        page_count = 0
        t_start = time.perf_counter()

        try:
            with self._client.get_subscription_receiver(
                topic_name, subscription_name
            ) as receiver:
                messages = self._peek_messages_with_retry(
                    receiver,
                    max_message_count,
                    start_sequence + 1 if start_sequence else 0,
                    label=f"peek_sub({topic_name}/{subscription_name})",
                )

                while messages:
                    page_count += 1
                    for msg in messages:
                        record = self._message_to_record(msg)
                        record["topic_name"] = topic_name
                        record["subscription_name"] = subscription_name
                        last_sequence = msg.sequence_number
                        records.append(record)
                        if self.debug_mode:
                            logger.debug(
                                "subscription_messages[%s/%s] seq=%d msg_id=%s",
                                topic_name,
                                subscription_name,
                                msg.sequence_number,
                                msg.message_id,
                            )

                    # Get next batch
                    messages = self._peek_messages_with_retry(
                        receiver,
                        max_message_count,
                        last_sequence + 1,
                        label=f"peek_sub({topic_name}/{subscription_name})",
                    )
        except (ServiceBusAuthenticationError, ServiceBusAuthorizationError) as e:
            raise ValueError(
                f"Authentication/authorization failed reading "
                f"subscription '{topic_name}/{subscription_name}': {e}"
            ) from e
        except ServiceBusError as e:
            raise RuntimeError(
                f"Service Bus error reading subscription "
                f"'{topic_name}/{subscription_name}': {e}"
            ) from e

        elapsed = time.perf_counter() - t_start
        logger.info(
            "subscription_messages[%s/%s]: read %d records in %d pages (%.2fs, %.0f msgs/sec)",
            topic_name,
            subscription_name,
            len(records),
            page_count,
            elapsed,
            len(records) / elapsed if elapsed > 0 else 0,
        )

        return iter(records), {"sequence_number": last_sequence}

    def _read_dead_letter_messages(  # pylint: disable=too-many-locals
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read dead-lettered messages from queues or subscriptions."""
        source_type = table_options.get("source_type")
        if source_type not in ("queue", "subscription"):
            raise ValueError("source_type must be 'queue' or 'subscription'")

        max_message_count = int(table_options.get("max_message_count", "100"))
        start_sequence = start_offset.get("sequence_number", 0)

        last_sequence = start_sequence
        records = []
        page_count = 0
        t_start = time.perf_counter()

        if source_type == "queue":
            queue_name = table_options.get("queue_name")
            if not queue_name:
                raise ValueError(
                    "queue_name is required when source_type is 'queue'"
                )
            source_name = queue_name
            receiver_ctx = self._client.get_queue_receiver(
                queue_name,
                sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            )
        else:
            topic_name = table_options.get("topic_name")
            subscription_name = table_options.get("subscription_name")
            if not topic_name or not subscription_name:
                raise ValueError(
                    "topic_name and subscription_name are required when "
                    "source_type is 'subscription'"
                )
            source_name = f"{topic_name}/{subscription_name}"
            receiver_ctx = self._client.get_subscription_receiver(
                topic_name,
                subscription_name,
                sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            )

        try:
            with receiver_ctx as receiver:
                messages = self._peek_messages_with_retry(
                    receiver,
                    max_message_count,
                    start_sequence + 1 if start_sequence else 0,
                    label=f"peek_dlq({source_name})",
                )

                while messages:
                    page_count += 1
                    for msg in messages:
                        record = self._message_to_record(msg)
                        record["source_type"] = source_type
                        record["source_name"] = source_name
                        record["dead_letter_reason"] = msg.dead_letter_reason
                        record["dead_letter_error_description"] = (
                            msg.dead_letter_error_description
                        )
                        last_sequence = msg.sequence_number
                        records.append(record)
                        if self.debug_mode:
                            logger.debug(
                                "dead_letter_messages[%s] seq=%d msg_id=%s reason=%s",
                                source_name,
                                msg.sequence_number,
                                msg.message_id,
                                msg.dead_letter_reason,
                            )

                    # Get next batch
                    messages = self._peek_messages_with_retry(
                        receiver,
                        max_message_count,
                        last_sequence + 1,
                        label=f"peek_dlq({source_name})",
                    )
        except (ServiceBusAuthenticationError, ServiceBusAuthorizationError) as e:
            raise ValueError(
                f"Authentication/authorization failed reading dead letters from "
                f"'{source_name}': {e}"
            ) from e
        except ServiceBusError as e:
            raise RuntimeError(
                f"Service Bus error reading dead letters from "
                f"'{source_name}': {e}"
            ) from e

        elapsed = time.perf_counter() - t_start
        logger.info(
            "dead_letter_messages[%s]: read %d records in %d pages (%.2fs, %.0f msgs/sec)",
            source_name,
            len(records),
            page_count,
            elapsed,
            len(records) / elapsed if elapsed > 0 else 0,
        )

        return iter(records), {"sequence_number": last_sequence}

    def _message_to_record(self, msg: Any) -> dict:  # pylint: disable=too-many-branches
        """Convert a ServiceBusReceivedMessage to a dictionary record."""
        # Handle message body with content-type awareness
        body = None
        try:
            body_bytes = b"".join(msg.body)

            # Check max body size
            if self.max_body_size > 0 and len(body_bytes) > self.max_body_size:
                logger.warning(
                    "Message %s body size %d exceeds max_body_size %d, truncating",
                    msg.message_id,
                    len(body_bytes),
                    self.max_body_size,
                )
                body_bytes = body_bytes[: self.max_body_size]

            # Content-type-aware decoding
            content_type = (msg.content_type or "").lower()
            if "json" in content_type or "text" in content_type or "xml" in content_type:
                # Text-based content types: decode as UTF-8
                try:
                    body = body_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    body = base64.b64encode(body_bytes).decode("ascii")
            elif (
                content_type.startswith("application/octet-stream")
                or content_type.startswith("image/")
            ):
                # Binary content types: always base64 encode
                body = base64.b64encode(body_bytes).decode("ascii")
            else:
                # Unknown / unset content type: try UTF-8 first, fallback to base64
                try:
                    body = body_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    body = base64.b64encode(body_bytes).decode("ascii")
        except Exception as e:
            logger.warning(
                "Failed to read message body for msg_id=%s: %s", msg.message_id, e
            )
            body = None

        # Handle application properties
        app_props = None
        if msg.application_properties:
            try:
                props = {}
                for k, v in msg.application_properties.items():
                    key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    if isinstance(v, bytes):
                        try:
                            value = v.decode("utf-8")
                        except UnicodeDecodeError:
                            value = base64.b64encode(v).decode("ascii")
                    elif isinstance(v, (datetime,)):
                        value = v.isoformat()
                    elif v is None:
                        value = None
                    else:
                        value = v
                    props[key] = value
                app_props = json.dumps(props)
            except Exception as e:
                logger.warning(
                    "Failed to serialize application properties for msg_id=%s: %s",
                    msg.message_id,
                    e,
                )
                app_props = None

        return {
            "sequence_number": msg.sequence_number,
            "message_id": msg.message_id,
            "body": body,
            "content_type": msg.content_type,
            "correlation_id": msg.correlation_id,
            "subject": msg.subject,
            "reply_to": msg.reply_to,
            "reply_to_session_id": msg.reply_to_session_id,
            "to": msg.to,
            "time_to_live": str(msg.time_to_live) if msg.time_to_live else None,
            "session_id": msg.session_id,
            "partition_key": msg.partition_key,
            "scheduled_enqueue_time_utc": msg.scheduled_enqueue_time_utc,
            "enqueued_time_utc": msg.enqueued_time_utc,
            "expires_at_utc": msg.expires_at_utc,
            "enqueued_sequence_number": msg.enqueued_sequence_number,
            "dead_letter_source": msg.dead_letter_source,
            "delivery_count": msg.delivery_count,
            "lock_token": str(msg.lock_token) if msg.lock_token else None,
            "locked_until_utc": msg.locked_until_utc,
            "state": (
                str(msg.state) if hasattr(msg, "state") and msg.state else None
            ),
            "application_properties": app_props,
        }

    def close(self) -> None:
        """Close all clients and release resources."""
        self._closed = True
        errors = []
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                errors.append(f"ServiceBusClient.close(): {e}")
            finally:
                self._client = None
        if self._admin_client:
            try:
                self._admin_client.close()
            except Exception as e:
                errors.append(f"ServiceBusAdministrationClient.close(): {e}")
            finally:
                self._admin_client = None
        if self._credential and hasattr(self._credential, "close"):
            try:
                self._credential.close()
            except Exception as e:
                errors.append(f"Credential.close(): {e}")
            finally:
                self._credential = None
        if errors:
            logger.warning("Errors during close: %s", "; ".join(errors))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
