# Azure Service Bus - Lakeflow Community Connector

Ingest data from [Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/) into Databricks using the Lakeflow Community Connector framework.

## Overview

This connector enables you to read metadata and messages from Azure Service Bus namespaces, including queues, topics, subscriptions, and dead-letter queues. Messages are peeked (not consumed), making this safe for monitoring and analytics use cases.

## Supported Tables

| Table | Ingestion Type | Description |
|-------|---------------|-------------|
| `queues` | snapshot | List of queues in the namespace with properties and runtime statistics |
| `topics` | snapshot | List of topics in the namespace with properties and runtime statistics |
| `subscriptions` | snapshot | Subscriptions across topics with properties and runtime statistics |
| `queue_messages` | append | Messages from a specific queue (peeked, non-destructive) |
| `subscription_messages` | append | Messages from a topic subscription (peeked, non-destructive) |
| `dead_letter_messages` | append | Dead-lettered messages from queues or subscriptions |

## Prerequisites

1. **Azure Service Bus Namespace** (Standard or Premium tier)
2. **Python packages**:
   ```
   azure-servicebus>=7.11.0
   azure-identity>=1.15.0
   ```
3. **Credentials** (one of the following):
   - Connection string with SAS token
   - Azure AD credentials
   - Service principal credentials
   - Managed identity (when running in Azure)

## Authentication

### Option 1: Connection String (Recommended for Development)

Get your connection string from the Azure Portal:

1. Navigate to your Service Bus namespace
2. Go to **Shared access policies** > **RootManageSharedAccessKey**
3. Copy the **Primary Connection String**

```python
options = {
    "connection_string": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY"
}
```

### Option 2: Azure AD (DefaultAzureCredential)

Uses the Azure Identity SDK credential chain (CLI, environment variables, managed identity, etc.):

```python
options = {
    "fully_qualified_namespace": "your-namespace.servicebus.windows.net",
    "credential_type": "azure_ad"
}
```

Prerequisite: Run `az login` or set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET` environment variables.

### Option 3: Service Principal

```python
options = {
    "fully_qualified_namespace": "your-namespace.servicebus.windows.net",
    "credential_type": "service_principal",
    "azure_tenant_id": "YOUR_TENANT_ID",
    "azure_client_id": "YOUR_CLIENT_ID",
    "azure_client_secret": "YOUR_CLIENT_SECRET"
}
```

The service principal requires these RBAC roles on the Service Bus namespace:
- **Azure Service Bus Data Receiver** (for reading messages)
- **Reader** (for listing queues/topics/subscriptions)

### Option 4: Managed Identity

```python
options = {
    "fully_qualified_namespace": "your-namespace.servicebus.windows.net",
    "credential_type": "managed_identity",
    # Optional: for user-assigned managed identity
    "azure_client_id": "YOUR_MI_CLIENT_ID"
}
```

## Table Options

### queue_messages

| Option | Required | Description |
|--------|----------|-------------|
| `queue_name` | Yes | Name of the queue to read messages from |
| `max_message_count` | No | Maximum messages per batch (default: 100) |

### subscription_messages

| Option | Required | Description |
|--------|----------|-------------|
| `topic_name` | Yes | Name of the topic |
| `subscription_name` | Yes | Name of the subscription |
| `max_message_count` | No | Maximum messages per batch (default: 100) |

### dead_letter_messages

| Option | Required | Description |
|--------|----------|-------------|
| `source_type` | Yes | Source type: `queue` or `subscription` |
| `queue_name` | Conditional | Queue name (required when source_type=queue) |
| `topic_name` | Conditional | Topic name (required when source_type=subscription) |
| `subscription_name` | Conditional | Subscription name (required when source_type=subscription) |
| `max_message_count` | No | Maximum messages per batch (default: 100) |

### subscriptions

| Option | Required | Description |
|--------|----------|-------------|
| `topic_name` | No | Filter subscriptions by topic name |

## Incremental Sync

Message tables (`queue_messages`, `subscription_messages`, `dead_letter_messages`) use `sequence_number` as the cursor field. The connector peeks messages starting from the last known sequence number, ensuring no messages are re-read and no messages are consumed (removed) from the queue.

## Usage Example

### Local Testing

```python
from sources.azure_servicebus.azure_servicebus import LakeflowConnect

# Initialize
connector = LakeflowConnect({
    "connection_string": "Endpoint=sb://..."
})

# List tables
tables = connector.list_tables()
print(tables)

# Read queue metadata
records, offset = connector.read_table("queues", {}, {})
for record in records:
    print(record)

# Read queue messages incrementally
records, offset = connector.read_table("queue_messages", {}, {
    "queue_name": "my-queue",
    "max_message_count": "50"
})
for record in records:
    print(record["body"])

# Next batch from offset
records2, offset2 = connector.read_table("queue_messages", offset, {
    "queue_name": "my-queue",
})
```

### Pipeline Spec (SDP)

```python
from pipeline.ingestion_pipeline import ingest

ingest(
    source_name="azure_servicebus",
    connection_options={
        "connection_string": "{{secrets/scope/servicebus-connection-string}}"
    },
    tables=[
        {"name": "queues"},
        {"name": "topics"},
        {"name": "subscriptions"},
        {
            "name": "queue_messages",
            "options": {"queue_name": "my-queue"}
        },
    ]
)
```

## Schema Reference

### queues

| Field | Type | Description |
|-------|------|-------------|
| name | string | Queue name |
| lock_duration | string | Lock duration (ISO 8601) |
| max_size_in_megabytes | long | Maximum queue size |
| requires_duplicate_detection | boolean | Duplicate detection enabled |
| requires_session | boolean | Session support enabled |
| default_message_time_to_live | string | Default TTL (ISO 8601) |
| dead_lettering_on_message_expiration | boolean | Auto dead-letter on expiry |
| max_delivery_count | long | Max delivery attempts |
| status | string | Queue status |
| enable_partitioning | boolean | Partitioning enabled |
| message_count | long | Current message count |
| size_in_bytes | long | Current queue size |
| created_at | timestamp | Creation time |
| updated_at | timestamp | Last update time |
| accessed_at | timestamp | Last access time |

### Message Tables (queue_messages, subscription_messages, dead_letter_messages)

| Field | Type | Description |
|-------|------|-------------|
| sequence_number | long | Unique sequence number (cursor field) |
| message_id | string | Application-set message ID |
| body | string | Message body (UTF-8 or base64 encoded) |
| content_type | string | MIME content type |
| correlation_id | string | Correlation ID |
| subject | string | Message subject/label |
| enqueued_time_utc | timestamp | Time message was enqueued |
| expires_at_utc | timestamp | Expiration time |
| delivery_count | long | Number of delivery attempts |
| application_properties | string | Custom properties (JSON) |

## Testing

1. Set up test resources:
   ```bash
   python sources/azure_servicebus/setup_test_resources.py \
       --connection-string "Endpoint=sb://..."
   ```

2. Copy and fill in config files:
   ```bash
   cp sources/azure_servicebus/configs/dev_config.example.json \
      sources/azure_servicebus/configs/dev_config.json
   cp sources/azure_servicebus/configs/dev_table_config.example.json \
      sources/azure_servicebus/configs/dev_table_config.json
   # Edit the config files with your credentials
   ```

3. Run the generic test suite:
   ```bash
   pytest sources/azure_servicebus/test/test_azure_servicebus_lakeflow_connect.py::test_azure_servicebus_connector -v
   ```

4. Run all tests (including auth-specific tests):
   ```bash
   pytest sources/azure_servicebus/test/test_azure_servicebus_lakeflow_connect.py -v
   ```

5. Clean up test resources:
   ```bash
   python sources/azure_servicebus/setup_test_resources.py \
       --connection-string "Endpoint=sb://..." --cleanup
   ```

## Troubleshooting

### "No Azure credentials found"
Run `az login` or set the `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_CLIENT_SECRET` environment variables.

### "Authorization failed" / 403 errors
Assign the correct RBAC roles on the Service Bus namespace:
- **Azure Service Bus Data Receiver** for message operations
- **Reader** for management operations (listing queues, topics)

### "queue_name is required"
Message tables require table-specific options. Pass the relevant option in `table_options`.

### Messages not appearing
- Messages must exist in the queue/subscription before reading
- Use `setup_test_resources.py` to send test messages
- Peek operations may have a brief delay after messages are sent
