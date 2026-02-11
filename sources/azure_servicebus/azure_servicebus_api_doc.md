# Azure Service Bus - Source API Documentation

## Overview

The Azure Service Bus connector uses two Azure SDK clients to interact with the Service Bus namespace:

1. **ServiceBusAdministrationClient** - For management operations (listing queues, topics, subscriptions, retrieving runtime properties)
2. **ServiceBusClient** - For message operations (peeking messages from queues and subscriptions)

### Base URL / Endpoint

- **Connection String**: Endpoint is embedded in the connection string (`Endpoint=sb://<namespace>.servicebus.windows.net/`)
- **Namespace URL**: `<namespace>.servicebus.windows.net`

### Authentication

| Method | SDK Class | Required Parameters |
|--------|-----------|-------------------|
| Connection String | `ServiceBusClient.from_connection_string()` | `connection_string` |
| Azure AD | `DefaultAzureCredential` | `fully_qualified_namespace` |
| Service Principal | `ClientSecretCredential` | `fully_qualified_namespace`, `azure_tenant_id`, `azure_client_id`, `azure_client_secret` |
| Managed Identity | `ManagedIdentityCredential` | `fully_qualified_namespace`, optionally `azure_client_id` |

### Required RBAC Roles (for Azure AD / Service Principal / Managed Identity)

| Role | Scope | Operations |
|------|-------|-----------|
| Reader | Namespace | List queues, topics, subscriptions |
| Azure Service Bus Data Receiver | Namespace | Peek messages |
| Azure Service Bus Data Owner | Namespace | Full access (alternative to above) |

---

## Endpoints / Operations

### 1. List Queues

**SDK Method**: `ServiceBusAdministrationClient.list_queues()`

Returns an iterable of `QueueProperties` objects for all queues in the namespace.

**Runtime Properties**: `ServiceBusAdministrationClient.get_queue_runtime_properties(queue_name)`

Returns `QueueRuntimeProperties` with message counts, sizes, and timestamps.

**Rate Limits**: Standard Azure Resource Manager throttling (read operations).

---

### 2. List Topics

**SDK Method**: `ServiceBusAdministrationClient.list_topics()`

Returns an iterable of `TopicProperties` objects for all topics in the namespace.

**Runtime Properties**: `ServiceBusAdministrationClient.get_topic_runtime_properties(topic_name)`

Returns `TopicRuntimeProperties` with subscription count, sizes, and timestamps.

---

### 3. List Subscriptions

**SDK Method**: `ServiceBusAdministrationClient.list_subscriptions(topic_name)`

Returns an iterable of `SubscriptionProperties` objects for subscriptions on a given topic.

**Runtime Properties**: `ServiceBusAdministrationClient.get_subscription_runtime_properties(topic_name, subscription_name)`

Returns `SubscriptionRuntimeProperties` with message counts and timestamps.

---

### 4. Peek Queue Messages

**SDK Method**: `ServiceBusReceiver.peek_messages(max_message_count, sequence_number)`

Peeks messages from a queue without removing them. This is non-destructive and does not affect message lock or delivery count.

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `max_message_count` | int | Maximum number of messages to peek (default: 1, max: varies) |
| `sequence_number` | long | Starting sequence number to peek from (0 = beginning) |

**Returns**: List of `ServiceBusReceivedMessage` objects.

**Important**: Peek does NOT consume messages. Messages remain in the queue and can be peeked again. The sequence_number is used for incremental reads.

---

### 5. Peek Subscription Messages

**SDK Method**: `ServiceBusReceiver.peek_messages(max_message_count, sequence_number)`

Same as queue peek but on a topic subscription receiver.

**Receiver creation**: `ServiceBusClient.get_subscription_receiver(topic_name, subscription_name)`

---

### 6. Peek Dead Letter Messages

**SDK Method**: `ServiceBusReceiver.peek_messages(max_message_count, sequence_number)`

Same as regular peek but using the dead-letter sub-queue.

**Queue Dead Letter Receiver**:
```python
ServiceBusClient.get_queue_receiver(
    queue_name,
    sub_queue=ServiceBusSubQueue.DEAD_LETTER
)
```

**Subscription Dead Letter Receiver**:
```python
ServiceBusClient.get_subscription_receiver(
    topic_name, subscription_name,
    sub_queue=ServiceBusSubQueue.DEAD_LETTER
)
```

---

## Schema Details

### QueueProperties Fields

| Field | Python Type | Spark Type | Notes |
|-------|-----------|-----------|-------|
| name | str | StringType | Queue name (primary key) |
| lock_duration | timedelta | StringType | ISO 8601 duration string |
| max_size_in_megabytes | int | LongType | Max size of queue |
| requires_duplicate_detection | bool | BooleanType | |
| requires_session | bool | BooleanType | |
| default_message_time_to_live | timedelta | StringType | ISO 8601 duration string |
| dead_lettering_on_message_expiration | bool | BooleanType | |
| duplicate_detection_history_time_window | timedelta | StringType | ISO 8601 duration string |
| max_delivery_count | int | LongType | |
| status | EntityStatus | StringType | Active, Disabled, etc. |
| enable_batched_operations | bool | BooleanType | |
| auto_delete_on_idle | timedelta | StringType | ISO 8601 duration string |
| enable_partitioning | bool | BooleanType | |

### QueueRuntimeProperties Additional Fields

| Field | Python Type | Spark Type |
|-------|-----------|-----------|
| total_message_count | int | LongType |
| size_in_bytes | int | LongType |
| created_at_utc | datetime | TimestampType |
| updated_at_utc | datetime | TimestampType |
| accessed_at_utc | datetime | TimestampType |

### ServiceBusReceivedMessage Fields

| Field | Python Type | Spark Type | Notes |
|-------|-----------|-----------|-------|
| sequence_number | int | LongType | Primary key, cursor field |
| message_id | str | StringType | Application-set ID |
| body | bytes/str | StringType | Decoded as UTF-8 or base64 |
| content_type | str | StringType | MIME type |
| correlation_id | str | StringType | |
| subject | str | StringType | Message label |
| reply_to | str | StringType | |
| reply_to_session_id | str | StringType | |
| to | str | StringType | |
| time_to_live | timedelta | StringType | ISO 8601 duration |
| session_id | str | StringType | |
| partition_key | str | StringType | |
| scheduled_enqueue_time_utc | datetime | TimestampType | |
| enqueued_time_utc | datetime | TimestampType | |
| expires_at_utc | datetime | TimestampType | |
| enqueued_sequence_number | int | LongType | |
| dead_letter_source | str | StringType | Original entity name |
| delivery_count | int | LongType | |
| lock_token | UUID | StringType | Converted to string |
| locked_until_utc | datetime | TimestampType | |
| state | MessageState | StringType | Active, Deferred, etc. |
| application_properties | dict | StringType | Serialized as JSON string |

### Dead Letter Additional Fields

| Field | Python Type | Spark Type |
|-------|-----------|-----------|
| dead_letter_reason | str | StringType |
| dead_letter_error_description | str | StringType |

---

## Incremental Read Strategy

### Cursor Field: `sequence_number`

The `sequence_number` is a monotonically increasing 64-bit integer assigned by Service Bus when a message is enqueued. It is unique per entity (queue/subscription).

### Read Flow

1. **Initial read**: Start with `sequence_number=0` to read from the beginning
2. **Subsequent reads**: Start from `last_sequence_number + 1`
3. **No more data signal**: When `peek_messages()` returns an empty list, all available messages have been read. The returned offset will match the input offset (same `sequence_number`), signaling to the framework that there's no more data.

### Peek vs Receive

This connector uses **peek** operations exclusively:
- `peek_messages()` does NOT lock or consume messages
- Messages remain available for receivers/consumers
- The same messages can be peeked repeatedly
- Safe for monitoring/analytics without affecting message processing pipelines

---

## Rate Limits & Throttling

Azure Service Bus has the following limits:

| Tier | Max Concurrent Connections | Max Messages/sec |
|------|--------------------------|-----------------|
| Basic | 100 | Varies |
| Standard | 1,000 | Varies |
| Premium | 10,000 | Varies |

The connector makes sequential peek calls per entity, so it generally stays well within limits. No explicit rate limiting is implemented.

---

## Known Quirks

1. **timedelta fields**: Python `timedelta` objects are serialized as ISO 8601 duration strings (e.g., "7 days, 0:00:00") since Spark doesn't have a native Duration type.

2. **Binary message bodies**: Messages with non-UTF-8 bodies are base64-encoded and stored as strings.

3. **Application properties**: Stored as a JSON string. Property keys that are bytes are decoded to UTF-8; datetime values are converted to ISO 8601 strings.

4. **Empty queues**: Peeking from an empty queue returns an empty list, not an error. The connector handles this gracefully.

5. **Sequence number gaps**: Sequence numbers may have gaps (e.g., after messages are consumed or dead-lettered). The connector handles this correctly by requesting from `last_sequence + 1`.

6. **Dead letter queue**: Uses `ServiceBusSubQueue.DEAD_LETTER` enum for proper dead-letter queue access.

---

## Premium Tier Testing

The connector is tested against Azure Service Bus **Premium tier** with the following capabilities:

### Message Size Limits by Tier

| Tier | Max Single Message | Max Batch Size | Sessions |
|------|--------------------|----------------|----------|
| Standard | 256 KB | ~1 MB | Yes |
| Premium | 100 MB (configurable per entity) | ~100 MB | Yes |

### Premium-Specific Test Coverage

| Test | What It Verifies |
|------|-----------------|
| 1 MB JSON body (10 msgs) | Large message round-trip, body integrity |
| 10 MB JSON body (5 msgs) | Memory handling for very large messages |
| 50 MB binary body (2 msgs) | Base64 encoding at Premium scale |
| Mixed sizes (1 KB - 10 MB) | Heterogeneous message handling in one read pass |
| 50K messages | High-volume ingest, no duplicates |
| Page sizes 100/500/1000 | Throughput comparison with larger peek batches |
| 20K incremental reads | Offset continuity at scale |
| Session-enabled queues | `session_id` field populated, multi-session peek |
| `max_body_size` truncation | 10 MB message truncated to 1 MB configurable limit |

### Running Premium Tests

```bash
# All premium tests
pytest sources/azure_servicebus/test/stress_test.py -v -m premium --timeout=1800

# Exclude premium (Standard-tier only)
pytest sources/azure_servicebus/test/stress_test.py -v -m "not premium"
```

### Notes

- The Premium tier default max message size per entity is **1 MB** unless reconfigured. The test utilities create queues with `max_message_size_in_kilobytes=102400` (100 MB) to exercise the full range.
- The 50 MB binary test produces ~67 MB base64-encoded strings; ensure sufficient memory on the test runner.
- The connector code itself is tier-agnostic — no code changes are needed between Standard and Premium.
