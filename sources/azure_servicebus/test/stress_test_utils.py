"""
Stress Test Utilities for Azure Service Bus Connector

Helpers for bulk message sending, resource creation/cleanup, and
message generators for various edge cases (large bodies, binary,
unicode, property edge cases).
"""

import json
import os
import time
import random
import string
import logging
from datetime import datetime, timezone
from typing import Optional

from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType
from azure.servicebus.management import ServiceBusAdministrationClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONNECTION_STRING_ENV = "AZURE_SERVICEBUS_CONNECTION_STRING"
STRESS_PREFIX = "stress-"
BATCH_SEND_LIMIT = 500  # Azure SDK limit per batch send

# Default threshold (bytes) above which messages are sent individually.
# Set to 5 MB for Premium tier; Standard tier users should lower to ~50 KB.
DEFAULT_LARGE_MSG_THRESHOLD = 5 * 1024 * 1024  # 5 MB


def get_connection_string() -> str:
    """Get connection string from environment or raise."""
    conn_str = os.environ.get(CONNECTION_STRING_ENV)
    if not conn_str:
        raise RuntimeError(
            f"Set {CONNECTION_STRING_ENV} environment variable with your "
            "Azure Service Bus connection string before running stress tests."
        )
    return conn_str


# ---------------------------------------------------------------------------
# Resource Management
# ---------------------------------------------------------------------------

class StressTestResources:
    """Create and clean up Azure Service Bus resources for stress testing."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.admin = ServiceBusAdministrationClient.from_connection_string(
            connection_string
        )
        self.client = ServiceBusClient.from_connection_string(connection_string)
        # WebSocket client for sending large messages (>~150 KB).  The default
        # AMQP-over-TCP transport struggles with payloads above ~150 KB due to
        # frame-size constraints, while AMQP-over-WebSocket handles MB-scale
        # messages reliably.
        self.ws_client = ServiceBusClient.from_connection_string(
            connection_string,
            transport_type=TransportType.AmqpOverWebsocket,
        )
        self._created_queues: list[str] = []
        self._created_topics: list[str] = []
        self._created_subscriptions: list[tuple[str, str]] = []

    def create_queue(self, name: str) -> str:
        """Create a queue, ignoring if it already exists."""
        try:
            self.admin.create_queue(name)
            logger.info(f"Created queue: {name}")
        except Exception as e:
            if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
                logger.info(f"Queue already exists: {name}")
            else:
                raise
        self._created_queues.append(name)
        return name

    def create_topic(self, name: str) -> str:
        """Create a topic, ignoring if it already exists."""
        try:
            self.admin.create_topic(name)
            logger.info(f"Created topic: {name}")
        except Exception as e:
            if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
                logger.info(f"Topic already exists: {name}")
            else:
                raise
        self._created_topics.append(name)
        return name

    def create_subscription(self, topic_name: str, sub_name: str) -> str:
        """Create a subscription on a topic, ignoring if it already exists."""
        try:
            self.admin.create_subscription(topic_name, sub_name)
            logger.info(f"Created subscription: {sub_name} on {topic_name}")
        except Exception as e:
            if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
                logger.info(f"Subscription already exists: {sub_name} on {topic_name}")
            else:
                raise
        self._created_subscriptions.append((topic_name, sub_name))
        return sub_name

    def create_session_queue(
        self, name: str, *, max_size_in_megabytes: int = 1024
    ) -> str:
        """Create a session-enabled queue, ignoring if it already exists."""
        try:
            self.admin.create_queue(
                name,
                requires_session=True,
                max_size_in_megabytes=max_size_in_megabytes,
            )
            logger.info(f"Created session queue: {name}")
        except Exception as e:
            if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
                logger.info(f"Session queue already exists: {name}")
            else:
                raise
        self._created_queues.append(name)
        return name

    def create_large_message_queue(
        self, name: str, max_message_size_in_kilobytes: int = 102400
    ) -> str:
        """Create a queue with a large max message size (Premium tier).

        Args:
            name: Queue name.
            max_message_size_in_kilobytes: Max single message size.
                Premium default is 1024 (1 MB); max is 102400 (100 MB).
        """
        try:
            self.admin.create_queue(
                name,
                max_message_size_in_kilobytes=max_message_size_in_kilobytes,
            )
            logger.info(
                f"Created large-message queue: {name} "
                f"(max_msg_size={max_message_size_in_kilobytes} KB)"
            )
        except Exception as e:
            if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
                logger.info(f"Queue already exists: {name}")
            else:
                raise
        self._created_queues.append(name)
        return name

    def cleanup(self):
        """Delete all resources created by this instance."""
        # Delete subscriptions first (must go before topics)
        for topic_name, sub_name in self._created_subscriptions:
            try:
                self.admin.delete_subscription(topic_name, sub_name)
                logger.info(f"Deleted subscription: {sub_name} from {topic_name}")
            except Exception:
                pass

        for name in self._created_topics:
            try:
                self.admin.delete_topic(name)
                logger.info(f"Deleted topic: {name}")
            except Exception:
                pass

        for name in self._created_queues:
            try:
                self.admin.delete_queue(name)
                logger.info(f"Deleted queue: {name}")
            except Exception:
                pass

        self._created_queues.clear()
        self._created_topics.clear()
        self._created_subscriptions.clear()

    def close(self):
        """Close clients."""
        try:
            self.client.close()
        except Exception:
            pass
        try:
            self.ws_client.close()
        except Exception:
            pass
        try:
            self.admin.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        self.close()
        return False


# ---------------------------------------------------------------------------
# Bulk Message Sending
# ---------------------------------------------------------------------------

def send_messages_bulk(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    client: ServiceBusClient,
    queue_or_topic: str,
    count: int,
    *,
    is_topic: bool = False,
    body_generator=None,
    subject_prefix: str = "stress",
    application_properties: Optional[dict] = None,
    content_type: str = "application/json",
    session_id: Optional[str] = None,
    large_msg_threshold: int = DEFAULT_LARGE_MSG_THRESHOLD,
    ws_client: Optional[ServiceBusClient] = None,
    ws_threshold: int = 150 * 1024,
) -> int:
    """
    Send messages in batches to a queue or topic.

    Args:
        client: ServiceBusClient instance (AMQP-over-TCP)
        queue_or_topic: name of the queue or topic
        count: total number of messages to send
        is_topic: True to send to topic, False for queue
        body_generator: callable(index) -> str/bytes, defaults to JSON
        subject_prefix: prefix for message subject
        application_properties: optional dict of app properties
        content_type: content type header
        session_id: optional session ID (required for session-enabled queues)
        large_msg_threshold: byte size above which messages are sent
            individually instead of batched (default: 5 MB for Premium tier)
        ws_client: optional ServiceBusClient using AMQP-over-WebSocket.
            When provided, messages exceeding *ws_threshold* bytes are sent
            via this client to avoid TCP frame-size timeouts.
        ws_threshold: byte size above which the *ws_client* is used instead
            of *client* (default: 150 KB).  Only takes effect when *ws_client*
            is provided.

    Returns:
        Number of messages successfully sent.
    """
    # Safe cumulative batch payload for AMQP-over-TCP.  The default TCP
    # transport on this namespace starts timing out on writes above
    # ~100-150 KB, so we flush the batch conservatively.
    max_batch_bytes_tcp = 50 * 1024  # 50 KB

    def _get_sender(svc_client):
        if is_topic:
            return svc_client.get_topic_sender(queue_or_topic)
        return svc_client.get_queue_sender(queue_or_topic)

    sent = 0

    # We keep the main sender open for the lifetime of the function.
    # If ws_client is provided we lazily open a second sender for large msgs.
    with _get_sender(client) as sender:
        ws_sender = None
        ws_sender_ctx = None
        try:
            batch: list = []
            batch_bytes = 0

            def _flush_batch():
                nonlocal sent, batch, batch_bytes
                if batch:
                    sender.send_messages(batch)
                    sent += len(batch)
                    batch = []
                    batch_bytes = 0

            for i in range(count):
                if body_generator:
                    body = body_generator(i)
                else:
                    body = json.dumps({
                        "id": i + 1,
                        "text": f"Stress test message {i + 1}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "batch_index": i,
                    })

                msg_kwargs = {
                    "body": body,
                    "content_type": content_type,
                    "subject": f"{subject_prefix}-{i + 1}",
                    "application_properties": application_properties,
                }
                if session_id is not None:
                    msg_kwargs["session_id"] = session_id

                msg = ServiceBusMessage(**msg_kwargs)

                msg_size = len(body) if isinstance(body, (str, bytes)) else 0

                # Route large messages through WebSocket client if available
                if ws_client and msg_size > ws_threshold:
                    _flush_batch()  # flush pending TCP batch first
                    if ws_sender is None:
                        ws_sender_ctx = _get_sender(ws_client)
                        ws_sender = ws_sender_ctx.__enter__()  # pylint: disable=unnecessary-dunder-call
                    ws_sender.send_messages(msg)
                    sent += 1
                elif msg_size > large_msg_threshold:
                    _flush_batch()
                    sender.send_messages(msg)
                    sent += 1
                else:
                    batch.append(msg)
                    batch_bytes += msg_size
                    if (
                        len(batch) >= BATCH_SEND_LIMIT
                        or batch_bytes >= max_batch_bytes_tcp
                    ):
                        _flush_batch()

            _flush_batch()
        finally:
            if ws_sender_ctx is not None:
                ws_sender_ctx.__exit__(None, None, None)

    return sent


def dead_letter_messages(
    client: ServiceBusClient,
    queue_name: str,
    count: int,
    reason: str = "StressTest",
    description: str = "Stress test dead letter",
) -> int:
    """
    Send messages to a queue, then receive and dead-letter them.

    Returns:
        Number of messages dead-lettered.
    """
    # First send
    send_messages_bulk(client, queue_name, count, subject_prefix="dl-stress")

    # Receive and dead-letter
    dead_lettered = 0
    with client.get_queue_receiver(queue_name, max_wait_time=10) as receiver:
        while dead_lettered < count:
            messages = receiver.receive_messages(
                max_message_count=min(100, count - dead_lettered),
                max_wait_time=10,
            )
            if not messages:
                break
            for msg in messages:
                receiver.dead_letter_message(
                    msg, reason=reason, error_description=description
                )
                dead_lettered += 1

    return dead_lettered


# ---------------------------------------------------------------------------
# Message Body Generators
# ---------------------------------------------------------------------------

def gen_large_json(size_kb: int = 200):
    """Generate a callable that produces a large JSON body of approximately size_kb KB."""
    def _generator(index: int) -> str:
        # Build a JSON object that's approximately the target size
        target_bytes = size_kb * 1024
        payload = {
            "id": index,
            "type": "large_json_stress_test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        # Fill with padding data
        padding_key_count = 0
        current_size = len(json.dumps(payload).encode("utf-8"))
        while current_size < target_bytes - 200:
            chunk_size = min(1024, target_bytes - current_size)
            payload[f"data_{padding_key_count}"] = "x" * chunk_size
            current_size += chunk_size + 20  # key overhead
            padding_key_count += 1
        return json.dumps(payload)
    return _generator


def gen_binary(size_bytes: int = 1024):
    """Generate a callable that produces random binary data."""
    def _generator(index: int) -> bytes:
        return os.urandom(size_bytes)
    return _generator


def gen_unicode_edge_cases():
    """Generate a callable that produces various unicode edge case strings."""
    cases = [
        # Emoji sequences
        "Hello \U0001F600\U0001F4A9\U0001F3F3\uFE0F\u200D\U0001F308 World",
        # CJK characters
        (
            "\u4F60\u597D\u4E16\u754C Hello "
            "\u3053\u3093\u306B\u3061\u306F \uC548\uB155\uD558\uC138\uC694"
        ),
        # RTL text (Arabic + Hebrew)
        (
            "\u0645\u0631\u062D\u0628\u0627 \u0628\u0627\u0644\u0639\u0627\u0644\u0645 "
            "\u05E9\u05DC\u05D5\u05DD \u05E2\u05D5\u05DC\u05DD"
        ),
        # Multi-byte UTF-8 (4-byte characters)
        (
            "\U0001F1FA\U0001F1F8 \U0001F1EE\U0001F1F3 "
            "\U0001F1EC\U0001F1E7 \U00010000\U00010001\U00010002"
        ),
        # Null bytes embedded in text (should handle gracefully)
        "before\x00after\x00end",
        # Mixed scripts
        (
            "English \u0420\u0443\u0441\u0441\u043A\u0438\u0439 "
            "\u0E44\u0E17\u0E22 \u0939\u093F\u0928\u094D\u0926\u0940"
        ),
        # Very long single line
        "A" * 10000,
        # Special JSON characters
        'He said "hello" and\\then <script>alert(1)</script> & more \t\n\r',
        # Mathematical symbols
        "\u2200x \u2208 \u211D: x\u00B2 \u2265 0, \u222B f(x)dx = \u03C0",
        # Combining characters and diacritics
        "a\u0300 e\u0301 n\u0303 o\u0308 u\u0302 Za\u0331lg\u033Do\u0335",
    ]

    def _generator(index: int) -> str:
        return json.dumps({
            "id": index,
            "text": cases[index % len(cases)],
            "case_index": index % len(cases),
        })
    return _generator


def gen_empty_body():
    """Generate a callable that produces empty bodies."""
    def _generator(index: int) -> str:
        return ""
    return _generator


def gen_nested_json(depth: int = 15):
    """Generate a callable that produces deeply nested JSON."""
    def _generator(index: int) -> str:
        obj = {"id": index, "leaf": True, "value": f"data-{index}"}
        for level in range(depth):
            obj = {"level": level, "nested": obj}
        return json.dumps(obj)
    return _generator


def gen_large_json_mb(size_mb: int = 1):
    """Generate a callable that produces a large JSON body of approximately *size_mb* MB.

    Designed for Premium tier testing where message sizes up to 100 MB are
    supported.  Uses 4 KB padding chunks to keep the generator fast even at
    50 MB.
    """
    size_kb = size_mb * 1024
    return gen_large_json(size_kb)


def gen_large_binary_mb(size_mb: int = 1):
    """Generate a callable that produces random binary data of *size_mb* MB.

    Returns bytes, not str — the connector should base64-encode these.
    """
    size_bytes = size_mb * 1024 * 1024
    return gen_binary(size_bytes)


def gen_mixed_sizes():
    """Generate a callable that produces messages of varying sizes.

    Cycle through 1 KB, 100 KB, 1 MB, and 10 MB JSON bodies.
    Useful for verifying the connector handles heterogeneous message sizes
    in a single read pass.
    """
    size_cycle_kb = [1, 100, 1024, 10240]  # 1 KB, 100 KB, 1 MB, 10 MB

    def _generator(index: int) -> str:
        target_kb = size_cycle_kb[index % len(size_cycle_kb)]
        inner_gen = gen_large_json(target_kb)
        return inner_gen(index)

    return _generator


# ---------------------------------------------------------------------------
# Session Message Helpers
# ---------------------------------------------------------------------------

def send_session_messages(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    client: ServiceBusClient,
    queue_name: str,
    count: int,
    session_id: str,
    *,
    body_generator=None,
    subject_prefix: str = "session",
) -> int:
    """Send messages with a specific session_id to a session-enabled queue.

    This is a convenience wrapper around :func:`send_messages_bulk` with the
    ``session_id`` parameter set.
    """
    return send_messages_bulk(
        client,
        queue_name,
        count,
        body_generator=body_generator,
        subject_prefix=subject_prefix,
        session_id=session_id,
    )


def send_multi_session_messages(
    client: ServiceBusClient,
    queue_name: str,
    count_per_session: int,
    session_ids: list[str],
    *,
    subject_prefix: str = "multi-session",
) -> dict[str, int]:
    """Send messages across multiple session IDs to a session-enabled queue.

    Returns:
        Dict mapping session_id -> number of messages sent.
    """
    results = {}
    for sid in session_ids:
        sent = send_session_messages(
            client, queue_name, count_per_session, sid,
            subject_prefix=f"{subject_prefix}-{sid}",
        )
        results[sid] = sent
    return results


# ---------------------------------------------------------------------------
# Message Property Generators
# ---------------------------------------------------------------------------

def gen_all_optional_properties():
    """Return kwargs for a message with all optional properties set."""
    return {
        "correlation_id": "stress-correlation-001",
        "reply_to": "stress-reply-queue",
        "reply_to_session_id": "stress-session-001",
        "to": "stress-destination",
        "time_to_live": 3600,  # 1 hour in seconds
        "subject": "stress-all-props",
        "content_type": "application/json",
        "message_id": None,  # Let SDK auto-generate
    }


def gen_large_app_properties(num_keys: int = 100) -> dict:
    """Generate application_properties with many key-value pairs."""
    props = {}
    for i in range(num_keys):
        props[f"stress_key_{i:04d}"] = f"value_{i:04d}_{'x' * 50}"
    return props


def gen_bytes_app_properties() -> dict:
    """Generate application_properties with bytes keys and values."""
    return {
        b"bytes_key_1": b"bytes_value_1",
        b"bytes_key_2": b"bytes_value_2",
        "string_key": "string_value",
        b"mixed_key": "mixed_value",
    }


# ---------------------------------------------------------------------------
# Timing Helpers
# ---------------------------------------------------------------------------

class Timer:
    """Simple context manager for timing operations."""

    def __init__(self, label: str = ""):
        self.label = label
        self.start_time = 0.0
        self.elapsed = 0.0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.start_time
        if self.label:
            logger.info(f"[Timer] {self.label}: {self.elapsed:.2f}s")

    @property
    def elapsed_ms(self) -> float:
        return self.elapsed * 1000


# ---------------------------------------------------------------------------
# Verification Helpers
# ---------------------------------------------------------------------------

def verify_no_duplicates(records: list[dict], key: str = "sequence_number") -> bool:
    """Verify no duplicate values for the given key in records."""
    values = [r[key] for r in records]
    return len(values) == len(set(values))


def verify_sequence_contiguous(records: list[dict], key: str = "sequence_number") -> bool:
    """Verify sequence numbers are contiguous (no gaps within the returned set)."""
    values = sorted(r[key] for r in records)
    if len(values) <= 1:
        return True
    for i in range(1, len(values)):
        if values[i] != values[i - 1] + 1:
            return False
    return True


def verify_all_fields_present(records: list[dict], required_fields: list[str]) -> list[str]:
    """Return list of missing fields across all records."""
    missing = []
    for i, record in enumerate(records):
        for field in required_fields:
            if field not in record:
                missing.append(f"Record {i} missing field: {field}")
    return missing
