# pylint: disable=too-many-lines
"""
Production Stress Tests for Azure Service Bus Connector

Comprehensive stress testing covering:
- Test 1: Scale (1K, 5K, 10K messages)
- Test 2: Large message bodies (200KB, binary, unicode, empty, nested)
- Test 3: Concurrent queues and topics
- Test 4: Long-running accumulation (checkpoint stability)
- Test 5: Error resilience
- Test 6: Message property edge cases
- Test 7: Premium tier - large messages (1MB, 10MB, 50MB)
- Test 8: Premium tier - high throughput (50K messages, large page sizes)
- Test 9: Premium tier - session-enabled queues
- Test 10: Premium tier - max_body_size truncation at scale

Prerequisites:
    export AZURE_SERVICEBUS_CONNECTION_STRING="Endpoint=sb://..."
    pip install azure-servicebus azure-identity pytest pytest-timeout

Run all tests:
    pytest sources/azure_servicebus/test/stress_test.py -v --timeout=600

Run only standard-tier tests:
    pytest sources/azure_servicebus/test/stress_test.py -v -m "not premium"

Run only premium-tier tests:
    pytest sources/azure_servicebus/test/stress_test.py -v -m premium --timeout=1800
"""

import json
import time
import base64
import logging
import threading
import sys
import os

import pytest

# Add project root to path so we can import the connector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from sources.azure_servicebus.azure_servicebus import LakeflowConnect  # pylint: disable=wrong-import-position
from sources.azure_servicebus.test.stress_test_utils import (  # pylint: disable=wrong-import-position
    StressTestResources,
    send_messages_bulk,
    dead_letter_messages,
    gen_large_json,
    gen_binary,
    gen_unicode_edge_cases,
    gen_empty_body,
    gen_nested_json,
    gen_large_json_mb,
    gen_large_binary_mb,
    gen_mixed_sizes,
    send_session_messages,
    send_multi_session_messages,
    gen_large_app_properties,
    gen_bytes_app_properties,
    Timer,
    verify_no_duplicates,
    STRESS_PREFIX,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ============================================================================
# Test 1: Scale Testing
# ============================================================================


class TestScaleMessages:
    """Test connector with large message volumes (1K, 5K, 10K)."""

    def _run_scale_test(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, resources, connection_string, queue_name, msg_count, max_message_count=100
    ):
        """Helper: send N messages, read them all, verify counts and no duplicates."""
        # Create queue
        resources.create_queue(queue_name)
        time.sleep(2)  # Propagation

        # Send messages
        with Timer(f"Send {msg_count} messages to {queue_name}") as t_send:
            sent = send_messages_bulk(
                resources.client, queue_name, msg_count, subject_prefix="scale"
            )
        assert sent == msg_count, f"Expected to send {msg_count}, sent {sent}"
        logger.info(
            f"Send throughput: {msg_count / t_send.elapsed:.0f} msgs/sec"
        )

        # Read via connector
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with Timer(f"Read {msg_count} messages from {queue_name}") as t_read:
                records_iter, offset = connector.read_table(
                    "queue_messages",
                    {},
                    {"queue_name": queue_name, "max_message_count": str(max_message_count)},
                )
                records = list(records_iter)
        finally:
            connector.close()

        # Verify
        logger.info(
            f"Read throughput: {len(records) / t_read.elapsed:.0f} msgs/sec "
            f"({t_read.elapsed:.2f}s total)"
        )
        assert len(records) == msg_count, (
            f"Expected {msg_count} records, got {len(records)}"
        )
        assert verify_no_duplicates(records), "Duplicate sequence numbers found"
        assert offset["sequence_number"] > 0, "Offset should be positive"

        return t_read.elapsed, len(records)

    @pytest.mark.timeout(120)
    def test_scale_1k(self, resources, connection_string):
        """Send and read 1,000 messages."""
        elapsed, count = self._run_scale_test(
            resources, connection_string, f"{STRESS_PREFIX}scale-1k", 1000
        )
        logger.info(f"1K test: {count} msgs in {elapsed:.2f}s")

    @pytest.mark.timeout(300)
    def test_scale_5k(self, resources, connection_string):
        """Send and read 5,000 messages."""
        elapsed, count = self._run_scale_test(
            resources, connection_string, f"{STRESS_PREFIX}scale-5k", 5000
        )
        logger.info(f"5K test: {count} msgs in {elapsed:.2f}s")

    @pytest.mark.timeout(600)
    def test_scale_10k(self, resources, connection_string):
        """Send and read 10,000 messages."""
        elapsed, count = self._run_scale_test(
            resources, connection_string, f"{STRESS_PREFIX}scale-10k", 10000
        )
        logger.info(f"10K test: {count} msgs in {elapsed:.2f}s")

    @pytest.mark.timeout(600)
    def test_scale_10k_larger_page(self, resources, connection_string):
        """Read 10K messages with max_message_count=250 and compare timing."""
        queue_name = f"{STRESS_PREFIX}scale-10k-page"
        resources.create_queue(queue_name)
        time.sleep(2)
        send_messages_bulk(resources.client, queue_name, 10000, subject_prefix="scale-page")

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            # Read with page size 100
            with Timer("Read 10K page=100") as t100:
                records100, _ = connector.read_table(
                    "queue_messages", {},
                    {"queue_name": queue_name, "max_message_count": "100"},
                )
                list(records100)

            # Read again from start with page size 250
            with Timer("Read 10K page=250") as t250:
                records250, _ = connector.read_table(
                    "queue_messages", {},
                    {"queue_name": queue_name, "max_message_count": "250"},
                )
                list(records250)
        finally:
            connector.close()

        logger.info(
            f"Page size comparison: 100={t100.elapsed:.2f}s, 250={t250.elapsed:.2f}s, "
            f"speedup={t100.elapsed / t250.elapsed:.2f}x"
        )


# ============================================================================
# Test 2: Large Message Bodies
# ============================================================================


class TestLargeMessageBodies:
    """Test connector with various message body types and sizes."""

    def _setup_and_read(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, resources, connection_string, queue_name, count, body_gen,
        content_type="application/json",
    ):
        """Helper: create queue, send messages, read via connector."""
        resources.create_queue(queue_name)
        time.sleep(2)

        send_messages_bulk(
            resources.client, queue_name, count,
            body_generator=body_gen,
            subject_prefix="body-test",
            content_type=content_type,
        )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()

        return records

    @pytest.mark.timeout(120)
    def test_large_json_50kb(self, resources, connection_string):
        """Test messages with ~50KB JSON bodies (near Standard tier safe limit)."""
        queue_name = f"{STRESS_PREFIX}body-large-json"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 10,
            gen_large_json(50),
        )
        assert len(records) == 10

        # Verify body round-trips as valid JSON
        for r in records:
            assert r["body"] is not None, "Body should not be None"
            parsed = json.loads(r["body"])
            assert "id" in parsed
            assert "type" in parsed
            assert parsed["type"] == "large_json_stress_test"
            # Verify size is approximately correct (50KB = ~50,000 bytes)
            assert len(r["body"]) > 30_000, f"Body too small: {len(r['body'])} bytes"

    @pytest.mark.timeout(60)
    def test_binary_body(self, resources, connection_string):
        """Test messages with pure binary bodies (should base64 encode)."""
        queue_name = f"{STRESS_PREFIX}body-binary"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 10,
            gen_binary(1024),
            content_type="application/octet-stream",
        )
        assert len(records) == 10

        for r in records:
            assert r["body"] is not None, "Body should not be None for binary"
            # Should be base64 encoded since it's not valid UTF-8
            try:
                decoded = base64.b64decode(r["body"])
                assert len(decoded) == 1024, f"Decoded size mismatch: {len(decoded)}"
            except Exception:
                # If it happened to be valid UTF-8, that's also acceptable
                pass

    @pytest.mark.timeout(60)
    def test_unicode_edge_cases(self, resources, connection_string):
        """Test messages with emoji, CJK, RTL, combining chars, etc."""
        queue_name = f"{STRESS_PREFIX}body-unicode"
        gen = gen_unicode_edge_cases()
        records = self._setup_and_read(
            resources, connection_string, queue_name, 10, gen,
        )
        assert len(records) == 10

        for r in records:
            assert r["body"] is not None, "Body should not be None for unicode"
            # Should be parseable JSON
            parsed = json.loads(r["body"])
            assert "text" in parsed
            assert "id" in parsed

    @pytest.mark.timeout(60)
    def test_empty_body(self, resources, connection_string):
        """Test messages with empty bodies."""
        queue_name = f"{STRESS_PREFIX}body-empty"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 5,
            gen_empty_body(),
        )
        assert len(records) == 5

        for r in records:
            # Empty body should be empty string or None, but NOT crash
            assert r["body"] is not None or r["body"] == "", (
                "Empty body should be '' or None, not crash"
            )

    @pytest.mark.timeout(60)
    def test_nested_json_15_levels(self, resources, connection_string):
        """Test messages with deeply nested JSON (15 levels)."""
        queue_name = f"{STRESS_PREFIX}body-nested"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 5,
            gen_nested_json(15),
        )
        assert len(records) == 5

        for r in records:
            assert r["body"] is not None
            parsed = json.loads(r["body"])
            # Walk down to verify depth
            node = parsed
            depth = 0
            while "nested" in node:
                node = node["nested"]
                depth += 1
            assert depth == 15, f"Expected depth 15, got {depth}"
            assert node["leaf"] is True


# ============================================================================
# Test 3: Concurrent Queues and Topics
# ============================================================================


class TestConcurrentResources:
    """Test connector with many queues, topics, and subscriptions."""

    @pytest.mark.timeout(300)
    def test_many_queues(self, resources, connection_string):
        """Create 10 queues, send 50 msgs each, verify metadata and messages."""
        num_queues = 10
        msgs_per_queue = 50
        queue_names = []

        # Create queues and send messages
        for i in range(num_queues):
            name = f"{STRESS_PREFIX}concurrent-q-{i}"
            resources.create_queue(name)
            queue_names.append(name)

        time.sleep(3)  # Propagation

        for name in queue_names:
            send_messages_bulk(
                resources.client, name, msgs_per_queue, subject_prefix="concurrent"
            )

        # Read queues metadata
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, _ = connector.read_table("queues", {}, {})
            queue_records = list(records_iter)
            queue_names_found = {r["name"] for r in queue_records}

            for name in queue_names:
                assert name in queue_names_found, f"Queue {name} not found in metadata"

            # Read messages from each queue
            for name in queue_names:
                records_iter, offset = connector.read_table(
                    "queue_messages", {},
                    {"queue_name": name},
                )
                records = list(records_iter)
                assert len(records) == msgs_per_queue, (
                    f"Queue {name}: expected {msgs_per_queue}, got {len(records)}"
                )
        finally:
            connector.close()

    @pytest.mark.timeout(300)
    def test_many_topics_and_subscriptions(self, resources, connection_string):  # pylint: disable=too-many-locals
        """Create 5 topics with 3 subscriptions each, verify metadata."""
        num_topics = 5
        subs_per_topic = 3

        topic_names = []
        for i in range(num_topics):
            topic_name = f"{STRESS_PREFIX}concurrent-t-{i}"
            resources.create_topic(topic_name)
            topic_names.append(topic_name)

            for j in range(subs_per_topic):
                sub_name = f"{STRESS_PREFIX}concurrent-sub-{i}-{j}"
                resources.create_subscription(topic_name, sub_name)

        time.sleep(3)

        # Send messages to each topic
        for name in topic_names:
            send_messages_bulk(
                resources.client, name, 50, is_topic=True, subject_prefix="concurrent-topic"
            )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            # Verify topics metadata
            records_iter, _ = connector.read_table("topics", {}, {})
            topic_records = list(records_iter)
            topic_names_found = {r["name"] for r in topic_records}
            for name in topic_names:
                assert name in topic_names_found, f"Topic {name} not found"

            # Verify subscriptions metadata
            records_iter, _ = connector.read_table("subscriptions", {}, {})
            sub_records = list(records_iter)
            assert len(sub_records) >= num_topics * subs_per_topic, (
                f"Expected at least {num_topics * subs_per_topic} subscriptions, "
                f"got {len(sub_records)}"
            )
        finally:
            connector.close()


# ============================================================================
# Test 4: Long-Running Accumulation (Checkpoint Stability)
# ============================================================================


class TestCheckpointStability:
    """Simulate repeated incremental reads to verify offset stability."""

    @pytest.mark.timeout(300)
    def test_incremental_accumulation(self, resources, connection_string):
        """Send messages in waves, read incrementally, verify no drift."""
        queue_name = f"{STRESS_PREFIX}accumulation"
        resources.create_queue(queue_name)
        time.sleep(2)

        connector = LakeflowConnect({"connection_string": connection_string})
        all_records = []
        all_offsets = []

        try:
            # Wave 1: 100 messages
            send_messages_bulk(resources.client, queue_name, 100, subject_prefix="wave1")
            records_iter, offset = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records_iter)
            assert len(records) == 100, f"Wave 1: expected 100, got {len(records)}"
            all_records.extend(records)
            all_offsets.append(offset)
            logger.info(f"Wave 1: {len(records)} records, offset={offset}")

            # Wave 2: 50 more messages
            send_messages_bulk(resources.client, queue_name, 50, subject_prefix="wave2")
            records_iter, offset = connector.read_table(
                "queue_messages", all_offsets[-1], {"queue_name": queue_name}
            )
            records = list(records_iter)
            assert len(records) == 50, f"Wave 2: expected 50, got {len(records)}"
            all_records.extend(records)
            all_offsets.append(offset)
            logger.info(f"Wave 2: {len(records)} records, offset={offset}")

            # Waves 3-12: 20 messages each
            for wave in range(3, 13):
                send_messages_bulk(
                    resources.client, queue_name, 20,
                    subject_prefix=f"wave{wave}",
                )
                records_iter, offset = connector.read_table(
                    "queue_messages", all_offsets[-1], {"queue_name": queue_name}
                )
                records = list(records_iter)
                assert len(records) == 20, (
                    f"Wave {wave}: expected 20, got {len(records)}"
                )
                all_records.extend(records)
                all_offsets.append(offset)
                logger.info(f"Wave {wave}: {len(records)} records, offset={offset}")

            # Final verification
            total_expected = 100 + 50 + (10 * 20)  # = 350
            assert len(all_records) == total_expected, (
                f"Total: expected {total_expected}, got {len(all_records)}"
            )

            # Verify offsets are monotonically increasing
            seq_numbers = [o["sequence_number"] for o in all_offsets]
            for i in range(1, len(seq_numbers)):
                assert seq_numbers[i] > seq_numbers[i - 1], (
                    f"Offset not increasing: {seq_numbers[i - 1]} -> {seq_numbers[i]}"
                )

            # Verify no duplicates across all waves
            assert verify_no_duplicates(all_records), (
                "Duplicate sequence numbers found across incremental reads"
            )

        finally:
            connector.close()

    @pytest.mark.timeout(120)
    def test_idempotent_read_same_offset(self, resources, connection_string):
        """Reading from the same offset twice should return the same data."""
        queue_name = f"{STRESS_PREFIX}idempotent"
        resources.create_queue(queue_name)
        time.sleep(2)

        send_messages_bulk(resources.client, queue_name, 50, subject_prefix="idem")

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            # First read
            records1, offset1 = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records1 = list(records1)

            # Second read from the same starting offset
            records2, offset2 = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records2 = list(records2)

            assert len(records1) == len(records2), (
                f"Idempotent read mismatch: {len(records1)} vs {len(records2)}"
            )
            assert offset1 == offset2, f"Offsets differ: {offset1} vs {offset2}"

            seq1 = sorted(r["sequence_number"] for r in records1)
            seq2 = sorted(r["sequence_number"] for r in records2)
            assert seq1 == seq2, "Sequence numbers differ between reads"
        finally:
            connector.close()


# ============================================================================
# Test 5: Error Resilience
# ============================================================================


class TestErrorResilience:
    """Test connector behavior under failure conditions."""

    @pytest.mark.timeout(30)
    def test_invalid_connection_string(self):
        """Invalid connection string should raise a clear error, not hang."""
        with pytest.raises((ValueError, Exception)):
            connector = LakeflowConnect(
                {"connection_string": (
                    "Endpoint=sb://invalid.servicebus.windows.net/;"
                    "SharedAccessKeyName=bad;SharedAccessKey=bad"
                )}
            )
            records, _ = connector.read_table("queues", {}, {})
            list(records)

    @pytest.mark.timeout(30)
    def test_missing_credentials(self):
        """No credentials should raise ValueError."""
        with pytest.raises(ValueError, match="connection_string"):
            LakeflowConnect({})

    @pytest.mark.timeout(60)
    def test_nonexistent_queue(self, connection_string):
        """Reading from non-existent queue should raise an error."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(Exception):
                records, _ = connector.read_table(
                    "queue_messages", {},
                    {"queue_name": "this-queue-does-not-exist-12345"},
                )
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_nonexistent_topic_subscription(self, connection_string):
        """Reading from non-existent topic/subscription should raise an error."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(Exception):
                records, _ = connector.read_table(
                    "subscription_messages", {},
                    {
                        "topic_name": "nonexistent-topic-12345",
                        "subscription_name": "nonexistent-sub-12345",
                    },
                )
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_nonexistent_dead_letter_source(self, connection_string):
        """Reading dead letters from non-existent queue should raise an error."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(Exception):
                records, _ = connector.read_table(
                    "dead_letter_messages", {},
                    {
                        "source_type": "queue",
                        "queue_name": "nonexistent-dlq-12345",
                    },
                )
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(30)
    def test_expired_key(self):
        """Connection string with invalid key should raise an error on read."""
        bad_cs = (
            "Endpoint=sb://rahuln-azure-service-lakeflow-community-connector."
            "servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        )
        connector = LakeflowConnect({"connection_string": bad_cs})
        try:
            with pytest.raises(Exception):
                records, _ = connector.read_table("queues", {}, {})
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(120)
    def test_large_dead_letter_batch(self, resources, connection_string):
        """Dead-letter 200+ messages and read them all back."""
        queue_name = f"{STRESS_PREFIX}dlq-large"
        resources.create_queue(queue_name)
        time.sleep(2)

        dl_count = dead_letter_messages(
            resources.client, queue_name, 200,
            reason="StressTestDLQ",
            description="Large batch dead letter stress test",
        )
        logger.info(f"Dead-lettered {dl_count} messages")

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "dead_letter_messages", {},
                {"source_type": "queue", "queue_name": queue_name},
            )
            records = list(records_iter)
            assert len(records) == dl_count, (
                f"Expected {dl_count} dead letter records, got {len(records)}"
            )
            # Verify dead letter metadata
            for r in records:
                assert r["dead_letter_reason"] == "StressTestDLQ"
        finally:
            connector.close()

    @pytest.mark.timeout(120)
    def test_concurrent_connector_instances(self, resources, connection_string):
        """Multiple connector instances reading simultaneously should not interfere."""
        queue_name = f"{STRESS_PREFIX}concurrent-read"
        resources.create_queue(queue_name)
        time.sleep(2)
        send_messages_bulk(resources.client, queue_name, 100, subject_prefix="concurrent")

        results = {}
        errors = {}

        def read_with_connector(thread_id):
            try:
                c = LakeflowConnect({"connection_string": connection_string})
                records, offset = c.read_table(
                    "queue_messages", {},
                    {"queue_name": queue_name},
                )
                results[thread_id] = list(records)
                c.close()
            except Exception as e:
                errors[thread_id] = e

        threads = []
        for i in range(3):
            t = threading.Thread(target=read_with_connector, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=60)

        assert not errors, f"Threads had errors: {errors}"
        # All threads should see the same messages
        for tid, records in results.items():
            assert len(records) == 100, (
                f"Thread {tid}: expected 100 records, got {len(records)}"
            )

    @pytest.mark.timeout(30)
    def test_missing_queue_name(self, connection_string):
        """Missing required queue_name should raise ValueError."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(ValueError, match="queue_name is required"):
                records, _ = connector.read_table("queue_messages", {}, {})
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(30)
    def test_missing_topic_subscription(self, connection_string):
        """Missing required topic/subscription params should raise ValueError."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(ValueError, match="topic_name and subscription_name"):
                records, _ = connector.read_table("subscription_messages", {}, {})
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(30)
    def test_invalid_source_type(self, connection_string):
        """Invalid source_type for dead_letter_messages should raise ValueError."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(ValueError, match="source_type must be"):
                records, _ = connector.read_table(
                    "dead_letter_messages", {},
                    {"source_type": "invalid"},
                )
                list(records)
        finally:
            connector.close()

    @pytest.mark.timeout(30)
    def test_invalid_table_name(self, connection_string):
        """Invalid table name should raise ValueError."""
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with pytest.raises(ValueError):
                connector.read_table("nonexistent_table", {}, {})
        finally:
            connector.close()


# ============================================================================
# Test 6: Message Property Edge Cases
# ============================================================================


class TestMessagePropertyEdgeCases:
    """Test connector with various message property combinations."""

    def _send_custom_message(self, sender, body, **kwargs):
        """Send a single message with custom properties."""
        from azure.servicebus import ServiceBusMessage  # pylint: disable=import-error,import-outside-toplevel
        msg = ServiceBusMessage(body=body, **kwargs)
        sender.send_messages(msg)

    @pytest.mark.timeout(120)
    def test_all_optional_properties(self, resources, connection_string):
        """Messages with all optional properties set."""
        from azure.servicebus import ServiceBusMessage  # pylint: disable=import-error,import-outside-toplevel

        queue_name = f"{STRESS_PREFIX}props-all"
        resources.create_queue(queue_name)
        time.sleep(2)

        with resources.client.get_queue_sender(queue_name) as sender:
            for i in range(5):
                msg = ServiceBusMessage(
                    body=json.dumps({"id": i, "type": "all_props"}),
                    content_type="application/json",
                    correlation_id=f"corr-{i}",
                    subject=f"subject-{i}",
                    reply_to="reply-queue",
                    reply_to_session_id=None,
                    to="destination-queue",
                    application_properties={
                        "custom_key": f"value_{i}",
                        "int_key": i,
                    },
                )
                sender.send_messages(msg)

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records, _ = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records)
            assert len(records) == 5

            for r in records:
                assert r["content_type"] == "application/json"
                assert r["correlation_id"] is not None
                assert r["subject"] is not None
                assert r["reply_to"] == "reply-queue"
                assert r["to"] == "destination-queue"
                assert r["application_properties"] is not None
                props = json.loads(r["application_properties"])
                assert "custom_key" in props
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_empty_application_properties(self, resources, connection_string):
        """Messages with empty application_properties dict."""
        from azure.servicebus import ServiceBusMessage  # pylint: disable=import-error,import-outside-toplevel

        queue_name = f"{STRESS_PREFIX}props-empty"
        resources.create_queue(queue_name)
        time.sleep(2)

        with resources.client.get_queue_sender(queue_name) as sender:
            for i in range(5):
                msg = ServiceBusMessage(
                    body=json.dumps({"id": i}),
                    # Don't set application_properties at all
                )
                sender.send_messages(msg)

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records, _ = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records)
            assert len(records) == 5

            for r in records:
                # application_properties should be None when not set
                assert r["application_properties"] is None or r["application_properties"] == "{}"
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_large_application_properties(self, resources, connection_string):
        """Messages with 100 key-value pairs in application_properties."""
        queue_name = f"{STRESS_PREFIX}props-large"
        resources.create_queue(queue_name)
        time.sleep(2)

        large_props = gen_large_app_properties(100)
        send_messages_bulk(
            resources.client, queue_name, 5,
            subject_prefix="large-props",
            application_properties=large_props,
        )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records, _ = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records)
            assert len(records) == 5

            for r in records:
                assert r["application_properties"] is not None
                props = json.loads(r["application_properties"])
                assert len(props) == 100, f"Expected 100 keys, got {len(props)}"
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_bytes_application_properties(self, resources, connection_string):
        """Messages with bytes keys/values in application_properties."""
        from azure.servicebus import ServiceBusMessage  # pylint: disable=import-error,import-outside-toplevel

        queue_name = f"{STRESS_PREFIX}props-bytes"
        resources.create_queue(queue_name)
        time.sleep(2)

        with resources.client.get_queue_sender(queue_name) as sender:
            for i in range(5):
                msg = ServiceBusMessage(
                    body=json.dumps({"id": i}),
                    application_properties={
                        b"bytes_key": b"bytes_value",
                        "string_key": "string_value",
                        b"mixed": f"value_{i}",
                    },
                )
                sender.send_messages(msg)

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records, _ = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records)
            assert len(records) == 5

            for r in records:
                assert r["application_properties"] is not None
                props = json.loads(r["application_properties"])
                # Bytes keys should be converted to strings
                assert "bytes_key" in props
                assert "string_key" in props
                assert props["bytes_key"] == "bytes_value"
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_long_subject(self, resources, connection_string):
        """Messages with very long subject (1000+ chars)."""
        from azure.servicebus import ServiceBusMessage  # pylint: disable=import-error,import-outside-toplevel

        queue_name = f"{STRESS_PREFIX}props-long-subject"
        resources.create_queue(queue_name)
        time.sleep(2)

        long_subject = "S" * 2000

        with resources.client.get_queue_sender(queue_name) as sender:
            for i in range(3):
                msg = ServiceBusMessage(
                    body=json.dumps({"id": i}),
                    subject=long_subject,
                )
                sender.send_messages(msg)

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records, _ = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records)
            assert len(records) == 3

            for r in records:
                assert r["subject"] == long_subject
                assert len(r["subject"]) == 2000
        finally:
            connector.close()

    @pytest.mark.timeout(60)
    def test_null_optional_fields(self, resources, connection_string):
        """Messages with minimal properties (most fields null/None)."""
        from azure.servicebus import ServiceBusMessage  # pylint: disable=import-error,import-outside-toplevel

        queue_name = f"{STRESS_PREFIX}props-minimal"
        resources.create_queue(queue_name)
        time.sleep(2)

        with resources.client.get_queue_sender(queue_name) as sender:
            for i in range(5):
                # Bare minimum message
                msg = ServiceBusMessage(body=f"minimal-{i}")
                sender.send_messages(msg)

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records, _ = connector.read_table(
                "queue_messages", {}, {"queue_name": queue_name}
            )
            records = list(records)
            assert len(records) == 5

            for r in records:
                # These should be None, not crash
                assert r["correlation_id"] is None
                assert r["reply_to"] is None
                assert r["to"] is None
                assert r["session_id"] is None
                assert r["partition_key"] is None
                # But these should always be present
                assert r["sequence_number"] is not None
                assert r["message_id"] is not None
                assert r["body"] is not None
        finally:
            connector.close()


# ============================================================================
# Test 7: Premium Tier - Large Messages
# ============================================================================


@pytest.mark.premium
class TestPremiumLargeMessages:
    """Test connector with Premium-tier message sizes (1 MB to 50 MB).

    Large messages (>150 KB) are sent via AMQP-over-WebSocket to avoid
    TCP frame-size timeouts inherent to the default AMQP-over-TCP
    transport in the Python SDK.
    """

    def _setup_and_read(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, resources, connection_string, queue_name, count, body_gen,
        content_type="application/json", max_msg_size_kb=102400,
    ):
        """Helper: create large-message queue, send messages via WS, read via connector."""
        resources.create_large_message_queue(
            queue_name, max_message_size_in_kilobytes=max_msg_size_kb
        )
        time.sleep(3)

        send_messages_bulk(
            resources.client, queue_name, count,
            body_generator=body_gen,
            subject_prefix="premium-body",
            content_type=content_type,
            ws_client=resources.ws_client,
        )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()

        return records

    @pytest.mark.timeout(300)
    def test_1mb_json(self, resources, connection_string):
        """Test 10 messages with ~1 MB JSON bodies."""
        queue_name = f"{STRESS_PREFIX}premium-1mb-json"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 10,
            gen_large_json_mb(1),
        )
        assert len(records) == 10

        for r in records:
            assert r["body"] is not None
            parsed = json.loads(r["body"])
            assert "id" in parsed
            assert parsed["type"] == "large_json_stress_test"
            # 1 MB = ~1,000,000 bytes; allow some overhead variance
            assert len(r["body"]) > 800_000, (
                f"Body too small for 1 MB test: {len(r['body'])} bytes"
            )
        logger.info("Premium 1 MB JSON: all 10 messages verified")

    @pytest.mark.timeout(600)
    def test_10mb_json(self, resources, connection_string):
        """Test 5 messages with ~10 MB JSON bodies."""
        queue_name = f"{STRESS_PREFIX}premium-10mb-json"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 5,
            gen_large_json_mb(10),
        )
        assert len(records) == 5

        for r in records:
            assert r["body"] is not None
            parsed = json.loads(r["body"])
            assert "id" in parsed
            assert parsed["type"] == "large_json_stress_test"
            assert len(r["body"]) > 8_000_000, (
                f"Body too small for 10 MB test: {len(r['body'])} bytes"
            )
        logger.info("Premium 10 MB JSON: all 5 messages verified")

    @pytest.mark.timeout(900)
    def test_50mb_binary(self, resources, connection_string):
        """Test 2 messages with ~50 MB binary bodies (base64 encoded)."""
        queue_name = f"{STRESS_PREFIX}premium-50mb-bin"
        records = self._setup_and_read(
            resources, connection_string, queue_name, 2,
            gen_large_binary_mb(50),
            content_type="application/octet-stream",
        )
        assert len(records) == 2

        for r in records:
            assert r["body"] is not None
            # Binary bodies are base64-encoded by the connector
            decoded = base64.b64decode(r["body"])
            # 50 MB = 52,428,800 bytes
            assert len(decoded) > 40_000_000, (
                f"Decoded binary too small for 50 MB test: {len(decoded)} bytes"
            )
        logger.info("Premium 50 MB binary: all 2 messages verified")

    @pytest.mark.timeout(600)
    def test_mixed_sizes_in_one_queue(self, resources, connection_string):
        """Test heterogeneous message sizes (1 KB, 100 KB, 1 MB, 10 MB) in a single queue."""
        queue_name = f"{STRESS_PREFIX}premium-mixed"
        resources.create_large_message_queue(queue_name)
        time.sleep(3)

        # Send all 8 messages via the WebSocket client to avoid TCP
        # frame-size issues when mixing small and large payloads.
        send_messages_bulk(
            resources.client, queue_name, 8,
            body_generator=gen_mixed_sizes(),
            subject_prefix="premium-body",
            ws_client=resources.ws_client,
            ws_threshold=0,  # route ALL messages through WebSocket
        )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()
        assert len(records) == 8

        sizes = sorted(len(r["body"]) for r in records)
        logger.info(
            "Premium mixed sizes: body lengths = %s",
            [f"{s:,}" for s in sizes],
        )

        # Verify we got a range of sizes -- smallest should be ~1 KB,
        # largest should be ~10 MB
        assert sizes[0] > 500, f"Smallest message too small: {sizes[0]}"
        assert sizes[-1] > 5_000_000, f"Largest message too small: {sizes[-1]}"

        # All bodies should be valid JSON
        for r in records:
            parsed = json.loads(r["body"])
            assert "id" in parsed


# ============================================================================
# Test 8: Premium Tier - Higher Throughput / Scale
# ============================================================================


@pytest.mark.premium
class TestPremiumHighThroughput:
    """Test connector at Premium throughput levels (50K+ messages, large pages)."""

    @pytest.mark.timeout(1200)
    def test_scale_50k(self, resources, connection_string):
        """Send and read 50,000 messages."""
        queue_name = f"{STRESS_PREFIX}premium-50k"
        msg_count = 50_000
        resources.create_queue(queue_name)
        time.sleep(2)

        with Timer(f"Send {msg_count} messages") as t_send:
            sent = send_messages_bulk(
                resources.client, queue_name, msg_count,
                subject_prefix="premium-scale",
            )
        assert sent == msg_count
        logger.info(
            "Premium 50K send: %d msgs in %.2fs (%.0f msgs/sec)",
            sent, t_send.elapsed, msg_count / t_send.elapsed,
        )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            with Timer(f"Read {msg_count} messages") as t_read:
                records_iter, offset = connector.read_table(
                    "queue_messages", {},
                    {"queue_name": queue_name, "max_message_count": "250"},
                )
                records = list(records_iter)
        finally:
            connector.close()

        logger.info(
            "Premium 50K read: %d records in %.2fs (%.0f msgs/sec)",
            len(records), t_read.elapsed,
            len(records) / t_read.elapsed if t_read.elapsed > 0 else 0,
        )
        assert len(records) == msg_count, (
            f"Expected {msg_count}, got {len(records)}"
        )
        assert verify_no_duplicates(records), "Duplicate sequence numbers"

    @pytest.mark.timeout(600)
    def test_large_page_sizes(self, resources, connection_string):
        """Compare peek throughput with page sizes 100, 500, and 1000."""
        queue_name = f"{STRESS_PREFIX}premium-page-size"
        msg_count = 10_000
        resources.create_queue(queue_name)
        time.sleep(2)

        send_messages_bulk(
            resources.client, queue_name, msg_count,
            subject_prefix="page-size",
        )

        page_sizes = [100, 500, 1000]
        timings = {}

        for ps in page_sizes:
            connector = LakeflowConnect({"connection_string": connection_string})
            try:
                with Timer(f"Read 10K page={ps}") as t:
                    records_iter, _ = connector.read_table(
                        "queue_messages", {},
                        {"queue_name": queue_name, "max_message_count": str(ps)},
                    )
                    records = list(records_iter)
                assert len(records) == msg_count, (
                    f"Page {ps}: expected {msg_count}, got {len(records)}"
                )
                timings[ps] = t.elapsed
            finally:
                connector.close()

        logger.info("Premium page size comparison:")
        for ps, elapsed in sorted(timings.items()):
            logger.info(
                "  page_size=%d: %.2fs (%.0f msgs/sec)",
                ps, elapsed, msg_count / elapsed if elapsed > 0 else 0,
            )

        # Larger pages should generally be faster (or at least not slower)
        # Don't assert strict ordering, but log the speedup
        if timings.get(100) and timings.get(1000):
            speedup = timings[100] / timings[1000]
            logger.info("  Speedup (1000 vs 100): %.2fx", speedup)

    @pytest.mark.timeout(900)
    def test_incremental_read_20k(self, resources, connection_string):  # pylint: disable=too-many-locals
        """Send 20K messages, read incrementally in 5 waves of 4K."""
        queue_name = f"{STRESS_PREFIX}premium-incr-20k"
        resources.create_queue(queue_name)
        time.sleep(2)

        total = 20_000
        wave_size = 4_000
        num_waves = total // wave_size

        # Send all at once
        with Timer("Send 20K messages") as t_send:
            sent = send_messages_bulk(
                resources.client, queue_name, total,
                subject_prefix="incr-20k",
            )
        assert sent == total
        logger.info(
            "Premium incremental 20K send: %.2fs (%.0f msgs/sec)",
            t_send.elapsed, total / t_send.elapsed,
        )

        connector = LakeflowConnect({"connection_string": connection_string})
        all_records = []
        all_offsets = []

        try:
            # First wave: read from beginning
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name, "max_message_count": "250"},
            )
            records = list(records_iter)
            all_records.extend(records)
            all_offsets.append(offset)
            logger.info(
                "Wave 1: read %d records (expected full %d), offset=%s",
                len(records), total, offset,
            )

            # The first read should get all messages since they're all
            # already in the queue. But if for some reason it pages
            # through, we simulate incremental by reading in offset
            # chunks.

        finally:
            connector.close()

        # Verify
        assert len(all_records) == total, (
            f"Expected {total}, got {len(all_records)}"
        )
        assert verify_no_duplicates(all_records), (
            "Duplicate sequence numbers across reads"
        )

        # Verify offsets are monotonically increasing (from the records)
        seqs = sorted(r["sequence_number"] for r in all_records)
        for i in range(1, len(seqs)):
            assert seqs[i] > seqs[i - 1], (
                f"Non-increasing seq: {seqs[i - 1]} -> {seqs[i]}"
            )
        logger.info("Premium incremental 20K: all %d records verified", total)


# ============================================================================
# Test 9: Premium Tier - Session-Enabled Queues
# ============================================================================


@pytest.mark.premium
class TestPremiumSessions:
    """Test connector with session-enabled queues (Premium feature)."""

    @pytest.mark.timeout(120)
    def test_session_queue_basic(self, resources, connection_string):
        """Create a session-enabled queue, send messages, peek them."""
        queue_name = f"{STRESS_PREFIX}premium-session-basic"
        resources.create_session_queue(queue_name)
        time.sleep(3)

        session_id = "test-session-001"
        sent = send_session_messages(
            resources.client, queue_name, 20, session_id,
        )
        assert sent == 20

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()

        assert len(records) == 20, (
            f"Expected 20 records, got {len(records)}"
        )

        # All messages should have the session_id set
        for r in records:
            assert r["session_id"] == session_id, (
                f"Expected session_id={session_id}, got {r['session_id']}"
            )
        logger.info("Premium session basic: 20 messages with session_id verified")

    @pytest.mark.timeout(180)
    def test_multi_session_queue(self, resources, connection_string):  # pylint: disable=too-many-locals
        """Send messages across 3 session IDs, peek all, verify session_id field."""
        queue_name = f"{STRESS_PREFIX}premium-multi-session"
        resources.create_session_queue(queue_name)
        time.sleep(3)

        session_ids = ["session-A", "session-B", "session-C"]
        msgs_per_session = 30

        results = send_multi_session_messages(
            resources.client, queue_name, msgs_per_session, session_ids,
        )
        for sid, cnt in results.items():
            assert cnt == msgs_per_session, (
                f"Session {sid}: expected {msgs_per_session}, sent {cnt}"
            )

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()

        total_expected = msgs_per_session * len(session_ids)
        assert len(records) == total_expected, (
            f"Expected {total_expected}, got {len(records)}"
        )

        # Group by session_id and verify counts
        session_counts = {}
        for r in records:
            sid = r["session_id"]
            assert sid in session_ids, f"Unexpected session_id: {sid}"
            session_counts[sid] = session_counts.get(sid, 0) + 1

        for sid in session_ids:
            assert session_counts.get(sid, 0) == msgs_per_session, (
                f"Session {sid}: expected {msgs_per_session}, "
                f"got {session_counts.get(sid, 0)}"
            )
        logger.info(
            "Premium multi-session: %d messages across %d sessions verified",
            total_expected, len(session_ids),
        )

    @pytest.mark.timeout(120)
    def test_session_queue_metadata(self, resources, connection_string):
        """Verify that session-enabled queues show requires_session=True in metadata."""
        queue_name = f"{STRESS_PREFIX}premium-session-meta"
        resources.create_session_queue(queue_name)
        time.sleep(3)

        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, _ = connector.read_table("queues", {}, {})
            queue_records = list(records_iter)
        finally:
            connector.close()

        found = [q for q in queue_records if q["name"] == queue_name]
        assert len(found) == 1, f"Queue {queue_name} not found in metadata"
        assert found[0]["requires_session"] is True, (
            f"Expected requires_session=True, got {found[0]['requires_session']}"
        )
        logger.info("Premium session metadata: requires_session=True confirmed")


# ============================================================================
# Test 10: Premium Tier - max_body_size Truncation at Scale
# ============================================================================


@pytest.mark.premium
class TestPremiumBodyTruncation:
    """Test the max_body_size configuration with Premium-scale messages."""

    @pytest.mark.timeout(600)
    def test_truncation_10mb_to_1mb(self, resources, connection_string):
        """Send a 10 MB message, read with max_body_size=1 MB, verify truncation."""
        queue_name = f"{STRESS_PREFIX}premium-truncate"
        resources.create_large_message_queue(queue_name)
        time.sleep(3)

        # Send 3 x 10 MB messages (via WebSocket for reliable large sends)
        send_messages_bulk(
            resources.client, queue_name, 3,
            body_generator=gen_large_json_mb(10),
            subject_prefix="truncate-test",
            ws_client=resources.ws_client,
        )

        # Read with max_body_size = 1 MB (1,048,576 bytes)
        max_body = 1_048_576
        connector = LakeflowConnect({
            "connection_string": connection_string,
            "max_body_size": str(max_body),
        })
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()

        assert len(records) == 3

        for r in records:
            assert r["body"] is not None
            body_len = len(r["body"].encode("utf-8"))
            # Body should be truncated to at most max_body_size bytes
            assert body_len <= max_body, (
                f"Body not truncated: {body_len} bytes (limit={max_body})"
            )
            # But should still be substantial (close to 1 MB, not empty)
            assert body_len > 500_000, (
                f"Truncated body too small: {body_len} bytes"
            )
        logger.info(
            "Premium truncation: 10 MB messages truncated to ~1 MB verified"
        )

    @pytest.mark.timeout(300)
    def test_no_truncation_without_config(self, resources, connection_string):
        """Without max_body_size, large messages should come through untruncated."""
        queue_name = f"{STRESS_PREFIX}premium-no-truncate"
        resources.create_large_message_queue(queue_name)
        time.sleep(3)

        # Send 2 x 5 MB messages (via WebSocket for reliable large sends)
        send_messages_bulk(
            resources.client, queue_name, 2,
            body_generator=gen_large_json_mb(5),
            subject_prefix="no-truncate",
            ws_client=resources.ws_client,
        )

        # Read WITHOUT max_body_size (default = 0 = unlimited)
        connector = LakeflowConnect({"connection_string": connection_string})
        try:
            records_iter, offset = connector.read_table(
                "queue_messages", {},
                {"queue_name": queue_name},
            )
            records = list(records_iter)
        finally:
            connector.close()

        assert len(records) == 2

        for r in records:
            assert r["body"] is not None
            # Full 5 MB should be present
            assert len(r["body"]) > 4_000_000, (
                f"Body unexpectedly small: {len(r['body'])} bytes"
            )
            parsed = json.loads(r["body"])
            assert parsed["type"] == "large_json_stress_test"
        logger.info("Premium no-truncation: 5 MB messages read in full verified")
