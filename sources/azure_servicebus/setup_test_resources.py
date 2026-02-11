"""
Setup Test Resources for Azure Service Bus Connector Testing

This script creates the necessary Azure Service Bus resources (queue, topic,
subscription) and sends test messages so you can run the connector tests.

Prerequisites:
    pip install azure-servicebus azure-identity

Usage:
    # Using connection string:
    python setup_test_resources.py --connection-string "Endpoint=sb://..."

    # Using Azure AD (must be logged in with 'az login'):
    python setup_test_resources.py --namespace "your-namespace.servicebus.windows.net"

    # Clean up resources when done:
    python setup_test_resources.py --connection-string "Endpoint=sb://..." --cleanup
"""

import argparse
import json
import sys
import time
from datetime import datetime

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient


def create_resources(admin_client: ServiceBusAdministrationClient) -> dict:
    """Create test queue, topic, and subscription."""
    resources = {
        "queue_name": "lakeflow-test-queue",
        "topic_name": "lakeflow-test-topic",
        "subscription_name": "lakeflow-test-subscription",
    }

    # Create test queue
    try:
        admin_client.create_queue(resources["queue_name"])
        print(f"  Created queue: {resources['queue_name']}")
    except Exception as e:
        if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
            print(f"  Queue already exists: {resources['queue_name']}")
        else:
            raise

    # Create test topic
    try:
        admin_client.create_topic(resources["topic_name"])
        print(f"  Created topic: {resources['topic_name']}")
    except Exception as e:
        if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
            print(f"  Topic already exists: {resources['topic_name']}")
        else:
            raise

    # Create test subscription
    try:
        admin_client.create_subscription(
            resources["topic_name"], resources["subscription_name"]
        )
        print(f"  Created subscription: {resources['subscription_name']} on topic {resources['topic_name']}")
    except Exception as e:
        if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
            print(
                f"  Subscription already exists: {resources['subscription_name']} "
                f"on topic {resources['topic_name']}"
            )
        else:
            raise

    return resources


def send_test_messages(
    client: ServiceBusClient,
    queue_name: str,
    topic_name: str,
    num_messages: int = 5,
) -> None:
    """Send test messages to the queue and topic."""
    # Send messages to queue
    with client.get_queue_sender(queue_name) as sender:
        for i in range(num_messages):
            msg = ServiceBusMessage(
                body=json.dumps(
                    {
                        "id": i + 1,
                        "text": f"Test message {i + 1}",
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "setup_test_resources",
                    }
                ),
                content_type="application/json",
                subject=f"test-{i + 1}",
                application_properties={
                    "test_run": "lakeflow-connector-test",
                    "message_number": i + 1,
                },
            )
            sender.send_messages(msg)
        print(f"  Sent {num_messages} messages to queue: {queue_name}")

    # Send messages to topic
    with client.get_topic_sender(topic_name) as sender:
        for i in range(num_messages):
            msg = ServiceBusMessage(
                body=json.dumps(
                    {
                        "id": i + 1,
                        "text": f"Test topic message {i + 1}",
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "setup_test_resources",
                    }
                ),
                content_type="application/json",
                subject=f"test-topic-{i + 1}",
                application_properties={
                    "test_run": "lakeflow-connector-test",
                    "message_number": i + 1,
                },
            )
            sender.send_messages(msg)
        print(f"  Sent {num_messages} messages to topic: {topic_name}")


def cleanup_resources(admin_client: ServiceBusAdministrationClient) -> None:
    """Remove test resources."""
    resources = [
        ("subscription", "lakeflow-test-topic", "lakeflow-test-subscription"),
        ("topic", "lakeflow-test-topic", None),
        ("queue", "lakeflow-test-queue", None),
    ]

    for resource_type, name, sub_name in resources:
        try:
            if resource_type == "subscription":
                admin_client.delete_subscription(name, sub_name)
                print(f"  Deleted subscription: {sub_name} from topic {name}")
            elif resource_type == "topic":
                admin_client.delete_topic(name)
                print(f"  Deleted topic: {name}")
            elif resource_type == "queue":
                admin_client.delete_queue(name)
                print(f"  Deleted queue: {name}")
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                print(f"  {resource_type} not found (already deleted): {name}")
            else:
                print(f"  Failed to delete {resource_type} {name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Set up Azure Service Bus test resources"
    )
    parser.add_argument(
        "--connection-string", help="Service Bus connection string"
    )
    parser.add_argument(
        "--namespace",
        help="Fully qualified namespace (e.g., myns.servicebus.windows.net)",
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Remove test resources"
    )
    parser.add_argument(
        "--num-messages",
        type=int,
        default=5,
        help="Number of test messages to send (default: 5)",
    )

    args = parser.parse_args()

    if not args.connection_string and not args.namespace:
        print("Error: Either --connection-string or --namespace is required")
        sys.exit(1)

    # Create admin client
    if args.connection_string:
        admin_client = ServiceBusAdministrationClient.from_connection_string(
            args.connection_string
        )
        sb_client = ServiceBusClient.from_connection_string(
            args.connection_string
        )
    else:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()
        admin_client = ServiceBusAdministrationClient(
            args.namespace, credential
        )
        sb_client = ServiceBusClient(args.namespace, credential)

    if args.cleanup:
        print("\nCleaning up test resources...")
        cleanup_resources(admin_client)
        print("\nCleanup complete!")
    else:
        print("\nCreating test resources...")
        resources = create_resources(admin_client)

        # Brief pause to let resources propagate
        time.sleep(2)

        print("\nSending test messages...")
        send_test_messages(
            sb_client,
            resources["queue_name"],
            resources["topic_name"],
            args.num_messages,
        )

        print("\nSetup complete! Use these values in your dev_config.json:")
        print(json.dumps(resources, indent=2))
        print(
            "\nNote: Messages sent to the queue are available for peeking immediately."
        )

    admin_client.close()
    sb_client.close()


if __name__ == "__main__":
    main()
