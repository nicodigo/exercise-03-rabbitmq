"""
Exercise 03 — Event Consumer

Implement a RabbitMQ consumer that:
- Connects to RabbitMQ at RABBITMQ_URL env var
- Consumes messages from the "node_events" queue
- Logs each event to stdout: "EVENT: {event} | node: {node_name} | time: {timestamp}"
- Acknowledges each message after processing
"""

import json
import logging
import os
import time

import pika

logger = logging.getLogger(__name__)


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        event = data.get("event", "unknown")
        node_name = data.get("node_name", "unknown")
        timestamp = data.get("timestamp", "unknown")
        print(f"EVENT: {event} | node: {node_name} | time: {timestamp}")
    except Exception as exc:
        logger.error("Failed to process message: %s", exc)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    for attempt in range(1, 11):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(url))
            break
        except Exception as exc:
            logger.warning(
                "RabbitMQ not ready (attempt %d/10): %s", attempt, exc
            )
            if attempt < 10:
                time.sleep(3)
    else:
        logger.error("Could not connect to RabbitMQ after 10 attempts")
        return

    channel = connection.channel()
    channel.queue_declare(queue="node_events", durable=False)
    channel.basic_consume(
        queue="node_events", on_message_callback=callback
    )
    print(" [*] Waiting for messages. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    main()
