import json
import sys
import os
import time

import pika

URL = os.environ.get("AMQP_URL", "amqp://bria:bria@rabbitmq:5672/%2F")

def get_connection(retries=10, delay=1):
    for i in range(retries):
        try:
            return pika.BlockingConnection(pika.URLParameters(URL))
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(delay)

def publish(exchange, routing_key, body):
    conn = get_connection()
    ch = conn.channel()
    for durable in (False, True):
        try:
            ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=durable)
            break
        except Exception:
            if durable:
                ch = conn.channel()
            continue
    props = pika.BasicProperties(content_type="application/json", delivery_mode=2)
    ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=props)
    conn.close()
    print(json.dumps({"status": "published", "exchange": exchange, "routing_key": routing_key}), flush=True)

def consume(exchange, routing_key, timeout_secs=15):
    conn = get_connection()
    ch = conn.channel()
    for durable in (False, True):
        try:
            ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=durable)
            break
        except Exception:
            if durable:
                ch = conn.channel()
            continue
    result = ch.queue_declare(queue="", exclusive=True, auto_delete=True)
    queue_name = result.method.queue
    ch.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)
    messages = []

    def on_message(ch, method, properties, body):
        messages.append({"routing_key": method.routing_key, "body": body.decode()})
        ch.stop_consuming()

    ch.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)
    try:
        conn.call_later(timeout_secs, lambda: ch.stop_consuming() if ch.is_open else None)
        ch.start_consuming()
    except Exception:
        pass
    conn.close()
    print(json.dumps(messages), flush=True)

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "publish":
        publish(sys.argv[2], sys.argv[3], sys.stdin.read())
    elif cmd == "consume":
        timeout = int(sys.argv[4]) if len(sys.argv) > 4 else 15
        consume(sys.argv[2], sys.argv[3], timeout)
    else:
        print(json.dumps({"error": f"unknown command: {cmd}"}), flush=True)
        sys.exit(1)
