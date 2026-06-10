FROM python:3-alpine
RUN pip install --no-cache-dir pika
COPY amqp-helper.py /scripts/amqp-helper.py
CMD ["sh", "-c", "trap 'exit 0' TERM; touch /tmp/ready; while true; do sleep 3600; done"]
