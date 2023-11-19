from confluent_kafka import Producer
import sys
from datetime import datetime

if __name__ == '__main__':

    broker = 'localhost:9093'
    topic = 'test'

    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def msg_generator(num_messages):
        for i in range(num_messages):
            yield f'message number: {i} - {datetime.now().strftime("%Y.%m.%d, %H:%M:%S %f")}'

    # Read lines from stdin, produce each line to Kafka
    for line in msg_generator(1000):
        try:
            # Produce line (without newline)
            p.produce(topic, line, callback=delivery_callback)

        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)
    p.produce(topic, 'the end', callback=delivery_callback)
    p.poll(0)
    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()