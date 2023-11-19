from confluent_kafka import Consumer, KafkaException
import sys


if __name__ == '__main__':

    broker = 'localhost:9093'
    group = 'test'
    topics = ['test',]
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'enable.auto.offset.store': False}

    c = Consumer(conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    messages_counter = 0

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=10.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            elif msg.value().decode() == 'the end':
                print(f'Total messages received: {messages_counter}')
                c.store_offsets(msg)
                break
            else:
                print(msg.value().decode())
                messages_counter += 1
                c.store_offsets(msg)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()