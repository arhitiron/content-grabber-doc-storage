import json
import logging
import threading
import time
import sys
from kafka import KafkaConsumer


class Consumer(threading.Thread):
    daemon = True

    def __init__(self, server, topic):
        super(Consumer, self).__init__()
        self._kafka_server = server
        self._kafka_topic = topic
        self._handlers = []

    def add_handler(self, handler):
        self._handlers.append(handler)

    def _consumer_optimistic_init(self):
        try:
            consumer = KafkaConsumer(bootstrap_servers=self._kafka_server,
                                     auto_offset_reset='earliest',
                                     group_id='doc-storage',
                                     value_deserializer=lambda m: json.loads(m))
            return consumer
        except:
            time.sleep(1)
            print "Unexpected error:", sys.exc_info()
            return self._consumer_optimistic_init()

    def run(self):
        consumer = self._consumer_optimistic_init()
        consumer.subscribe([self._kafka_topic])
        logging.log(logging.INFO, "self._handlers")
        logging.log(logging.INFO, self._handlers)
        for msg in consumer:
            logging.log(logging.INFO, "New message")
            logging.log(logging.INFO, msg)
            val = msg.value
            # TODO: not sure that it's a good idea work without interface, maybe will be better change implementation
            for handler in self._handlers:
                handler(val)
