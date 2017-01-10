import logging
import os
import time

from consumer import Consumer
from storage import ElasticStorage

ADDR = os.environ['ADDRESS']
KAFKA_ADDR = os.environ['KAFKA_ADDRESS']
DOC_QUEUE_TOPIC = os.environ['DOC_QUEUE_TOPIC']
ELASTIC_ADDRESS = os.environ['ELASTIC_ADDRESS']
DOC_INDEX_NAME = os.environ['DOC_INDEX_NAME']


class DocStorage(object):
    # TODO: add default values
    def __init__(self, kafka_addr, doc_queue_topic, elastic_address, elastic_index):
        self._link_consumer = Consumer(kafka_addr, doc_queue_topic)
        self._storage = ElasticStorage(elastic_address, elastic_index)
        self.serve()

    def serve(self):
        self._link_consumer.add_handler(self.handle_document)
        self._link_consumer.start()

    def handle_document(self, document):
        logging.log(logging.INFO, "Handle document")
        logging.log(logging.INFO, document)
        # TODO: add document preprocessing
        self._storage.save_document(document)


def main():
    DocStorage(KAFKA_ADDR, DOC_QUEUE_TOPIC, ELASTIC_ADDRESS, DOC_INDEX_NAME)

    # TODO: change to long time leave
    while True:
        time.sleep(20)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
