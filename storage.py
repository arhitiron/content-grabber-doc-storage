from elasticsearch import Elasticsearch


class ElasticStorage:
    DOC_INDEX = 'doc-storage'
    DOC_TYPE = 'doc'

    def __init__(self, addr):
        self._client = Elasticsearch(addr)
        self._client.indices.create(index=self.DOC_INDEX, ignore=400)

    def save_document(self, doc):
        self._client.index(self.DOC_INDEX, self.DOC_TYPE, doc)
