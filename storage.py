from elasticsearch import Elasticsearch


class ElasticStorage:
    DOC_INDEX = 'doc-storage'
    DOC_TYPE = 'doc'

    def __init__(self, addr, doc_index=DOC_INDEX):
        self._doc_index = doc_index
        self._client = Elasticsearch(addr)
        self._client.indices.create(index=doc_index, ignore=400)

    def save_document(self, doc):
        self._client.index(self._doc_index, self.DOC_TYPE, doc)
