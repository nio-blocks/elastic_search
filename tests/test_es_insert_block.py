import logging

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase
from ..es_insert_block import ESInsert
from ..tests import elasticsearch_running, delete_elasticsearch_index


class TestESInsert(NIOBlockTestCase):
    """ Tests elasticsearch block insert functionality provided by
    ESInsert class
    """

    def setUp(self):
        self._outcome.success = elasticsearch_running()
        if self._outcome.success:
            super().setUp()

    def test_search(self):
        blk = ESInsert()
        index = "test_index"
        doc_type = "test_type"

        # make sure document doesn't exist
        delete_elasticsearch_index(index)

        self.configure_block(blk, {
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        self._assert_hit(blk, doc_type)

        blk.stop()

    def _assert_hit(self, blk, doc_type):
        # wait before asserting, it is possible that api wrapper
        # executes the request asynchronously
        from time import sleep
        sleep(1)
        search_results = blk.search(doc_type=doc_type,
                                    body={"query": {"match_all": {}}},
                                    params={"size": 10000})
        self.assertIsNotNone(search_results)
        self.assertIn('hits', search_results)
        self.assertIn('hits', search_results['hits'])
        self.assertGreater(len(search_results['hits']['hits']), 0)

        doc_type_hits = 0
        for hit in search_results['hits']['hits']:
            if hit["_type"] == doc_type:
                doc_type_hits += 1
        self.assertGreater(doc_type_hits, 0)

    def test_search_default_type(self):
        blk = ESInsert()
        default_doc_type = "Signal"

        index = "test_index"
        # make sure document doesn't exist
        delete_elasticsearch_index(index)

        self.configure_block(blk, {
            "index": index,
            "log_level": logging.DEBUG
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        self._assert_hit(blk, default_doc_type)

        blk.stop()
