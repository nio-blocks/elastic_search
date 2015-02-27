import logging
from ..es_base_block import ESBase

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase


class TestESBase(NIOBlockTestCase):
    """ Tests basic elasticsearch functionality provided by
    ESBase class
    """

    def setUp(self):
        super().setUp()
        try:
            from elasticsearch import Elasticsearch
        except:
            self._outcome.success = False
            return

        es = Elasticsearch()
        # try just once
        es.transport.max_retries = 1
        if not es.ping():
            print("elasticsearch connection error, Test skipped")
            self._outcome.success = False

    def test_connected(self):
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk.connected())

    def test_search(self):
        blk = ESBase()
        self.configure_block(blk, {
            "index": "test_index",
            "type": "test_type",
            "log_level": logging.DEBUG
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        search_results = blk.search({"query": {"match_all": {}}})
        self.assertIsNotNone(search_results)
        self.assertIn('hits', search_results)
        self.assertIn('hits', search_results['hits'])
        self.assertGreater(len(search_results['hits']['hits']), 0)
        blk.stop()
