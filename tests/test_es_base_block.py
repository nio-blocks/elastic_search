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
        test_type = "test_type"
        self.configure_block(blk, {
            "index": "test_index",
            "doc_type": test_type,
            "log_level": logging.DEBUG
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        search_results = blk.search(doc_type=test_type,
                                    body={"query": {"match_all": {}}},
                                    params={"size": 10000})
        self.assertIsNotNone(search_results)
        self.assertIn('hits', search_results)
        self.assertIn('hits', search_results['hits'])
        self.assertGreater(len(search_results['hits']['hits']), 0)

        test_type_hits = 0
        for hit in search_results['hits']['hits']:
            if hit["_type"] == test_type:
                test_type_hits += 1
        self.assertGreater(test_type_hits, 0)

        blk.stop()

    def test_search_default_type(self):
        blk = ESBase()
        self.configure_block(blk, {
            "index": "test_index",
            "log_level": logging.DEBUG
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        search_results = blk.search(doc_type="Signal",
                                    body={"query": {"match_all": {}}},
                                    params={"size": 10000})
        self.assertIsNotNone(search_results)
        self.assertIn('hits', search_results)
        self.assertIn('hits', search_results['hits'])
        self.assertGreater(len(search_results['hits']['hits']), 0)

        signal_type = Signal().__class__.__name__
        signal_type_hits = 0
        for hit in search_results['hits']['hits']:
            if hit["_type"] == signal_type:
                signal_type_hits += 1
        self.assertGreater(signal_type_hits, 0)

        blk.stop()
