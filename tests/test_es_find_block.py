import logging

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.util.unique import Unique
from ..es_insert_block import ESInsert
from ..es_find_block import ESFind
from ..tests import elasticsearch_running, delete_elasticsearch_document


class TestESFind(NIOBlockTestCase):
    """ Tests elasticsearch block find functionality provided by
    ESFind class
    """

    def setUp(self):
        super().setUp()
        self._outcome.success = elasticsearch_running()

    def test_find(self):
        # execute an insertion
        blk = ESInsert()
        index = "ESFindIndex".lower()
        doc_type = "test_type"

        # make sure document doesn't exist
        delete_elasticsearch_document(doc_type)

        self.configure_block(blk, {
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        blk.start()
        self.id_to_find = Unique.id()
        signal = Signal({"id_field": self.id_to_find})
        blk.process_signals([signal])
        self._assert_hit(blk, doc_type)
        blk.stop()

        # now create a find block and find just inserted item
        blk = ESFind()
        condition = '{"term": {"id_field": "{{$id_field}}"}}'
        self.configure_block(blk, {
            "index": index,
            "doc_type": doc_type,
            "condition": condition,
            "log_level": logging.DEBUG
        })
        blk.start()
        blk.process_signals([signal])
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

    def signals_notified(self, signals, output_id='default'):
        self.assertEqual(len(signals), 1)
        self.assertEqual(len(signals[0].source), 1)
        self.assertEqual(signals[0].source['id_field'], self.id_to_find)
