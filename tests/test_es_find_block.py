import logging

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.util.unique import Unique
from nio.util.support.fast_tests import sleeping_test
from ..es_insert_block import ESInsert
from ..es_find_block import ESFind
from ..tests import elasticsearch_running, delete_elasticsearch_index


class TestESFind(NIOBlockTestCase):
    """ Tests elasticsearch block find functionality provided by
    ESFind class
    """

    def setUp(self):
        self._outcome.success = elasticsearch_running()
        if self._outcome.success:
            super().setUp()

    @sleeping_test
    def test_find(self):
        # execute an insertion
        insert_blk = ESInsert()
        index = "ESFindIndex".lower()
        doc_type = "test_type"

        # make sure document doesn't exist
        delete_elasticsearch_index(index)

        self.configure_block(insert_blk, {
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        insert_blk.start()
        id_to_find = Unique.id()
        signal = Signal({"id_field": id_to_find})
        self._insert_signals(insert_blk, [signal])
        self._assert_hit(insert_blk, doc_type)

        self._es_find_signals_notified = []
        # now create a find block and find just inserted item
        find_blk = ESFind()
        condition = '{"term": {"id_field": "{{$id_field}}"}}'
        self.configure_block(find_blk, {
            "index": index,
            "doc_type": doc_type,
            "condition": condition,
            "log_level": logging.DEBUG
        })
        find_blk.start()
        self._find_signals(find_blk, [signal])
        self._assert_id_fields([id_to_find])

        # insert another element
        id2_to_find = Unique.id()
        signal = Signal({"id_field": id2_to_find})
        self._insert_signals(insert_blk, [signal])

        # find again, it should pickup one since we are sending one signal
        self._find_signals(find_blk, [signal])
        self._assert_id_fields([id2_to_find])

        insert_blk.stop()
        find_blk.stop()

    @sleeping_test
    def test_size_and_offset(self):
        # execute an insertion
        insert_blk = ESInsert()
        index = "ESFindIndex".lower()
        doc_type = "test_type"

        # make sure document doesn't exist
        delete_elasticsearch_index(index)

        self.configure_block(insert_blk, {
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        insert_blk.start()
        id1 = Unique.id()
        id2 = Unique.id()
        id3 = Unique.id()
        signal1 = Signal({"id_field": id1, "offset": 1})
        signal2 = Signal({"id_field": id2, "offset": 1})
        signal3 = Signal({"id_field": id3, "offset": 1})
        all_signals = [signal1, signal2, signal3]
        self._insert_signals(insert_blk, all_signals)
        self._assert_hit(insert_blk, doc_type)

        self._es_find_signals_notified = []
        # now create a find block and do a find
        find_blk = ESFind()
        self.configure_block(find_blk, {
            "size": 1,
            "offset": "{{$offset}}",
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        find_blk.start()
        self._find_signals(find_blk, [signal1])
        self.assertEqual(len(self._es_find_signals_notified), 1)

        search_results = find_blk.search(doc_type=doc_type,
                                         body={"query": {"match_all": {}}},
                                         params={"size": 10000})
        self.assertEqual(len(search_results['hits']['hits']), 3)

        # assert that it got the second one, since offset is 1
        self.assertEqual(
            self._es_find_signals_notified[0].source['id_field'],
            search_results['hits']['hits'][1]["_source"]["id_field"])

    @sleeping_test
    def test_size_from_signal(self):
        # execute an insertion
        insert_blk = ESInsert()
        index = "ESFindIndex".lower()
        doc_type = "test_type"

        # make sure document doesn't exist
        delete_elasticsearch_index(index)

        self.configure_block(insert_blk, {
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        insert_blk.start()
        id1 = Unique.id()
        id2 = Unique.id()
        signal1 = Signal({"id_field": id1, "size": 0})
        signal2 = Signal({"id_field": id2, "size": 1})
        all_signals = [signal1, signal2]
        self._insert_signals(insert_blk, all_signals)
        self._assert_hit(insert_blk, doc_type)

        self._es_find_signals_notified = []
        # now create a find block and do a find
        find_blk = ESFind()
        self.configure_block(find_blk, {
            "size": "{{$size}}",
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        find_blk.start()
        # this signal's size evaluates to 0, meaning size parameter won't be
        # added and find result will contain all signals inserted
        self._find_signals(find_blk, [signal1])
        self.assertEqual(len(self._es_find_signals_notified), 2)

        # this signal's size evaluates to 1, meaning size parameter will be
        # added and find result will contain one signal
        self._find_signals(find_blk, [signal2])
        self.assertEqual(len(self._es_find_signals_notified), 1)

    @sleeping_test
    def test_sort(self):
        # execute an insertion
        insert_blk = ESInsert()
        index = "ESFindIndex".lower()
        doc_type = "test_type"

        # make sure document doesn't exist
        delete_elasticsearch_index(index)

        self.configure_block(insert_blk, {
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        insert_blk.start()
        signal1 = Signal({"key_field": 5})
        signal2 = Signal({"key_field": 1})
        signal3 = Signal({"key_field": 3})
        all_signals = [signal1, signal2, signal3]
        self._insert_signals(insert_blk, all_signals)
        self._assert_hit(insert_blk, doc_type)

        self._es_find_signals_notified = []
        # now create a find block and do a find
        find_blk = ESFind()
        self.configure_block(find_blk, {
            "sort": [{"key": "key_field"}],
            "index": index,
            "doc_type": doc_type,
            "log_level": logging.DEBUG
        })
        find_blk.start()
        self._find_signals(find_blk, [signal1])
        self.assertEqual(len(self._es_find_signals_notified), 3)

        # TODO, test order, it is not taking effect (python elasticsearch?)


    def _insert_signals(self, insert_blk, signals):
        insert_blk.process_signals(signals)
        from time import sleep
        sleep(1)

    def _find_signals(self, find_blk, signals):
        # reset previous-find information
        self._es_find_signals_notified = []
        # perform new search
        find_blk.process_signals(signals)

    def _assert_id_fields(self, id_fields):

        self.assertEqual(len(self._es_find_signals_notified), len(id_fields))
        self.assertEqual(len(self._es_find_signals_notified[0].source),
                         len(id_fields))
        for id_field in id_fields:
            self.assertEqual(
                self._es_find_signals_notified[0].source['id_field'],
                id_field)

    def _assert_hit(self, blk, doc_type):
        # wait before asserting, it looks like api wrapper
        # executes the request asynchronously
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
        print('signals_notified called with: {}'.format(len(signals)))
        if hasattr(self, "_es_find_signals_notified"):
            self._es_find_signals_notified.extend(signals)
