from unittest.mock import patch
from nio.block.terminals import DEFAULT_TERMINAL
from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase
from ..es_find_block import ESFind

# The search method is what searches an ES instance. It should be
# called with the following fields, in this order:
#  - index=name of index
#  - doc_type=document type
#  - body=query body (Should be wrapped in query, sort, size, and/or from)


@patch('elasticsearch.Elasticsearch.search')
class TestESFind(NIOBlockTestCase):

    """ Tests elasticsearch block find functionality """

    def test_normal_query_dict(self, search_method):
        """ Tests that a normal query happens properly when given a dict """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "condition": '{{ {"expr": $val} }}'
        })
        blk.start()
        blk.process_signals([Signal({'val': '123'})])
        search_method.assert_called_once_with(
            index="index_name",
            doc_type="doc_type_name",
            body={"query": {"expr": "123"}})
        blk.stop()

    def test_normal_query_str(self, search_method):
        """ Tests that a normal query happens properly when given a string """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "condition": '{"expr": "{{ $val }}"}'
        })
        blk.start()
        blk.process_signals([Signal({'val': '123'})])
        search_method.assert_called_once_with(
            index="index_name",
            doc_type="doc_type_name",
            body={"query": {"expr": "123"}})
        blk.stop()

    def test_sort_query(self, search_method):
        """ Tests that a sorted query happens properly when given a dict """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "sort": [{
                "key": "sort_key",
                "direction": "desc"
            }],
            'condition': '{{ {"expr": $val} }}'
        })
        blk.start()
        blk.process_signals([Signal({'val': '123'})])
        search_method.assert_called_once_with(
            index="index_name",
            doc_type="doc_type_name",
            body={
                "query": {"expr": "123"},
                "sort": [{"sort_key": "desc"}]
            })
        blk.stop()

    def test_limit_query(self, search_method):
        """ Tests that a limited query happens properly when given a dict """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "size": "10",
            "offset": "5",
            'condition': '{{ {"expr": $val} }}'
        })
        blk.start()
        blk.process_signals([Signal({'val': '123'})])
        search_method.assert_called_once_with(
            index="index_name",
            doc_type="doc_type_name",
            body={
                "query": {"expr": "123"},
                "size": 10,
                "from": 5
            })
        blk.stop()

    def test_full_notify(self, search_method):
        """ Tests that a full result set is notified """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "condition": '{{ {"expr": $val} }}',
            "pretty_results": False  # to get us the full result
        })
        blk.start()
        # This is a snippet of a standard ES response
        search_method.return_value = {
            "hits": {
                "hits": [{
                    "_index": "index_name",
                    "_source": {
                        "result_key_1": "result_val_1"
                    }
                }]
            }
        }
        blk.process_signals([Signal({'val': '123'})])
        self.assert_num_signals_notified(1)
        # We should still have the index information
        # and it should have been renamed to remove the leading underscore
        self.assertEqual(
            self.last_notified[DEFAULT_TERMINAL][0].index, "index_name")
        # We should also have the signal information, buried inside the
        # "source" attribute
        self.assertEqual(
            self.last_notified[DEFAULT_TERMINAL][0].source["result_key_1"],
            "result_val_1")
        blk.stop()

    def test_pretty_notify(self, search_method):
        """ Tests that only the signal is notified if pretty results is on """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "condition": '{{ {"expr": $val} }}',
            "pretty_results": True  # to get us the pretty result
        })
        blk.start()
        # This is a snippet of a standard ES response
        search_method.return_value = {
            "hits": {
                "hits": [{
                    "_index": "index_name",
                    "_source": {
                        "result_key_1": "result_val_1"
                    }
                }]
            }
        }
        blk.process_signals([Signal({'val': '123'})])
        self.assert_num_signals_notified(1)
        # It should only be the "source" object that was notified
        self.assertDictEqual(
            {"result_key_1": "result_val_1"},
            self.last_notified[DEFAULT_TERMINAL][0].__dict__)
        blk.stop()

    def test_multiple_notify(self, search_method):
        """ Tests that multiple signals can be notified """
        blk = ESFind()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "condition": '{{ {"expr": $val} }}'
        })
        blk.start()
        # This is a snippet of a standard ES response
        search_method.return_value = {
            "hits": {
                "hits": [{
                    "_index": "index_name",
                    "_source": {
                        "result_key_1": "result_val_1"
                    }
                }, {
                    "_index": "index_name",
                    "_source": {
                        "result_key_2": "result_val_2"
                    }
                }]
            }
        }
        # Only one signal drives the query
        blk.process_signals([Signal({'val': '123'})])
        # but multiple results are returned
        self.assert_num_signals_notified(2)
        self.assertEqual(
            self.last_notified[DEFAULT_TERMINAL][0].result_key_1,
            "result_val_1"
        )
        self.assertEqual(
            self.last_notified[DEFAULT_TERMINAL][1].result_key_2,
            "result_val_2"
        )
        blk.stop()
