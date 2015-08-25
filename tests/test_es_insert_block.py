from unittest.mock import patch

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase

from ..es_insert_block import ESInsert

# The index method is what stores the signals. It should be called
# with the following fields, in this order:
#  - name of index
#  - document type
#  - signal body (dict)


@patch('elasticsearch.Elasticsearch.index')
class TestESInsert(NIOBlockTestCase):

    """ Tests elasticsearch block insert functionality """

    def setUp(self):
        super().setUp()
        self._signals_notified = []

    def signals_notified(self, signals, output_id='default'):
        self._signals_notified.extend(signals)

    def test_insert_called(self, index_method):
        """ Tests that inserts get called for each signal """
        blk = ESInsert()

        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name"
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        index_method.assert_called_once_with(
            "index_name", "doc_type_name", {"field1": "1"})
        blk.stop()

    def test_insert_called_default_vals(self, index_method):
        """ Tests the behavior of the block's default values """
        blk = ESInsert()
        self.configure_block(blk, {})
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        index_method.assert_called_once_with(
            "nio", "Signal", {"field1": "1"})
        blk.stop()

    def test_insert_with_type(self, index_method):
        """ Tests that signals can get inserted with their signal types """
        blk = ESInsert()
        self.configure_block(blk, {
            "with_type": True,
            "index": "index_name",
            "doc_type": "doc_type_name"
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        index_method.assert_called_once_with(
            "index_name", "doc_type_name", {
                "field1": "1",
                "_type": "nio.common.signal.base.Signal"
            })
        blk.stop()

    def test_index_return(self, index_method):
        """ Tests that insert calls can return the id of the insertion """
        blk = ESInsert()
        index_method.return_value = {"_id": "inserted_id"}
        self.configure_block(blk, {
            "with_type": True,
            "index": "index_name",
            "doc_type": "doc_type_name"
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        # Assert one signal was notified and it has the inserted id in it
        self.assert_num_signals_notified(1)
        self.assertDictEqual(self._signals_notified[0].to_dict(),
                             {"id": "inserted_id"})
        blk.stop()

    def test_enrich_signals_merge(self, index_method):
        """ Tests enrich signals """
        blk = ESInsert()
        index_method.return_value = {"_id": "inserted_id"}
        self.configure_block(blk, {
            "with_type": True,
            "index": "index_name",
            "doc_type": "doc_type_name",
            "enrich": {"exclude_existing": False}
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        # Assert one signal was notified and it has the inserted id in it
        self.assert_num_signals_notified(1)
        self.assertDictEqual(self._signals_notified[0].to_dict(),
                             {"field1": "1",
                              "id": "inserted_id"})
        blk.stop()

    def test_enrich_signals_field(self, index_method):
        """ Tests enrich signals """
        blk = ESInsert()
        index_method.return_value = {"_id": "inserted_id"}
        self.configure_block(blk, {
            "with_type": True,
            "index": "index_name",
            "doc_type": "doc_type_name",
            "enrich": {"exclude_existing": False,
                       "enrich_field": "result"}
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        # Assert one signal was notified and it has the inserted id in it
        self.assert_num_signals_notified(1)
        self.assertDictEqual(self._signals_notified[0].to_dict(),
                             {"field1": "1",
                              "result": {"id": "inserted_id"}})
        blk.stop()
