import logging
from unittest.mock import patch

from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal

from ..es_base_block import ESBase


# Let's simulate that our execute query returns two signals/results
@patch(ESBase.__module__ + '.ESBase.execute_query',
       return_value=[{}, {}])
class TestESBase(NIOBlockTestCase):

    """ Tests basic elasticsearch functionality provided by
    ESBase class
    """

    def setUp(self):
        super().setUp()
        self._signals_notified = []

    def signals_notified(self, signals, output_id='default'):
        self._signals_notified.extend(signals)

    @patch('elasticsearch.Elasticsearch.ping')
    def test_connected(self, ping_method, exec_method):
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": "DEBUG"
        })
        blk.connected()
        ping_method.assertCalledOnceWith()

    def test_query_execute_and_return(self, exec_method):
        """ Test that if queries return signals they get notified """
        blk = ESBase()
        self.configure_block(blk, {})
        blk.start()

        # Send the block 3 signals
        blk.process_signals([Signal() for i in range(3)])

        # Execute query should have been called once for each of our
        # 3 query driving signals
        self.assertEqual(blk.execute_query.call_count, 3)

        # 2 signals should get notified for each of our 3 query driving signals
        self.assert_num_signals_notified(6)
        blk.stop()

    def test_sets_elastic_log_level(self, exec_method):
        """ Tests that the elasticssearch logger inherits the log level """
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": "DEBUG"
        })
        # Make sure the elastic search logger's log level is the same value
        # as logging.DEBUG
        self.assertEqual(
            logging.getLogger('elasticsearch').level, logging.DEBUG)

    def test_bad_query(self, exec_method):
        """ Make sure that no signals get processed on a bad doc_type """
        blk = ESBase()
        self.configure_block(blk, {})
        blk.start()

        # Execute query will raise an exception
        exec_method.side_effect = Exception('bad query')

        blk.process_signals([Signal()])

        # Make sure no signals notified and one query execution still occurred
        self.assert_num_signals_notified(0)
        self.assertEqual(blk.execute_query.call_count, 1)

        blk.stop()

    def test_bad_doctype(self, exec_method):
        """ Make sure that no signals get processed on a bad doc_type """
        blk = ESBase()
        self.configure_block(blk, {
            "doc_type": "{{1 + 'str'}}"
        })
        blk.start()
        blk.process_signals([Signal()])

        # Make sure no signals notified and no query executions occurred
        self.assert_num_signals_notified(0)
        self.assertEqual(blk.execute_query.call_count, 0)

        blk.stop()

    def test_enrich_signals_merge(self, exec_method):
        """ Tests enrich signals """
        exec_method.return_value = [{'result': 'value'}]
        blk = ESBase()
        self.configure_block(blk, {
            "index": "index_name",
            "doc_type": "doc_type_name",
            "enrich": {"exclude_existing": False}
        })
        blk.start()
        blk.process_signals([Signal({"field1": "1"})])
        # Assert one signal was notified and it has the mocked value in it
        self.assert_num_signals_notified(1)
        self.assertDictEqual(self._signals_notified[0].to_dict(),
                             {"field1": "1",
                              "result": "value"})
        blk.stop()

    def test_enrich_signals_field(self, exec_method):
        """ Tests enrich signals """
        exec_method.return_value = [{'result': 'value'}]
        blk = ESBase()
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
                              "result": {"result": "value"}})
        blk.stop()
