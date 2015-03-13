import logging
from unittest.mock import patch, MagicMock

from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal

from ..es_base_block import ESBase


class TestESBase(NIOBlockTestCase):

    """ Tests basic elasticsearch functionality provided by
    ESBase class
    """

    @patch('elasticsearch.Elasticsearch.ping')
    def test_connected(self, ping_method):
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": "DEBUG"
        })
        blk.connected()
        ping_method.assertCalledOnceWith()

    def test_query_execute_and_return(self):
        """ Test that if queries return signals they get notified """
        blk = ESBase()
        self.configure_block(blk, {})
        blk.start()

        # Execute query will return a list of 2 signals
        blk.execute_query = MagicMock(return_value=[Signal(), Signal()])

        # Send the block 3 signals
        blk.process_signals([Signal() for i in range(3)])

        # Execute query should have been called once for each of our
        # 3 query driving signals
        self.assertEqual(blk.execute_query.call_count, 3)

        # 2 signals should get notified for each of our 3 query driving signals
        self.assert_num_signals_notified(6)
        blk.stop()

    def test_sets_elastic_log_level(self):
        """ Tests that the elasticssearch logger inherits the log level """
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": "DEBUG"
        })
        # Make sure the elastic search logger's log level is the same value
        # as logging.DEBUG
        self.assertEqual(
            logging.getLogger('elasticsearch').level, logging.DEBUG)

    def test_bad_query(self):
        """ Make sure that no signals get processed on a bad doc_type """
        blk = ESBase()
        self.configure_block(blk, {})
        blk.start()

        # Execute query will raise an exception
        blk.execute_query = MagicMock(side_effect=Exception('bad query'))

        blk.process_signals([Signal()])

        # Make sure no signals notified and one query execution still occurred
        self.assert_num_signals_notified(0)
        self.assertEqual(blk.execute_query.call_count, 1)

        blk.stop()

    def test_bad_doctype(self):
        """ Make sure that no signals get processed on a bad doc_type """
        blk = ESBase()
        blk.execute_query = MagicMock()
        self.configure_block(blk, {
            "doc_type": "{{1 + 'str'}}"
        })
        blk.start()
        blk.process_signals([Signal()])

        # Make sure no signals notified and no query executions occurred
        self.assert_num_signals_notified(0)
        self.assertEqual(blk.execute_query.call_count, 0)

        blk.stop()
