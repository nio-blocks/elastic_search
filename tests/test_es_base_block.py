import logging
from unittest.mock import patch
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal
from ..es_base_block import ESBase


# Let's simulate that our execute query returns two signals/results
@patch(ESBase.__module__ + '.ESBase.execute_query',
       return_value=[Signal(), Signal()])
class TestESBase(NIOBlockTestCase):

    """ Tests basic elasticsearch functionality provided by
    ESBase class
    """

    @patch('elasticsearch.Elasticsearch.ping')
    def test_connected(self, ping_method, exec_method):
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": "DEBUG"
        })
        blk.connected()
        ping_method.assert_called_once_with()

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

    @patch('elasticsearch.Elasticsearch')
    def test_elasticsearch_url(self, es, exec_method):
        blk = ESBase()
        # With defaults
        self.configure_block(blk, {})
        self.assertEqual(['http://127.0.0.1:9200/'],
                         es.call_args[1]['hosts'])
        # With auth
        self.configure_block(blk, {
            "auth": {
                "username": "user",
                "password": "pwd"
            }
        })
        self.assertEqual(['http://user:pwd@127.0.0.1:9200/'],
                         es.call_args[1]['hosts'])
        # With https
        self.configure_block(blk, {
            "auth": {
                "username": "user",
                "password": "pwd",
                "use_https": True
            }
        })
        self.assertEqual(['https://user:pwd@127.0.0.1:9200/'],
                         es.call_args[1]['hosts'])
