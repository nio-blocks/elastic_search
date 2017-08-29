import logging
from unittest.mock import patch
from nio.block.terminals import DEFAULT_TERMINAL
from nio.testing.block_test_case import NIOBlockTestCase
from nio.signal.base import Signal
from ..es_base import ESBase


# Let's simulate that our execute query returns two signals/results
@patch(ESBase.__module__ + '.ESBase.execute_query',
       return_value=[{}, {}])
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
        self.configure_block(blk, {"retry_options": {"max_retry": 1}})
        blk.start()
        # Execute query will raise an exception
        exec_method.side_effect = Exception('bad query')
        # Set __name__ so Retry mixin can log it's warning
        exec_method.__name__ = "execute_query"
        blk.process_signals([Signal()])
        # Make sure no signals notified
        self.assert_num_signals_notified(0)
        # Make sure 2 queries were attempted (1 for retry)
        self.assertEqual(blk.execute_query.call_count, 2)
        blk.stop()

    def test_retry(self, exec_method):
        """ Make sure that a succesful retry notifies a signal """
        blk = ESBase()
        self.configure_block(blk, {})
        blk.start()
        # Execute query will raise an exception the first time
        # and then return one signal during the retry.
        exec_method.side_effect = [Exception('bad query'), [{}]]
        # Set __name__ so Retry mixin can log it's warning
        exec_method.__name__ = "execute_query"
        blk.process_signals([Signal()])
        # Make sure a signal was notified from the retry
        self.assert_num_signals_notified(1)
        # Make sure 2 queries were attempted (1 for retry)
        self.assertEqual(blk.execute_query.call_count, 2)
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
    def test_elasticsearch_client_kwargs(self, es, exec_method):
        """Elasticsearch client is instantiated with configured kwargs."""
        blk = ESBase()
        self.configure_block(blk, {
            "elasticsearch_client_kwargs": {"maxsize": 10}})
        self.assertDictEqual({
            # hosts is always used
            "hosts": ["http://127.0.0.1:9200/"],
            # maxsize comes from elasticsearch_client_kwargs property
            "maxsize": 10
        }, es.call_args[1])

    @patch('elasticsearch.Elasticsearch')
    def test_elasticsearch_client_kwargs_as_string(self, es, exec_method):
        """Client Arguments can be a string."""
        blk = ESBase()
        self.configure_block(blk, {
            "elasticsearch_client_kwargs": '{"maxsize": 10}'})
        self.assertDictEqual({
            # hosts is always used
            "hosts": ["http://127.0.0.1:9200/"],
            # maxsize comes from elasticsearch_client_kwargs property
            "maxsize": 10
        }, es.call_args[1])

    @patch('elasticsearch.Elasticsearch')
    def test_elasticsearch_client_kwargs_invalid_str(self, es, exec_method):
        """Client Arguments that are a string needs to be of dict format."""
        blk = ESBase()
        self.configure_block(blk, {
            "elasticsearch_client_kwargs": 'not a dict'})
        self.assertDictEqual({
            # not called with any additional kwargs
            "hosts": ["http://127.0.0.1:9200/"]
        }, es.call_args[1])
        # TODO: assert that blk.logger.warning is called

    @patch('elasticsearch.Elasticsearch')
    def test_empty_client_kwargs(self, es, exec_method):
        """kwargs that is an empty string should not log a warning."""
        blk = ESBase()
        self.configure_block(blk, {
            "elasticsearch_client_kwargs": ''})
        self.assertDictEqual({
            # not called with any additional kwargs
            "hosts": ["http://127.0.0.1:9200/"]
        }, es.call_args[1])
        # TODO: assert that blk.logger.warning is not called

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
        self.assertDictEqual(
            {"field1": "1", "result": "value"},
            self.last_notified[DEFAULT_TERMINAL][0].__dict__)
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
        self.assertDictEqual(
            {"field1": "1", "result": {"result": "value"}},
            self.last_notified[DEFAULT_TERMINAL][0].__dict__)
        blk.stop()
