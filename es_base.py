from time import sleep
import json
import logging

from nio.block.base import Block
from nio.properties import StringProperty, Property, \
    IntProperty, BoolProperty, ObjectProperty, PropertyHolder
from nio.command import command
from nio.block.mixins.retry.retry import Retry
from nio.block.mixins.retry.strategy import BackoffStrategy
from nio.block.mixins.enrich.enrich_signals import EnrichSignals, \
    EnrichProperties
from nio.util.discovery import not_discoverable


class SleepBackoffStrategy(BackoffStrategy):

    def next_retry(self):
        if self.retry_num > 1:
            self.logger.error("Aborty retries")
            return False
        self.logger.debug(
            "Waiting {} seconds before retrying execute method".format(
                self.retry_num))
        sleep(self.retry_num)
        return True


class AuthData(PropertyHolder):
    username = StringProperty(title="Username", default="")
    password = StringProperty(title="Password", default="")
    use_https = BoolProperty(title="Use HTTPS?", default=False)


@not_discoverable
@command("connected")
class ESBase(Retry, EnrichSignals, Block):

    """ A base block for Elasticsearch.

    Properties:
        index (str): The name of the index (equivalent to database)
        type (expression): The type of the document (equivalent to table)

    """
    host = StringProperty(title='ES Host', default="127.0.0.1")
    port = IntProperty(title='ES Port', default=9200)
    index = Property(title='Index', default="nio")
    doc_type = Property(title='Type', default="{{($__class__.__name__)}}")
    auth = ObjectProperty(AuthData, title="Authentication", default=AuthData())
    elasticsearch_client_kwargs = Property(title='Client Argurments',
                                           default=None, allow_none=True)
    # TODO: remove this when nio framework is fixed
    enrich = ObjectProperty(EnrichProperties, title='Signal Enrichment',
                            default=EnrichProperties())

    def __init__(self):
        super().__init__()
        self._es = None
        self._backoff_strategy = SleepBackoffStrategy(logger=self.logger)

    def configure(self, context):
        super().configure(context)
        self._es = self.create_elastic_search_instance()
        logging.getLogger('elasticsearch').setLevel(self.logger.logger.level)

    def create_elastic_search_instance(self):
        url = self.build_host_url()
        self.logger.debug(
            "Creating ElasticSearch instance for {}".format(url))
        from elasticsearch import Elasticsearch
        from elasticsearch.connection import RequestsHttpConnection
        return Elasticsearch(**self._build_elasticsearch_client_kwargs(url))

    def build_host_url(self):
        if self.auth().username():
            return "{}://{}:{}@{}:{}/".format(
                'https' if self.auth().use_https() else 'http',
                self.auth().username(), self.auth().password(),
                self.host(), self.port())
        else:
            return "{}://{}:{}/".format(
                'https' if self.auth().use_https() else 'http',
                self.host(), self.port())

    def _build_elasticsearch_client_kwargs(self, url):
        kwargs = {'hosts': [url]}
        client_kwargs = self.elasticsearch_client_kwargs() or {}
        if client_kwargs is not None:
            try:
                if isinstance(client_kwargs, str):
                    client_kwargs = json.loads(client_kwargs)
                kwargs.update(client_kwargs)
            except:
                self.logger.warning(
                    "Client Arguments needs to be a dictionary: {}".format(
                        self.elasticsearch_client_kwargs()), exc_info=True)
        return kwargs

    def process_signals(self, signals, input_id='default'):
        output = []
        for s in signals:
            doc_type = self._evaluate_doc_type(s)
            self.logger.debug("doc_type evaluated to: {}".format(doc_type))
            if doc_type:
                try:
                    result = self.execute_with_retry(
                        self.execute_query, doc_type=doc_type, signal=s)
                    # Expect execute_query to return a dictionary for a signal,
                    # we will enrich according to configuration here
                    if result and isinstance(result, list):
                        output.extend([self.get_output_signal(res, s)
                                       for res in result])
                except:
                    # If the execute call fails, we won't use this signal
                    self.logger.exception("Query failed")
                    continue

        # Check if we have anything to output
        if output:
            self.notify_signals(output)

    def query_args(self, signal=None):
        """ Query arguments to use in the ES query.

        Returns:
            args (dict): A dictionary of kwargs to pass to ES queries
        """
        return {}

    def execute_query(self, doc_type, signal):
        """ Run this block's query on the provided collection.

        This should be overriden in the child blocks. It will be passed
        a document type and a signal which caused the query to run.

        If the block wishes, it may return a list of signals that will be
        notified.

        Params:
            doc_type: The type of the document
            signal (Signal): The signal which triggered the query

        Returns:
            signals (list): Any signals to notify
        """
        raise NotImplementedError()

    def _evaluate_doc_type(self, signal):
        try:
            return self.doc_type(signal)
        except:
            self.logger.exception("doc_type failed to evaluate")

    def connected(self):
        return {'connected': self._es.ping()}
