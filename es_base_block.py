import logging
from nio.common.block.base import Block
from nio.metadata.properties import StringProperty, ExpressionProperty, \
    IntProperty
from nio.common.command import command
from .mixins.enrich.enrich_signals import EnrichSignals


@command("connected")
class ESBase(EnrichSignals, Block):

    """ A base block for Elasticsearch.

    Properties:
        index (str): The name of the index (equivalent to database)
        type (expression): The type of the document (equivalent to table)

    """
    host = StringProperty(title='ES Host', default="127.0.0.1")
    port = IntProperty(title='ES Port', default=9200)
    index = StringProperty(title='Index', default="nio")
    doc_type = ExpressionProperty(title='Type',
                                  default="{{($__class__.__name__)}}")

    def __init__(self):
        super().__init__()
        self._es = None

    def configure(self, context):
        super().configure(context)
        self._es = self.create_elastic_search_instance()
        logging.getLogger('elasticsearch').setLevel(self._logger.logger.level)

    def create_elastic_search_instance(self):
        from elasticsearch import Elasticsearch
        from elasticsearch.connection import RequestsHttpConnection
        return Elasticsearch(
            hosts=[{'host': self.host, 'port': self.port}],
            transport=RequestsHttpConnection)

    def process_signals(self, signals, input_id='default'):
        output = []
        for s in signals:
            doc_type = self._evaluate_doc_type(s)
            self._logger.debug("doc_type evaluated to: {}".format(doc_type))
            if doc_type:
                try:
                    result = self.execute_query(doc_type, s)

                    # Expect execute_query to return a dictionary for a signal,
                    # we will enrich according to configuration here
                    if result and isinstance(result, list):
                        output.extend([self.get_output_signal(res, s)
                                       for res in result])
                except:
                    # If the execute call fails, we won't use this signal
                    self._logger.exception("Query failed")
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
            self._logger.exception("doc_type failed to evaluate")

    def connected(self):
        return {'connected': self._es.ping()}
