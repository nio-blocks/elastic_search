import logging
from nio.common.block.base import Block
from nio.metadata.properties import StringProperty, ExpressionProperty
from nio.common.command import command
from nio.common.command.params.dict import DictParameter
from nio.common.command.params.string import StringParameter


@command("search",
         StringParameter("doc_type", default=""),
         DictParameter("body", default={}),
         DictParameter("params", default={}))
@command("connected")
class ESBase(Block):

    """ A base block for Elasticsearch.

    Properties:
        index (str): The name of the index (equivalent to database)
        type (expression): The type of the document (equivalent to table)

    """
    index = StringProperty(title='Index', default="nio")
    doc_type = ExpressionProperty(title='Type',
                                  default="{{($__class__.__name__)}}")

    def __init__(self):
        super().__init__()
        self._es = None

    def configure(self, context):
        super().configure(context)
        self._es = self.create_elastic_search_instance(context)
        logging.getLogger('elasticsearch').setLevel(self.log_level.value)

    def create_elastic_search_instance(self, context):
        from elasticsearch import Elasticsearch
        return Elasticsearch()

    def process_signals(self, signals, input_id='default'):
        output = []
        for s in signals:
            doc_type = self._evaluate_doc_type(s)
            self._logger.debug("doc_type evaluated to: {}".format(doc_type))
            if doc_type:
                try:
                    result = self.execute_query(doc_type, s)
                    if result:
                        output.extend(result)
                except Exception as e:
                    # If the execute call fails, we won't use this signal
                    self._logger.error("Query failed: {}: {}".format(
                        type(e).__name__, str(e)))
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
            raise

    def connected(self):
        return {'connected': self._es.ping()}

    def search(self, doc_type, body=None, params=None):
        doc_type = doc_type if doc_type else self.doc_type
        body = body if body else {}
        params = params if params else {}
        return self._es.search(index=self.index, doc_type=doc_type,
                               body=body, params=params)
