from nio.common.block.base import Block
from nio.common.command.params.dict import DictParameter
from nio.common.command.params.string import StringParameter
from nio.metadata.properties.expression import ExpressionProperty
from nio.metadata.properties.string import StringProperty
from nio.common.command import command
from nio.common.discovery import Discoverable, DiscoverableType


@command("search",
         StringParameter("doc_type", default=""),
         DictParameter("body", default={}),
         DictParameter("params", default={}))
@command("connected")
@Discoverable(DiscoverableType.block)
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

    def create_elastic_search_instance(self, context):
        from elasticsearch import Elasticsearch
        return Elasticsearch()

    def process_signals(self, signals, input_id='default'):
        for s in signals:
            try:
                self._insert_signal(s)
            except Exception as e:
                # If the call fails, we won't use this signal
                self._logger.error("Failed to insert signal: {}, details: {}".
                                   format(s.to_dict(), str(e)))

    def _evaluate_doc_type(self, signal):
        try:
            return self.doc_type(signal)
        except Exception as e:
            self._logger.error("doc_type failed to evaluate, details: {0}".
                               format(str(e)))
            raise e

    def _insert_signal(self, signal):
        body = signal.to_dict()
        doc_type = self._evaluate_doc_type(signal)

        self._logger.debug("Inserting {} to: {}, type: {}".
                           format(body, self.index, doc_type))

        self._es.index(self.index, doc_type, body)

    def connected(self):
        return self._es.ping()

    def search(self, doc_type, body=None, params=None):
        doc_type = doc_type if doc_type else self.doc_type
        body = body if body else {}
        params = params if params else {}
        return self._es.search(index=self.index, doc_type=doc_type,
                               body=body, params=params)
