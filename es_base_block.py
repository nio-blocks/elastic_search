from nio.common.block.base import Block
from nio.common.command.params.dict import DictParameter
from nio.metadata.properties.expression import ExpressionProperty
from nio.metadata.properties.string import StringProperty
from nio.common.command import command
from nio.common.discovery import Discoverable, DiscoverableType


@command("search", DictParameter("body", default={}))
@command("connected")
@Discoverable(DiscoverableType.block)
class ESBase(Block):

    """ A base block for Elasticsearch.

    Properties:
        index (str): The name of the index (equivalent to database)
        type (expression): The type of the document (equivalent to table)

    """
    index = StringProperty(title='Index', default="nio")
    type = ExpressionProperty(title='Type', default="signals")

    def __init__(self):
        super().__init__()
        self._es = None

    def configure(self, context):
        super().configure(context)
        from elasticsearch import Elasticsearch
        self._es = Elasticsearch()

    def process_signals(self, signals, input_id='default'):
        for s in signals:
            try:
                self._insert_signal(s)
            except Exception as e:
                # If the call fails, we won't use this signal
                self._logger.error("Failed to insert signal: {}, details: {}".
                                   format(s.to_dict(), str(e)))

    def _insert_signal(self, signal):

        body = signal.to_dict()
        _type = self.type(signal)

        self._logger.debug("Inserting {} to: {}, type: {}".
                           format(body, self.index, _type))

        self._es.index(index=self.index, doc_type=_type,
                       body=body)

    def connected(self):
        return self._es.ping()

    def search(self, body=None):
        body = body if body else {}
        return self._es.search(body=body, index=self.index)
