from .es_base_block import ESBase
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import BoolProperty, VersionProperty


@Discoverable(DiscoverableType.block)
class ESInsert(ESBase):

    """ A block for recording signals or other such
    system-external store.

    Properties:
        with_type (str): include the signal type in the record?

    """
    version = VersionProperty('1.0.0')
    with_type = BoolProperty(
        title='Include the type of logged signals?',
        default=False,
        visible=False)

    def execute_query(self, doc_type, signal):
        body = signal.to_dict(self.with_type)
        self._logger.debug("Inserting {} to: {}, type: {}".
                           format(body, self.index, doc_type))

        result = self._es.index(self.index, doc_type, body)
        if result and "_id" in result:
            return [{'id': result["_id"]}]
