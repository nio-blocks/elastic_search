from .es_base_block import ESBase
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.bool import BoolProperty
from nio.common.signal.base import Signal


@Discoverable(DiscoverableType.block)
class ESInsert(ESBase):

    """ A block for recording signals or other such
    system-external store.

    Properties:
        with_type (str): include the signal type in the record?

    """
    with_type = BoolProperty(
        title='Include the type of logged signals?',
        default=False,
        visible=False)

    def execute_query(self, doc_type, signal):
        body = signal.to_dict(self.with_type)
        try:
            index = self.index(signal)
            if not index:
                raise Exception("{} is an invalid index".format(index))
        except:
            self._logger.exception(
                "Unable to determine index for {}".format(signal))
            return []

        self._logger.debug("Inserting {} to: {}, type: {}".
                           format(body, index, doc_type))

        result = self._es.index(index, doc_type, body)
        if result and "_id" in result:
            return [Signal({'id': result["_id"]})]
