from .es_base import ESBase
from nio.util.discovery import discoverable
from nio.properties import BoolProperty, VersionProperty


@discoverable
class ESInsert(ESBase):

    """ A block for recording signals or other such
    system-external store.

    Properties:
        with_type (str): include the signal type in the record?

    """
    version = VersionProperty('0.1.0')
    with_type = BoolProperty(
        title='Include the type of logged signals?',
        default=False,
        visible=False)

    def execute_query(self, doc_type, signal):
        if self.with_type():
            with_type = "_type"
        else:
            with_type = None
        body = signal.to_dict(with_type=with_type)
        try:
            index = self.index(signal)
            if not index:
                raise Exception("{} is an invalid index".format(index))
        except:
            self.logger.exception(
                "Unable to determine index for {}".format(signal))
            return []

        self.logger.debug("Inserting {} to: {}, type: {}".
                           format(body, index, doc_type))

        result = self._es.index(index, doc_type, body)
        if result and "_id" in result:
            return [{'id': result["_id"]}]
