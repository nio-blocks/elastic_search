from enum import Enum

from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import ListProperty, SelectProperty, \
    PropertyHolder, StringProperty, ExpressionProperty
from nio.common.signal.base import Signal

from .es_base_block import ESBase
from . import evaluate_expression


class Limitable():

    """ A elasticsearch block mixin that allows you to limit results

    A limit of zero is useful to know the amount of items that can be
    retrieved, subsequent calls can include a specific limit and an offset
    """

    size = ExpressionProperty(title='Size', default="")

    def query_args(self, signal=None):
        existing_args = super().query_args(signal)
        size = evaluate_expression(self.size, signal, False)
        if size:
            size = int(size)
            # if size ends up being 0, discard parameter, thus use default
            if size:
                existing_args['size'] = size
        return existing_args


class Offset():

    """ A elasticsearch block mixin that allows you to offset results

    An offset of zero would allow to return elements from the beginning,
    subsequent searches could advance this offset
    """

    offset = ExpressionProperty(title='Offset', default="")

    def query_args(self, signal=None):
        existing_args = super().query_args(signal)
        offset = evaluate_expression(self.offset, signal, False)
        if offset:
            existing_args['from_'] = int(offset)
        return existing_args


class SortDirection(Enum):
    DESCENDING = -1
    ASCENDING = 1


class Sort(PropertyHolder):
    key = StringProperty(title="Key", default="key")
    direction = SelectProperty(SortDirection,
                               default=SortDirection.ASCENDING,
                               title="Direction")


class Sortable():

    """ A elasticsearch block mixin that allows you to sort results """

    sort = ListProperty(Sort, title='Sort')

    def __init__(self):
        super().__init__()
        self._sort = []

    def configure(self, context):
        super().configure(context)

        self._sort = [(s.key, s.direction.value) for s in self.sort]

    def query_args(self, signal=None):
        existing_args = super().query_args(signal)
        if self._sort:
            existing_args['sort'] = [
                {key_field:
                    {"order": "asc"
                    if order == SortDirection.ASCENDING.value else "desc"}}
                for (key_field, order) in self._sort]
        return existing_args


@Discoverable(DiscoverableType.block)
class ESFind(Limitable, Sortable, Offset, ESBase):

    """ A block for running `search` against a elasticsearch.

    Properties:
        condition (expression): A dictionary form of a search expression.
        This is an expression property that can evaluate to a dictionary
        or be a parseable JSON string

    """
    condition = ExpressionProperty(title='Condition',
                                   default="{'match_all': {}}")

    def execute_query(self, doc_type, signal):
        condition = evaluate_expression(self.condition, signal)
        self._logger.debug("Condition evaluated to: {}".format(condition))

        search_results = self.search(doc_type=doc_type,
                                     body={"query": condition},
                                     params=self.query_args(signal))
        if search_results and "hits" in search_results:
            return [Signal(self._process_fields(hit))
                    for hit in search_results['hits']['hits']]

    def _process_fields(self, elasticsearch_resulting_dict):
        """ elasticsearch return fields starting with underscore,
        and nio Signal would not consider them, therefore, remove
        underscore and let nio Signal grab them.

        Args:
            elasticsearch_resulting_dict: dict as returned by elasticsearch

        Returns:
            dictionary with processes keys
        """
        d = {key if not key.startswith('_') else key[1:]: value
             for key, value in elasticsearch_resulting_dict.items() }
        return d
