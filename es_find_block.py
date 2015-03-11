from enum import Enum

from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import ListProperty, SelectProperty, \
    PropertyHolder, StringProperty, ExpressionProperty, BoolProperty
from nio.common.signal.base import Signal

from .es_base_block import ESBase
from . import evaluate_expression


class Limitable():

    """ A elasticsearch block mixin that allows you to limit results

    A limit of zero is useful to know the amount of items that can be
    retrieved, subsequent calls can include a specific limit and an offset
    """

    size = ExpressionProperty(title='Size', default="")
    offset = ExpressionProperty(title='Offset', default="")

    def query_args(self, signal=None):
        existing_args = super().query_args(signal)
        size = evaluate_expression(self.size, signal, False)
        if size:
            size = int(size)
            # if size ends up being 0, discard parameter, thus use default
            if size:
                existing_args['size'] = size
        offset = evaluate_expression(self.offset, signal, False)
        if offset:
            existing_args['from'] = int(offset)
        return existing_args


class SortDirection(Enum):
    DESCENDING = "desc"
    ASCENDING = "asc"


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

        self._sort = [{s.key: s.direction.value} for s in self.sort]

    def query_args(self, signal=None):
        existing_args = super().query_args(signal)
        if self._sort:
            existing_args['sort'] = self._sort
        return existing_args


@Discoverable(DiscoverableType.block)
class ESFind(Limitable, Sortable, ESBase):

    """ A block for running `search` against a elasticsearch.

    Properties:
        condition (expression): A dictionary form of a search expression.
        This is an expression property that can evaluate to a dictionary
        or be a parseable JSON string

    """
    condition = ExpressionProperty(
        title='Condition', default="{'match_all': {}}")
    pretty_results = BoolProperty(title='Pretty Results', default=True)

    def execute_query(self, doc_type, signal):
        condition = evaluate_expression(self.condition, signal)
        self._logger.debug("Condition evaluated to: {}".format(condition))

        query_body = {"query": condition}
        query_body.update(self.query_args(signal))

        search_params = {
            'index': self.index,
            'doc_type': doc_type,
            'body': query_body
        }
        self._logger.debug("Searching with params: {}".format(search_params))

        search_results = self._es.search(**search_params)

        if search_results and "hits" in search_results:
            return [Signal(self._process_fields(hit))
                    for hit in search_results['hits']['hits']]

    def _process_fields(self, result_dict):
        """ elasticsearch return fields starting with underscore,
        and nio Signal would not consider them, therefore, remove
        underscore and let nio Signal grab them.

        Args:
            result_dict: dict as returned by elasticsearch

        Returns:
            dictionary with processes keys
        """
        if self.pretty_results and '_source' in result_dict:
            # If they want pretty results, just give them the source
            # which will likely represent a signal
            result_dict = result_dict['_source']
        else:
            # No pretty results means give them everything, however, let's
            # get rid of the leading underscores first
            result_dict = {
                key if not key.startswith('_') else key[1:]: value
                for key, value in result_dict.items()}

        return result_dict
