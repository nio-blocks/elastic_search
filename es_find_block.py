from .es_base_block import ESBase, Limitable, Sortable
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.expression import ExpressionProperty
from nio.common.signal.base import Signal


@Discoverable(DiscoverableType.block)
class ESFind(Limitable, Sortable, ESBase):

    """ A block for running `search` against a elasticsearch.

    Properties:
        condition (expression): A dictionary form of a search expression.
        This is an expression property that can evaluate to a dictionary
        or be a parseable JSON string

    """
    condition = ExpressionProperty(title='Condition',
                                   default="{'match_all': {}}")

    def execute_query(self, doc_type, signal):
        condition = self.evaluate_expression(self.condition, signal)
        self._logger.debug("Condition evaluated to: {}".format(condition))

        search_results = self.search(doc_type=doc_type,
                                     body={"query": condition},
                                     params=self.query_args())
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
