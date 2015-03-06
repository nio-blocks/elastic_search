import ast
from enum import Enum

from nio.common.block.base import Block
from nio.common.command.params.dict import DictParameter
from nio.common.command.params.string import StringParameter
from nio.metadata.properties import ListProperty, SelectProperty, \
    PropertyHolder, IntProperty
from nio.metadata.properties.expression import ExpressionProperty
from nio.metadata.properties.string import StringProperty
from nio.common.command import command
from nio.common.discovery import Discoverable, DiscoverableType


class Limitable():

    """ A elasticsearch block mixin that allows you to limit results """

    limit = IntProperty(title='Limit', default=0)

    def query_args(self):
        existing_args = super().query_args()
        existing_args['limit'] = self.limit
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

    def query_args(self):
        existing_args = super().query_args()
        if self._sort:
            existing_args['sort'] = self._sort
        return existing_args


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

    def query_args(self):
        """ Query arguments to use in the pymongo query.

        Returns:
            args (dict): A dictionary of kwargs to pass to pymongo queries
        """
        return {}

    def execute_query(self, doc_type, signal):
        """ Run this block's query on the provided collection.

        This should be overriden in the child blocks. It will be passed
        a valid pymongo collection against which it can query.

        If the block wishes, it may return a list of signals that will be
        notified.

        Params:
            collection (pymongo.Collection): A valid collection
            signal (Signal): The signal which triggered the query

        Returns:
            signals (list): Any signals to notify
        """
        raise NotImplementedError()

    def _evaluate_doc_type(self, signal):
        try:
            return self.doc_type(signal)
        except Exception as e:
            self._logger.error("doc_type failed to evaluate, details: {0}".
                               format(str(e)))
            raise e

    # TODO: this functionality is copied from mongo_base_block,
    # consider unifying
    def evaluate_expression(self, expression, signal, force_dict=True):
        """ Evaluates an expression against a signal.

        This method will allow the expression to evaluate to a dictionary or
        a string representing a dictionary. In either case, a dictionary will
        be returned. If both of those fail, the value of force_dict determines
        whether or not the expression can be returned.

        Params:
            expression (expression): The ExpressionProperty reference
            signal (Signal): The signal to use to evaluate the expression
            force_dict (bool): Whether or not the expression has to evaluate
                to a dictionary

        Returns:
            result: The result of the expression evaluated with the signal

        Raises:
            TypeError: If force_dict is True and the expression is not a dict
        """
        exp_result = expression(signal)
        if not isinstance(exp_result, dict):
            try:
                # Let's at least try to make it a dict first
                exp_result = ast.literal_eval(exp_result)
            except Exception as e:
                # Didn't work, this may or may not be a problem, we'll find out
                # in the next block of code
                print(str(e))
                pass

        if not isinstance(exp_result, dict):
            # Ok, this is still not a dict, what should we do?
            if force_dict:
                raise TypeError("Expression needs to eval to a dict: "
                                "{}".format(expression))

        return exp_result

    def connected(self):
        return self._es.ping()

    def search(self, doc_type, body=None, params=None):
        doc_type = doc_type if doc_type else self.doc_type
        body = body if body else {}
        params = params if params else {}
        return self._es.search(index=self.index, doc_type=doc_type,
                               body=body, params=params)
