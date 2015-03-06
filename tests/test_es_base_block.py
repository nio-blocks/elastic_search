import logging
from ..es_base_block import ESBase

from nio.util.support.block_test_case import NIOBlockTestCase
from ..tests import elasticsearch_running


class TestESBase(NIOBlockTestCase):
    """ Tests basic elasticsearch functionality provided by
    ESBase class
    """

    def setUp(self):
        super().setUp()
        self._outcome.success = elasticsearch_running()

    def test_connected(self):
        blk = ESBase()
        self.configure_block(blk, {
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk.connected())
