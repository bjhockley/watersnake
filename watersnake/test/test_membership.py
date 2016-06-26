import membership

# Related third party imports
import twisted.trial.unittest
import mock



class TestWaterSnake(twisted.trial.unittest.TestCase):
    """
    Tests the Watersnake library
    """
    timeout = 180

    def setUp(self):
        """Perform test setup."""

    def tearDown(self):
        """test Teardown."""


    def _create_harness(self):
        self.transport = membership.MessageTransport()
        self.router = membership.MessageRouter(transport)
        self.memb = membership.Membership(router)

    def test_instantiation(self):
        """Simply instantiate stuff"""


