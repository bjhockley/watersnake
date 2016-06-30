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
        self.router = membership.MessageRouter(self.transport)
        self.members = []
        global_members = ["a", "b", "c"]
        for member_id in global_members:
            remote_members = [ membership.RemoteMember(x) for x in global_members if x != member_id ]
            self.members.append(membership.Membership(member_id, remote_members, self.router))


    def test_instantiation(self):
        """Simply instantiate stuff"""
        self._create_harness()


