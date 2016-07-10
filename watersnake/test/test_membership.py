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
        self.transport = membership.LoopbackMessageTransport()
        self.router = membership.MessageRouter(self.transport)
        self.members = []
        global_members = ["a", "b", "c"]
        for member_id in global_members:
            remote_members = [ membership.RemoteMember(x) for x in global_members if x != member_id ]
            self.members.append(membership.Membership(member_id, remote_members, self.router))


    def test_simple_non_swim_propagation(self):
        """Test simple non-swim message propagation (all nodes directly broadcast all
        messages to all other nodes) """
        self._create_harness()
        sending_member = self.members[0]
        receiving_members = self.members[1:]
        self.assertTrue(all([recipient.last_received_message == None for recipient in receiving_members]))
        sending_member.broadcast_message("Hello")
        self.assertTrue(all([recipient.last_received_message == "Hello" for recipient in receiving_members]))
        self.assertEqual(self.transport.sent_messages, 2)
        self.assertEqual(self.transport.received_messages, 2)


    def test_instantiation(self):
        """Simply instantiate stuff"""
        self._create_harness()


