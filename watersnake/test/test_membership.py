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


    def _create_harness(self, n_members):
        self.transport = membership.LoopbackMessageTransport()
        self.router = membership.MessageRouter(self.transport)
        self.members = []
        global_members = [chr(n) for n in range(65, 65+n_members)]
        #print "Global_members=", global_members
        for member_id in global_members:
            remote_members = [ membership.RemoteMember(x) for x in global_members if x != member_id ]
            self.members.append(membership.Membership(member_id, remote_members, self.router))


    def test_simple_message_broadcast(self):
        """Test single node broadcasting to all all other nodes (this is not SWIM)"""
        self._create_harness(n_members=3)
        sending_member = self.members[0]
        receiving_members = self.members[1:]
        self.assertTrue(all([recipient.last_received_message == None for recipient in receiving_members]))
        sending_member.broadcast_message("Hello")
        self.assertTrue(all([recipient.last_received_message == "Hello" for recipient in receiving_members]))
        self.assertEqual(self.transport.sent_messages, 2)
        self.assertEqual(self.transport.received_messages, 2)


    def _test_all_broadcast_alive_non_swim(self, n_members):
        """Test simple non-swim message propagation where all nodes directly broadcast to all other nodes
        that they are alive """
        self._create_harness(n_members=n_members)

        self.assertTrue(all([recipient.last_received_message == None for recipient in self.members]))

        for sending_member in self.members:
            receiving_members = [member for member in self.members if member != sending_member]
            sending_member_is_alive_msg = membership.alive(sending_member.member_id)
            sending_member.broadcast_message(sending_member_is_alive_msg)
            self.assertTrue(all([recipient.last_received_message == sending_member_is_alive_msg for recipient in receiving_members]))

        return (self.transport.sent_messages, self.transport.received_messages)

    def test_all_broadcast_alive_non_swim_n_members(self):
        """Verify message counts for different size process groups using inefficient non-swim broadcast"""
        for n_members in range(2, 32):
            sent, recvd = self._test_all_broadcast_alive_non_swim(n_members=n_members)
            print "members=%s \tsent=%s \trecvd=%s"  % (n_members, sent, recvd)
            # Messages sent and recvd = (n-1) * n  for a group size of n
            self.assertEqual(sent, (n_members -1) * n_members)
            self.assertEqual(recvd, (n_members -1) * n_members)


    def test_swim_ping_ack(self):
        """Test SWIM ping message is responded to with an ack"""
        self._create_harness(n_members=3)
        sending_member = self.members[0]
        receiving_members = self.members[1:]
        self.assertEqual(sending_member.last_received_message, None)
        self.assertEqual(sending_member.received_messages, 0)
        self.assertTrue(all([recipient.last_received_message == None for recipient in receiving_members]))
        for member in sending_member.expected_remote_members:
            sending_member.send_message_to_member('ping', member)
        self.assertTrue(all([recipient.last_received_message == "ping" for recipient in receiving_members]))

        # 2 pings should have been sent by sending_member; 2 acks should have been received in response
        self.assertEqual(self.transport.sent_messages, 4)
        self.assertEqual(self.transport.received_messages, 4)
        self.assertEqual(sending_member.last_received_message, "ack")
        self.assertEqual(sending_member.received_messages, 2)

    def test_instantiation(self):
        """Simply instantiate stuff"""
        self._create_harness(n_members=20)

    def test_serialisation_deserialisation(self):
        """Test message serialisation/deserialisation"""
        for message_name in membership.SWIMMessage.MESSAGE_NAMES:
            mess = membership.SWIMMessage(message_name, u"from", u"to", {u"meta": u"data"}, {u"piggyback": u"data"})
            serialised_mess_buff = membership.SWIMJSONMessageSerialiser.serialise_to_buffer(mess)
            deserialised_mess = membership.SWIMJSONMessageSerialiser.deserialise_from_buffer(serialised_mess_buff)
            self.assertEqual(str(mess), str(deserialised_mess))
            self.assertEqual(mess.message_name, deserialised_mess.message_name)
            self.assertEqual(mess.from_address, deserialised_mess.from_address)
            self.assertEqual(mess.to_address, deserialised_mess.to_address)
            self.assertEqual(mess.meta_data, deserialised_mess.meta_data)
            self.assertEqual(mess.piggyback_data, deserialised_mess.piggyback_data)
            self.assertEqual(mess, deserialised_mess)

