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
        self.tick_count = 0

    def do_tick(self):
        """Simulate tick timer firing """
        for member in self.members:
            member.tick(self.tick_count * membership.SWIM.T)
        self.tick_count = self.tick_count + 1

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
        test_msg = membership.test()
        sending_member.broadcast_message(test_msg)
        self.assertTrue(all([recipient.last_received_message == test_msg for recipient in receiving_members]))
        self.assertEqual(self.transport.sent_messages, 2)
        self.assertEqual(self.transport.received_messages, 2)


    def _test_all_broadcast_alive_non_swim(self, n_members):
        """Test simple non-swim message propagation where all nodes directly broadcast to all other nodes
        that they are alive """
        self._create_harness(n_members=n_members)

        self.assertTrue(all([recipient.last_received_message == None for recipient in self.members]))

        for sending_member in self.members:
            receiving_members = [member for member in self.members if member != sending_member]
            test_msg = membership.test()
            sending_member.broadcast_message(test_msg)
            self.assertTrue(all([recipient.last_received_message == test_msg for recipient in receiving_members]))

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
        for member in self.members:
            member.start()

        sending_member = self.members[0]
        receiving_members = self.members[1:]
        self.assertEqual(sending_member.last_received_message, None)
        self.assertEqual(sending_member.received_messages, 0)
        self.assertTrue(all([recipient.last_received_message == None for recipient in receiving_members]))
        ping_msg = membership.ping()
        ack_msg = membership.ack()
        for member in sending_member.expected_remote_members:
            sending_member.send_message_to_member_id(ping_msg, member.remote_member_id)
        self.assertTrue(all([recipient.last_received_message == ping_msg for recipient in receiving_members]))

        # 2 pings should have been sent by sending_member; 2 acks should have been received in response
        self.assertEqual(self.transport.sent_messages, 4)
        self.assertEqual(self.transport.received_messages, 4)
        self.assertEqual(sending_member.last_received_message, ack_msg)
        self.assertEqual(sending_member.received_messages, 2)

    def test_instantiation(self):
        """Simply instantiate stuff"""
        self._create_harness(n_members=20)

    def test_serialisation_deserialisation(self):
        """Test message serialisation/deserialisation"""
        for message_name in membership.SWIMMessage.MESSAGE_NAMES:
            mess = membership.SWIMMessage(message_name, meta_data={u"meta": u"data"}, piggyback_data={u"piggyback": u"data"})
            serialised_mess_buff = membership.SWIMJSONMessageSerialiser.serialise_to_buffer(mess)
            deserialised_mess = membership.SWIMJSONMessageSerialiser.deserialise_from_buffer(serialised_mess_buff)
            self.assertEqual(str(mess), str(deserialised_mess))
            self.assertEqual(mess.message_name, deserialised_mess.message_name)
            self.assertEqual(mess.meta_data, deserialised_mess.meta_data)
            self.assertEqual(mess.piggyback_data, deserialised_mess.piggyback_data)
            self.assertEqual(mess, deserialised_mess)

    def test_swim_ping_req(self):
        """Test SWIM ping_req message behaviour.
        Node a should be able to send a ping_req(c) message to node b
        in order to get node b to ping c on its behalf."""
        self._create_harness(n_members=3)
        for member in self.members:
            member.start()
        a = self.members[0]
        b = self.members[1]
        c = self.members[2]
        self.assertEqual(a.last_received_message, None)
        self.assertEqual(b.received_messages, 0)
        self.assertTrue(all([other_member.last_received_message == None for other_member in [b, c]]))
        ping_req_msg = membership.ping_req(a.member_id,
                                           c.member_id)
        ping_req_ack_msg = membership.ping_req_ack(a.member_id,
                                                   c.member_id)
        ping_msg = membership.ping(meta_data={u'member_id_to_ping': u'C', u'requested_by_member_id': u'A'})
        ack_msg = membership.ack(meta_data={u'member_id_to_ping': u'C', u'requested_by_member_id': u'A'})

        a.send_message_to_member_id(ping_req_msg, b.member_id)
        self.assertEqual(c.last_received_message, ping_msg)
        self.assertEqual(b.last_received_message, ack_msg, "%s != %s" % (b.last_received_message, ack_msg))
        self.assertEqual(a.last_received_message, ping_req_ack_msg)

        self.assertEqual(a.received_messages, 1)
        self.assertEqual(b.received_messages, 2)
        self.assertEqual(c.received_messages, 1)

        # 4 messages should have been sent (ping_req, ping, ack, ping_req_ack)
        self.assertEqual(self.transport.sent_messages, 4)
        self.assertEqual(self.transport.received_messages, 4)

    def test_statefulness(self):
        """Test statefulness"""
        self._create_harness(n_members=3)

        for member in self.members:
            assert(all([remote_member.state == "unknown" for remote_member in member.expected_remote_members]))
            self.assertEqual(len(member.expected_remote_members), 2)

        for member in self.members:
            member.start()

        for member in self.members:
            member.tick(0)

        for member in self.members:
            # After 1 tick, some but not all nodes should have been pinged and found to be alive
            assert(any([remote_member.state == "alive" for remote_member in member.expected_remote_members]))
            assert(any([remote_member.state == "unknown" for remote_member in member.expected_remote_members]))
            assert(not all([remote_member.state == "alive" for remote_member in member.expected_remote_members]))

        for member in self.members:
            member.tick(1 * membership.SWIM.T)

        # with 2 remote members, after 2 ticks, all nodes should have been pinged and found to be alive (as we have
        # implemented the "strong completeness" round-robin mechanism described in section 4.3 of the SWIM paper)
        for member in self.members:
            # states = [remote_member.state for remote_member in member.expected_remote_members]
            # print "For member %s remote_member states are %s" % (member, states)
            assert(all([remote_member.state == "alive" for remote_member in member.expected_remote_members]))

    def test_partial_partition(self):
        """Test that if a node cannot be pinged directly that the ping_req can establish liveness"""
        self._create_harness(n_members=3)

        for member in self.members:
            assert(all([remote_member.state == "unknown" for remote_member in member.expected_remote_members]))
            self.assertEqual(len(member.expected_remote_members), 2)

        self.transport.simulate_network_partition_between("A", "B")
        for member in self.members:
            member.start()

        self.do_tick()

        for member in self.members:
            # After 1 tick, some but certainly not all nodes may have been pinged and found to be alive
            assert(not all([remote_member.state == "alive" for remote_member in member.expected_remote_members]))

        self.do_tick()
        self.do_tick()
        self.do_tick()

        # with 2 remote members, after 4 ticks (assuming zero network latency), all nodes should have been pinged and found to be alive (as we have
        # implemented the "strong completeness" round-robin mechanism described in section 4.3 of the SWIM paper)
        for member in self.members:
            # states = [remote_member.state for remote_member in member.expected_remote_members]
            # print "For member %s remote_member states are %s" % (member, states)
            assert(all([remote_member.state == "alive" for remote_member in member.expected_remote_members]))


    def test_full_partition(self):
        """Test that if a node cannot be pinged directly or indirectly that it is eventually marked as dead"""
        self._create_harness(n_members=3)

        for member in self.members:
            assert(all([remote_member.state == "unknown" for remote_member in member.expected_remote_members]))
            self.assertEqual(len(member.expected_remote_members), 2)

        self.transport.simulate_network_partition_between("A", "B")
        self.transport.simulate_network_partition_between("A", "C")
        self.transport.simulate_network_partition_between("B", "A")
        self.transport.simulate_network_partition_between("C", "A")
        for member in self.members:
            member.start()

        self.do_tick()

        for member in self.members:
            # After 1 tick, some but certainly not all nodes may have been pinged and found to be alive
            assert(not all([remote_member.state == "alive" for remote_member in member.expected_remote_members]))
            assert(not all([remote_member.state == "dead" for remote_member in member.expected_remote_members]))

        self.do_tick()
        self.do_tick()
        self.do_tick()
        self.do_tick()
        # self.do_tick()
        # self.do_tick()

        # with 2 remote members, after 4 ticks (assuming zero network latency), all nodes should have been pinged and found to be alive (as we have
        # implemented the "strong completeness" round-robin mechanism described in section 4.3 of the SWIM paper)
        for member in self.members:
            # states = [(remote_member.remote_member_id, remote_member.state) for remote_member in member.expected_remote_members]
            # print "For member %s remote_member states are %s" % (member, states)
            assert(any([remote_member.state == "dead" for remote_member in member.expected_remote_members]))

