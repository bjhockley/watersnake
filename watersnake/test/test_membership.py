""" Unit tests for watersnake membership module. """
# Disable 'Line too long'                   pylint: disable=C0301
# Disable 'Too many public methods'         pylint: disable=R0904

import membership
import swimmsg

# Related third party imports
import twisted.trial.unittest


class TestWaterSnake(twisted.trial.unittest.TestCase):
    """
    Tests the Watersnake library
    """
    timeout = 180

    def __init__(self, method_name):
        twisted.trial.unittest.TestCase.__init__(self, method_name)
        self.transport = None
        self.router = None
        self.members = []
        self.tick_count = 0

    def do_tick(self):
        """Simulate tick timer firing """
        for member in self.members:
            member.tick(self.tick_count * membership.SWIM.T)
        self.tick_count = self.tick_count + 1

    def _create_harness(self, n_members):
        """ Create n unit testable Membership objects, each 'monitoring' the others,
        connected over a (fake) LoopbackMessageTransport (instead of a real network) """
        self.transport = membership.LoopbackMessageTransport()
        self.router = membership.MessageRouter(self.transport)
        self.members = []
        self.tick_count = 0
        global_members = [chr(n) if n < 127 else hex(n) for n in range(65, 65+n_members)]
        for member_id in global_members:
            remote_members = [ membership.RemoteMember(x) for x in global_members if x != member_id ]
            self.members.append(membership.Membership(member_id, remote_members, self.router))


    def test_simple_message_broadcast(self):
        """Test single node broadcasting to all all other nodes (this is not SWIM)"""
        self._create_harness(n_members=3)
        sending_member = self.members[0]
        receiving_members = self.members[1:]
        self.assertTrue(all([recipient.last_received_message == None for recipient in receiving_members]))
        test_msg = swimmsg.test()
        sending_member.broadcast_message(test_msg)
        self.assertTrue(all([recipient.last_received_message == test_msg for recipient in receiving_members]))
        self.assertEqual(self.transport.sent_messages, 2)
        self.assertEqual(self.transport.received_messages, 2)


    def _test_all_broadcast_alive(self, n_members):
        """Test simple non-SWIM message propagation where all nodes directly broadcast to all other nodes
        that they are alive """
        self._create_harness(n_members=n_members)

        self.assertTrue(all([recipient.last_received_message == None for recipient in self.members]))

        for sending_member in self.members:
            receiving_members = [member for member in self.members if member != sending_member]
            test_msg = swimmsg.test()
            sending_member.broadcast_message(test_msg)
            self.assertTrue(all([recipient.last_received_message == test_msg for recipient in receiving_members]))

        return (self.transport.sent_messages, self.transport.received_messages)

    def test_n_broadcast_alive(self):
        """Verify message counts for different size process groups using inefficient non-swim broadcast"""
        for n_members in range(2, 133, 10):
            sent, recvd = self._test_all_broadcast_alive(n_members=n_members)
            # With non-SWIM broadcast, all nodes will coalesce within just one "tick" (assuming zero network latency)
            bandwidth = ( (self.transport.sent_bytes + self.transport.received_bytes) / membership.SWIM.T ) / 1024.0
            print "members=%s \tsent=%s \trecvd=%s \tavg-b/w=%s kbps"  % (n_members, sent, recvd, bandwidth)
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
        ping_msg = swimmsg.ping()
        ack_msg = swimmsg.ack()
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

    def test_wire_format(self):
        """Test message serialisation/deserialisation"""
        for message_name in swimmsg.SWIMMessage.MESSAGE_NAMES:
            mess = swimmsg.SWIMMessage(message_name, meta_data={u"meta": u"data"}, piggyback_data={u"piggyback": u"data"})
            serialised_mess_buff = swimmsg.SWIMJSONMessageSerialiser.to_buffer(mess)
            deserialised_mess = swimmsg.SWIMJSONMessageSerialiser.from_buffer(serialised_mess_buff)
            self.assertEqual(str(mess), str(deserialised_mess))
            self.assertEqual(mess.message_name, deserialised_mess.message_name)
            self.assertEqual(mess.meta_data, deserialised_mess.meta_data)
            self.assertEqual(mess.piggyback_data, deserialised_mess.piggyback_data)
            self.assertEqual(mess, deserialised_mess)

    def test_deserialise_bad_message(self):
        """Test message deserialisation raises an exception on receipt of a grossly bad message"""
        self.assertRaises(swimmsg.SWIMDeserialisationException,
                          swimmsg.SWIMJSONMessageSerialiser.from_buffer, "an invalid message")

    def test_str(self):
        """Execute str representations"""
        self._create_harness(n_members=2)
        strmember = str(self.members[0])
        self.assertEqual(strmember, 'Membership(member_id=A)')
        strremotemember = str(self.members[0].expected_remote_members[0])
        self.assertEqual(strremotemember, 'RemoteMember(remote_member_id=B, state=unknown)')

    def test_direct_from_unk_sender(self):
        """Test that a direct message from an unknown/unexpected sender doesn't cause explosions"""
        self._create_harness(n_members=2)
        rogue_ping = swimmsg.ping()
        self.members[0].on_incoming_message(rogue_ping, "D")

    def test_indirect_unk_sender(self):
        """Test that an indirect message from an unknown/unexpected sender doesn't cause explosions"""
        self._create_harness(n_members=2)
        rogue_ping = swimmsg.ping_req_ack("A", "E")
        self.members[0].member_indirectly_reachable("E", "D", rogue_ping)

    def test_swim_ping_req(self):
        """Test SWIM ping_req message behaviour.
        Node a should be able to send a ping_req(c) message to node b
        in order to get node b to ping c on its behalf."""
        self._create_harness(n_members=3)
        for member in self.members:
            member.start()
        node_a = self.members[0]
        node_b = self.members[1]
        node_c = self.members[2]
        self.assertEqual(node_a.last_received_message, None)
        self.assertEqual(node_b.received_messages, 0)
        self.assertTrue(all([other_member.last_received_message == None for other_member in [node_b, node_c]]))
        ping_req_msg = swimmsg.ping_req(node_a.member_id,
                                        node_c.member_id)
        ping_req_ack_msg = swimmsg.ping_req_ack(node_a.member_id,
                                                node_c.member_id)
        ping_msg = swimmsg.ping(meta_data={u'member_id_to_ping': u'C', u'requested_by_member_id': u'A'})
        ack_msg = swimmsg.ack(meta_data={u'member_id_to_ping': u'C', u'requested_by_member_id': u'A'})

        node_a.send_message_to_member_id(ping_req_msg, node_b.member_id)
        self.assertEqual(node_c.last_received_message, ping_msg)
        self.assertEqual(node_b.last_received_message, ack_msg, "%s != %s" % (node_b.last_received_message, ack_msg))
        self.assertEqual(node_a.last_received_message, ping_req_ack_msg)

        self.assertEqual(node_a.received_messages, 1)
        self.assertEqual(node_b.received_messages, 2)
        self.assertEqual(node_c.received_messages, 1)

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
        """Test that if a node cannot be pinged directly that the ping_req can
        indirectly establish liveness"""
        self._create_harness(n_members=3)

        for member in self.members:
            assert(all([remote_member.state == "unknown" for remote_member in member.expected_remote_members]))
            self.assertEqual(len(member.expected_remote_members), 2)

        self.transport.simulate_partition_between("A", "B")
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

        self.transport.simulate_partition_between("A", "B")
        self.transport.simulate_partition_between("A", "C")
        self.transport.simulate_partition_between("B", "A")
        self.transport.simulate_partition_between("C", "A")
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

        # with 2 remote members, after 5 ticks (assuming zero network latency), all nodes should have been pinged and found to be alive (as we have
        # implemented the "strong completeness" round-robin mechanism described in section 4.3 of the SWIM paper)
        for member in self.members:
            # states = [(remote_member.remote_member_id, remote_member.state) for remote_member in member.expected_remote_members]
            # print "For member %s remote_member states are %s" % (member, states)
            assert(any([remote_member.state == "dead" for remote_member in member.expected_remote_members]))

