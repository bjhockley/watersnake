""" Implementation of a protocol based ont SWIM protocol described in http://www.cs.cornell.edu/~asdas/research/dsn02-SWIM.pdf """

import json
import random
import itertools
from twisted.internet import reactor


class SWIM(object):
    """Class for namespacing SWIM protocol config parameters.  See original paper."""
    T = 2.0  # SWIM protocol period (in seconds)
    K = 3   # SWIM protocol failure detection subgroup size



class SWIMMessage(object):
    MESSAGE_NAMES = [ u'ping', u'ack', u'ping_req', u'ping_req_ack', u'test' ] # 'suspect', 'alive'
    def __init__(self, message_name, meta_data=None, piggyback_data=None):
        assert message_name in SWIMMessage.MESSAGE_NAMES, 'Invalid message name: %s not in %s' % (message_name,
                                                                                                  SWIMMessage.MESSAGE_NAMES)
        self.message_name = unicode(message_name)
        if meta_data is not None:
            assert isinstance(meta_data, dict)
        self.meta_data = meta_data
        if piggyback_data is not None:
            assert isinstance(piggyback_data, dict)
        self.piggyback_data = piggyback_data

    def __str__(self):
        return '%s(meta_data=%s, piggyback_data=%s)' % (
            self.message_name, self.meta_data, self.piggyback_data
        )

    def __eq__(self, other):
        return ( type(self) == type(other) and
                 self.message_name == other.message_name and
                 self.meta_data == other.meta_data and
                 self.piggyback_data == other.piggyback_data)


class SWIMDeserialisationException(Exception):
    pass

class SWIMJSONMessageSerialiser(object):
    """A class capable of serialising / deserialising SWIMMessages using the JSON format"""
    @staticmethod
    def serialise_to_buffer(swim_message):
        """Serialises the swim_message object to a form suitable for sending on the wire"""
        message_as_dict = {
            u"message_name" : swim_message.message_name,
            u"meta_data" : swim_message.meta_data,
            u"piggyback_data" : swim_message.piggyback_data
        }
        return json.dumps(message_as_dict)

    @staticmethod
    def deserialise_from_buffer(buffer):
        """Parses buffer and deserialises the swim_message object and returns it if possible;
        raises a SWIMDeserialisationException otherwise"""
        try:
            message_as_dict = json.loads(buffer)
            return SWIMMessage(message_name=message_as_dict["message_name"],
                               meta_data=message_as_dict["meta_data"],
                               piggyback_data=message_as_dict["piggyback_data"])
        except Exception as _err:
            raise SWIMDeserialisationException()



def ping(meta_data=None, piggyback_data=None):
    """ Factory function to create a ping SWIM message """
    return SWIMMessage(message_name="ping",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)

def ack(meta_data=None, piggyback_data=None):
    """ Factory function to create an ack SWIM message """
    return SWIMMessage(message_name="ack",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)

def test(meta_data=None, piggyback_data=None):
    """ Factory function to create a test SWIM message """
    return SWIMMessage(message_name="test",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)


def ping_req(requested_by_member_id, member_id_to_ping, piggyback_data=None):
    """ Factory function to create a ping_req SWIM message """

    meta_data = { "requested_by_member_id" : requested_by_member_id,
                  "member_id_to_ping" : member_id_to_ping }
    return SWIMMessage(message_name="ping_req",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)

def ping_req_ack(requested_by_member_id, member_id_to_ping, piggyback_data=None):
    """ Factory function to create a ping_req_ack SWIM message """
    meta_data = { "requested_by_member_id" : requested_by_member_id,
                  "member_id_to_ping" : member_id_to_ping }
    return SWIMMessage(message_name="ping_req_ack",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)


class MessageTransport(object):
    """ """
    def __init__(self):
        """ """
        self.message_router = None
        self.sent_messages = 0
        self.received_messages = 0
        self.sent_bytes = 0
        self.received_bytes = 0

    def register_message_router(self, message_router):
        """"""
        self.message_router = message_router

    def send_message_to(self, address, message, from_sender):
        """Send message to the member identified by address"""
        self.sent_messages += 1
        serialised_mess_buff = SWIMJSONMessageSerialiser.serialise_to_buffer(message)
        self.sent_bytes = self.sent_bytes + len(serialised_mess_buff)
        self.send_message_impl(address, serialised_mess_buff, from_sender)

    def send_message_impl(self, address, message, from_sender):
        """Takes care of the nuts and bolts of message transmission"""
        # FIXME: need to hook this up to a socket

    def on_incoming_message(self, address, message, from_sender):
        """We've received a message off the wire and need to route it to any
        local objects that may be interested in this message. """
        self.received_messages += 1
        self.received_bytes = self.received_bytes + len(message)
        deserialised_mess = SWIMJSONMessageSerialiser.deserialise_from_buffer(message)
        self.message_router.on_incoming_message(address, deserialised_mess, from_sender)
        # FIXME: need to hook this up to a socket


class LoopbackMessageTransport(MessageTransport):
    """ This is a specialization of MessageTransport that can only transport messages
    to other local objects (i.e. can't use a real network).  Useful for testing. """
    def __init__(self):
        """
        """
        MessageTransport.__init__(self)
        self._blocked_routes = []

    def simulate_network_partition_between(self, from_address, to_address):
        self._blocked_routes.append((from_address, to_address))

    def send_message_impl(self, address, message, from_sender):
        """As all objects reachable over LoopbackMessageTransport are local, sending
        a message can just be treated the same way as receiving a message"""
        #print "%s => %s : %s" % (from_sender, address, message)
        if (from_sender, address) in self._blocked_routes:
            # print "*Simulated partition*: dropping message from %s to %s : %s " % (from_sender, address, message)
            pass
        else:
            self.on_incoming_message(address, message, from_sender)


class MessageRouter(object):
    """ The Message Router is responsible for routing outgoing messages sent to a logical destination
    over the appropriate transport to the correct destination, and routing incoming messages to the
    correct 'Membership' object.
    """
    def __init__(self, message_transport):
        """ """
        self.transport = message_transport
        self.transport.register_message_router(self)
        self.members = {}

    def register_for_messages_for_member(self, member_id, member):
        """ The object 'member' wishes to receive all messages routed to recipient identified by member_id"""
        self.members[member_id] = member

    def on_incoming_message(self, address, message, from_sender):
        """
        """
        self.members[address].on_incoming_message(message, from_sender)

    def send_message_to(self, recipient_member_id, message, from_sender):
        """
        """
        self.transport.send_message_to(recipient_member_id, message, from_sender)




class Membership(object):
    """  Each member of the distributed process group
    should instantiate a single instance of this class.

    This class is responsible for:
     -  maintaining the membership of this local process in the distributed process group
     -  maintaining an up-to-date local view of the membership of the distributed process
        group
     -  detecting failures in remote members
     -  disseminating information about joined/left/failed members
    """

    def __init__(self, member_id, expected_remote_members, messagerouter):
        """
        """
        self.member_id = member_id
        self.messagerouter = messagerouter
        self.incarnation_number = 0
        self.expected_remote_members = expected_remote_members
        self.alive_remote_members = []
        self.last_received_message = None
        self.received_messages = 0
        self.messagerouter.register_for_messages_for_member(self.member_id, self)
        self.nodes_to_ping = None

    def __str__(self):
        return "Membership(member_id=%s)" % self.member_id

    def start(self):
        for remote_member in self.expected_remote_members:
            remote_member.start(self)

    def tick(self, time_now):
        """time_now should be some sort of monotonic time"""
        new_node_to_ping = self._select_node_to_ping()
        if new_node_to_ping.is_currently_being_checked():
            # We've already got a ping in progress for this node. Let's wait for that to succeed/fail.
            pass
        else:
            # self.send_message_to_member_id(ping(), new_node_to_ping.remote_member_id)
            new_node_to_ping.begin_checking_for_failure(time_now)

        # prod each node to see if it needs to change state/time out etc.
        for node in self.expected_remote_members:
            node.on_tick(time_now)

    def _select_node_to_ping(self):
        """Select a node to ping using randomised round-robin as per section 4.3 of the SWIM paper
        (in order to provide time bounded strong completeness)"""
        if self.nodes_to_ping is None:
            random.shuffle(self.expected_remote_members)
            self.nodes_to_ping = itertools.cycle(self.expected_remote_members)
        return self.nodes_to_ping.next()

    def select_nodes_to_ping_req(self, node_id_to_ping):
        """Returns K nodes for the failure detection subgroup """
        nodes_to_ping_req = []
        if self.nodes_to_ping is not None:
            aux = list(self.expected_remote_members)
            random.shuffle(aux)
            nodes_to_ping_req = [ node for node in aux if node.remote_member_id != node_id_to_ping ]
        return nodes_to_ping_req[: SWIM.K ]

    def broadcast_message(self, message):
        """ Broadcast a message to all known members """
        for member in self.expected_remote_members:
            self.send_message_to_member_id(message, member.remote_member_id)

    def send_message_to_member_id(self, message, remote_member_id):
        """ Send a message to a specific member"""
        self.messagerouter.send_message_to(remote_member_id, message, self.member_id)

    def _logical_sender_from_id(self, from_sender):
        """Returns the RemoteMember from expected_remote_members with the
        id matching 'from_sender' if one can be found; None otherwise"""
        logical_from_sender = None
        for member in self.expected_remote_members:
            if member.remote_member_id == from_sender:
                logical_from_sender = member
                break
        return logical_from_sender

    def on_incoming_message(self, message, from_sender_id):
        """We've received a message from the sender identified by from_sender_id"""
        self.last_received_message = message
        self.received_messages = self.received_messages + 1
        logical_from_sender = self._logical_sender_from_id(from_sender_id)
        if logical_from_sender is None:
            # Could be a message sent by recently added or removed node; log & ignore
            print "Warning: %s got message from unknown sender '%s' : %s " % (self.member_id, from_sender_id, message)
        else:
             self.handle_incoming_message(message, logical_from_sender)

    def handle_incoming_message(self, message, remote_member):
        """We've received a message from the specified remote_member"""
        remote_member.handle_incoming_message(message)

    def member_indirectly_reachable(self, member_id, reachable_from_member_id, message):
        assert message.message_name in ['ping_req_ack']
        # print "%s reports that they can reach %s (whereas we=%s cannot) " % (reachable_from_member_id, member_id, self.member_id)
        alive_member = self._logical_sender_from_id(member_id)
        if alive_member is None:
            # Could be a message sent by recently added or removed node; log & ignore
            print "Warning: %s got member_indirectly_reachable for unknown member '%s' : %s" % (self.member_id, alive_member, message)
        else:
            alive_member.handle_incoming_message(message)

class FailureDetectionTransaction(object):
    """ Class to handle failure detection """
    def __init__(self, time_now, owner, remote_member_id):
        self.start_time = time_now
        self.owner = owner
        self.remote_member_id = remote_member_id
        self.ack_received = False
        self.ping_req_ack_received = False
        self.response_timeout = 2
        self.state = "idle"

    def start(self):
        self.state = "ping_sent"
        self.owner.send_ping()

    def on_tick(self, time_now):
        """time_now should be some sort of monotonic time"""
        # print "FDT %s on_tick start_time=%s time_now=%s state=%s" % (self.remote_member_id, self.start_time, time_now, self.state)
        if self.state == "ping_sent":
            if time_now > self.start_time + self.response_timeout:
                # Direct ping has failed; let's try indirect ping (ping_req)
                self.state = "ping_req_sent"
                self.owner.send_ping_reqs()
        elif self.state == "ping_req_sent":
            if time_now > self.start_time + (self.response_timeout * 2):
                # print "FailureDetectionTransaction not heard back regarding %s; assuming failure" % self.remote_member_id
                self.state = "failure_detected"
                self.owner.node_failed()

    def on_ack(self):
        self.state = "alive"
        self.owner.node_alive()

    def on_ping_req_ack(self):
        self.state = "alive"
        self.owner.node_alive()

class RemoteMember(object):
    """  Represents a remote member of the distributed process group; handles  messages communicated
    between this node and the remote member and maintains 'state' about the remote member's liveness. """
    # callLater = reactor.callLater
    def __init__(self, remote_member_id):
        """
        """
        self.remote_member_id = remote_member_id
        # self.last_observed_incarnation_number = None
        self.state = "unknown"
        self.failure_detection_transaction = None
        self.membership = None

    def __str__(self):
        return 'RemoteMember(remote_member_id=%s, state=%s)' % (
            self.remote_member_id, self.state
        )

    def start(self, membership):
        self.membership = membership

    def on_tick(self, time_now):
        if self.failure_detection_transaction is not None:
            self.failure_detection_transaction.on_tick(time_now)
        else:
            # We're not in the midst of a ping transaction; we need do nothing.
            pass

    def is_currently_being_checked(self):
        return self.failure_detection_transaction is not None

    def begin_checking_for_failure(self, time_now):
        assert self.membership is not None
        assert self.failure_detection_transaction is None
        self.failure_detection_transaction = FailureDetectionTransaction(time_now, self, self.remote_member_id)
        self.failure_detection_transaction.start()

    # def set_suspect(self):
    #     self.state = "suspect"

    def node_alive(self):
        self.state = "alive"
        self.failure_detection_transaction = None

    def node_failed(self):
        self.state = "dead"
        self.failure_detection_transaction = None

    def send_ping(self):
        """Send a ping (along with some piggyback data) to the remote member tracked by this object"""
        ping_msg = ping()
        self.membership.send_message_to_member_id(ping_msg, self.remote_member_id)

    def send_ping_reqs(self):
        """Our owner wishes to request that this remote member sends a ping (along with some piggyback data)
        to another remote member"""
        ping_req_msg = ping_req(self.membership.member_id, self.remote_member_id)
        members_to_send_ping_reqs_to = self.membership.select_nodes_to_ping_req(self.remote_member_id)
        for member in members_to_send_ping_reqs_to:
            self.membership.send_message_to_member_id(ping_req_msg, member.remote_member_id)

    def handle_incoming_message(self, message):
        """We've received a message from the wire"""
        # FIXME: if we are "dead" should we treat receipt of a message from a node as meaning that node is alive again?
        if self.failure_detection_transaction is not None and message.message_name in ['ack', 'ping_req_ack']:
            if message.message_name == 'ack':
                self.failure_detection_transaction.on_ack()
            elif message.message_name == 'ping_req_ack':
                self.failure_detection_transaction.on_ping_req_ack()
        else:
            if message.message_name == 'ping':
                # Always respond to a ping with an ack
                self.membership.send_message_to_member_id(ack(meta_data=message.meta_data), self.remote_member_id)
            elif message.message_name == 'ping_req':
                # On receipt of a ping_req, attempt to ping the node in question
                member_id_to_ping = message.meta_data.get("member_id_to_ping", None)
                self.membership.send_message_to_member_id(ping(meta_data=message.meta_data), member_id_to_ping)
            elif message.message_name == 'ack':
                # if this is an ack for a ping sent in response to a ping_req, we need to send
                # a ping_req_ack to the original requester.
                if message.meta_data is not None:
                    requested_by_member_id = message.meta_data.get("requested_by_member_id", None)
                    member_id_to_ping = message.meta_data.get("member_id_to_ping", None)
                    if requested_by_member_id and member_id_to_ping:
                        # Send the ping_req_ack to the member to sent the ping_req
                        self.membership.send_message_to_member_id(ping_req_ack(requested_by_member_id, member_id_to_ping), requested_by_member_id)
            elif message.message_name == 'ping_req_ack':
                requested_by_member_id = message.meta_data.get("requested_by_member_id", None)
                member_id_to_ping = message.meta_data.get("member_id_to_ping", None)
                if requested_by_member_id == self.membership.member_id and member_id_to_ping != self.remote_member_id:
                    if member_id_to_ping is not None:
                        # We've got a ping_req_ack that is of interest to one of our peers - bounce this over, via our owner
                        self.membership.member_indirectly_reachable(member_id_to_ping, self.remote_member_id, message)
