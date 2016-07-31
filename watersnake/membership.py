"""
"""

import json
from twisted.internet import reactor


class SWIM(object):
    """Class for namespacing SWIM protocol config parameters.  See """
    T = 2.0  # SWIM protocol period (in seconds)
    K = 3   # SWIM protocol failure detection subgroup size


def alive(remote_member_id):
    """ """
    return json.dumps({ "alive" : [remote_member_id] })

def ping(remote_member_id):
    """ """
    return json.dumps({ "ping" : [remote_member_id] })


class MessageTransport(object):
    """ """
    def __init__(self):
        """
        """
        self.message_router = None
        self.sent_messages = 0
        self.received_messages = 0

    def register_message_router(self, message_router):
        """"""
        self.message_router = message_router

    def send_message_to(self, address, message, from_sender):
        """Send message to the member identified by address"""
        self.sent_messages += 1
        self.send_message_impl(address, message, from_sender)

    def send_message_impl(self, address, message, from_sender):
        """Takes care of the nuts and bolts of message transmission"""
        # FIXME: need to hook this up to a socket
        pass

    def on_incoming_message(self, address, message, from_sender):
        """We've received a message off the wire and need to route it to any
        local objects that may be interested in this message. """
        self.received_messages += 1
        self.message_router.on_incoming_message(address, message, from_sender)
        # FIXME: need to hook this up to a socket


class LoopbackMessageTransport(MessageTransport):
    """ This is a specialization of MessageTransport that can only transport messages
    to other local objects (i.e. can't use a real network).  Useful for testing. """
    def __init__(self):
        """
        """
        MessageTransport.__init__(self)

    def send_message_impl(self, address, message, from_sender):
        """As all objects reachable over LoopbackMessageTransport are local, sending
        a message can just be treated the same way as receiving a message"""
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

    def send_message_to(self, remote_member, message, from_sender):
        """
        """
        recipient_member_id = remote_member.remote_member_id
        self.transport.send_message_to(recipient_member_id, message, from_sender)


class Membership(object):
    """  Each member of the distributed process group
    should instantiate a single instance of this class.

    This class is responsible for:
     -  maintaining the membership of this local process in the distributed process group
     -  maintaining an up-to-date local view of the membership of the distributed process
        group
     -  detecting failures in remote members
     -  disseminating informantion about joined/left/failed members
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

    def __str__(self):
        return "Membership(member_id=%s)" % self.member_id

    def broadcast_message(self, message):
        """ Broadcast a message to all known members """
        for member in self.expected_remote_members:
            self.send_message_to_member(message, member)

    def send_message_to_member(self, message, member):
        """ Send a message to a specific member"""
        self.messagerouter.send_message_to(member, message, self.member_id)

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
        # print "%s got %s from %s" % (str(self), message, from_sender)
        logical_from_sender = self._logical_sender_from_id(from_sender_id)
        if logical_from_sender is None:
            # Could be a message sent by recently added or removed node; log & ignore
            print "Warning: got message from unknown sender '%s'" % from_sender_id
        else:
             self.handle_incoming_message(message, logical_from_sender)

    def handle_incoming_message(self, message, remote_member):
        """We've received a message from the specified remote_member"""
        if message == 'ping':
            # Always respond to a ping with an ack
            self.send_message_to_member('ack', remote_member)


class RemoteMember(object):
    """  Represents a remote member of the distributed process group """
    # callLater = reactor.callLater
    def __init__(self, remote_member_id):
        """
        """
        self.remote_member_id = remote_member_id
        self.last_observed_incarnation_number = None
        self.state = None

    # def set_alive(self):
    #     self.state = "alive"
    #     self.callLater(SWIM.T * 2, self.set_suspect)

    # def set_suspect(self):
    #     self.state = "suspect"

    # def set_dead(self):
    #     self.state = "dead"

    # def ping(self, piggbyback_data):
    #     """Send a ping (along with some piggyback data) to the remote member tracked by this object"""
    #     pass

    # def ping_req(self, piggbyback_data, other_remote_member_id):
    #     """Our owner wishes to request that this remote member sends a ping (along with some piggyback data)
    #     to another remote member"""
    #     pass

    # Do these belong here?
