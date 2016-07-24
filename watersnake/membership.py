"""
"""

import json


def suspect(other_remote_member_id):
    """ """
    return json.dumps({ "suspect" : [other_remote_member_id] })

def alive(remote_member_id):
    """ """
    return json.dumps({ "alive" : [remote_member_id] })

def confirm(remote_member_id):
    """ """
    return json.dumps({ "confirm" : [remote_member_id] })


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

    def send_message_to(self, address, message):
        """Send message to the member identified by address"""
        self.sent_messages += 1
        self.send_message_impl(address, message)

    def send_message_impl(self, address, message):
        """Takes care of the nuts and bolts of message transmission"""
        # FIXME: need to hook this up to a socket
        pass

    def on_incoming_message(self, address, message):
        """We've received a message off the wire and need to route it to any
        local objects that may be interested in this message. """
        self.received_messages += 1
        self.message_router.on_incoming_message(address, message)
        # FIXME: need to hook this up to a socket


class LoopbackMessageTransport(MessageTransport):
    """ This is a specialization of MessageTransport that can only transport messages
    to other local objects (i.e. can't use a real network).  Useful for testing. """
    def __init__(self):
        """
        """
        MessageTransport.__init__(self)

    def send_message_impl(self, address, message):
        """As all objects reachable over LoopbackMessageTransport are local, sending
        a message can just be treated the same way as receiving a message"""
        self.on_incoming_message(address, message)


class MessageRouter(object):
    """
    """
    def __init__(self, message_transport):
        """
        """
        self.transport = message_transport
        self.transport.register_message_router(self)
        self.members = {}

    def register_for_messages_for_member(self, member_id, member):
        """ The object 'member' wishes to receive all messages routed to recipient identified by member_id
        """
        self.members[member_id] = member

    def on_incoming_message(self, address, message):
        """
        """
        self.members[address].on_incoming_message(message)

    def send_message_to(self, remote_member, message):
        """
        """
        recipient_member_id = remote_member.remote_member_id
        self.transport.send_message_to(recipient_member_id, message)


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
        self.messagerouter.register_for_messages_for_member(self.member_id, self)
        #self.broadcast_message(alive(self.member_id))

    def __str__(self):
        return "Membership(member_id=%s)" % self.member_id

    def broadcast_message(self, message):
        """ """
        for member in self.expected_remote_members:
            self.messagerouter.send_message_to(member, message)

    def on_incoming_message(self, message):
        self.last_received_message = message
        #print "%s got msg=%s" % (str(self), message)

class RemoteMember(object):
    """  Represents a remote member of the distributed process group """
    def __init__(self, remote_member_id):
        """
        """
        self.remote_member_id = remote_member_id
        self.last_observed_incarnation_number = None

    def ping(self, piggbyback_data):
        """Send a ping (along with some piggyback data) to this remote member"""
        pass

    def ping_req(self, other_remote_member_id):
        """Send a ping_req (along with some piggyback data) to this remote
        member, asking it to ping other_remote_member_id on our behalf"""
        pass


    # Do these belong here?
