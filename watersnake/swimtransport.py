""" Implementation of a protocol based ont SWIM protocol described in
http://www.cs.cornell.edu/~asdas/research/dsn02-SWIM.pdf ("the paper") """
import watersnake.swimmsg as swimmsg


class MessageTransport(object):
    """Abstract base class for real "MessageTransport" classes capable of
    sending and receiving messages using the network """
    def __init__(self):
        self.message_router = None
        self.sent_messages = 0
        self.received_messages = 0
        self.sent_bytes = 0
        self.received_bytes = 0

    def register_message_router(self, message_router):
        """Hook transport up to the message router object so we can deliver
        incoming messages to whoever is interested in handling them"""
        self.message_router = message_router

    def send_message_to(self, address, message, from_sender):
        """Send message to the member identified by address"""
        self.sent_messages += 1
        serialised_buff = swimmsg.SWIMJSONMessageSerialiser.to_buffer(message)
        self.sent_bytes = self.sent_bytes + len(serialised_buff)
        self.send_message_impl(address, serialised_buff, from_sender)

    def send_message_impl(self, address, message, from_sender):
        """Takes care of the nuts and bolts of message transmission"""
        # Derived class should hook this up to a socket

    def on_incoming_message(self, address, message, from_sender):
        """We've received a message off the wire and need to route it to any
        local objects that may be interested in this message. """
        self.received_messages += 1
        self.received_bytes = self.received_bytes + len(message)
        message = swimmsg.SWIMJSONMessageSerialiser.from_buffer(message)
        self.message_router.on_incoming_message(address, message, from_sender)
        # Derived class should hook this up to a socket


class LoopbackMessageTransport(MessageTransport):
    """ This is a specialization of MessageTransport that can only transport
     messages to other local objects (i.e. can't use a real network).
    Intended for unit testing or modeling use; not production use. """
    def __init__(self):
        MessageTransport.__init__(self)
        self._blocked_routes = []

    def simulate_partition_between(self, from_address, to_address):
        """ Simulates a uni-directional routing problem between from_address and
        to_address that causes all packets sent in that direction to be
        dropped. """
        self._blocked_routes.append((from_address, to_address))

    def send_message_impl(self, address, message, from_sender):
        """For LoopbackMessageTransport all reachable entities are local
        in-process objects, so sending a message can just be treated
        the same way as receiving a message """
        #print "%s => %s : %s" % (from_sender, address, message)
        if (from_sender, address) in self._blocked_routes:
            # print "*Partitions*: dropping message from %s to %s : %s " % (
            # from_sender, address, message
            # )
            pass
        else:
            self.on_incoming_message(address, message, from_sender)


class MessageRouter(object):
    """ The Message Router is responsible for routing outgoing messages sent to
    a logical destination over the appropriate transport to the correct
    destination, and routing incoming messages to the correct 'Membership' object.
    """
    def __init__(self, message_transport):
        self.transport = message_transport
        self.transport.register_message_router(self)
        self.members = {}

    def register_for_messages(self, member_id, member):
        """ The object 'member' wishes to receive all messages sent
        to 'member_id' """
        self.members[member_id] = member

    def on_incoming_message(self, address, message, from_sender):
        """ We've received a 'message' from 'from_sender' sent to 'address' """
        self.members[address].on_incoming_message(message, from_sender)

    def send_message_to(self, recipient_member_id, message, from_sender):
        """ Send  'message' (from 'from_sender') to the recipient identifed by
        'recipient_member_id'"""
        self.transport.send_message_to(
            recipient_member_id,
            message,
            from_sender
        )


