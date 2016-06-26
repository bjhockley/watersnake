"""
"""

class MessageTransport(object):
    """
    """
    def __init__(self):
        """
        """
        self.message_router = None

    def register_message_router(self, message_router):
        """"""
        self.message_router = message_router



class MessageRouter(object):
    """
    """
    def __init__(self, message_transport):
        """
        """
        self.message_transport = message_transport
        self.message_transport.register_message_router(self)

    def on_incoming_message(self):
        """
        """
        pass

    def send_message_to(self, remote_member, message):
        """
        """
        pass


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

    def __init__(self, expected_remote_members, messagerouter):
        """
        """
        self.messagerouter = messagerouter
        self.incarnation_number = 0
        self.expected_remote_members = expected_remote_members
        self.alive_remote_members = []



class RemoteMember(object):
    """  Represents a remote member of the distributed process group
    """

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
    def suspect(self, other_remote_member_id):
        pass

    def alive(self, remote_member_id):
        pass

    def confirm(self, remote_member_id):
        pass
