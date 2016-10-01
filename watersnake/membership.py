""" Implementation of a protocol based on the SWIM protocol described in
http://www.cs.cornell.edu/~asdas/research/dsn02-SWIM.pdf ("the paper") """

# Disable 'Too many instance attributes'             pylint: disable=R0902


import random
import itertools
import watersnake.swimmsg as swimmsg
import watersnake.swimprotocol as swimprotocol

class Membership(object):
    """  Each member of the distributed process group
    should instantiate a single instance of this class.

    This class is responsible for:
     -  maintaining the membership of this local process in the distributed
        process group
     -  maintaining an up-to-date local view of the membership of the
        distributed process group
     -  detecting failures in remote members
     -  disseminating information about joined/left/failed members
    """

    def __init__(
            self,
            member_id,
            expected_remote_members,
            messagerouter,
            enable_infection_dissemination=True
        ):
        self.member_id = member_id
        self.messagerouter = messagerouter
        self.incarnation_number = 0 # Futures: should persist
        self.expected_remote_members = expected_remote_members
        self.alive_remote_members = []
        self.last_received_message = None
        self.received_messages = 0
        self.messagerouter.register_for_messages(self.member_id, self)
        self.nodes_to_ping = None
        self.enable_infection_dissemination = enable_infection_dissemination
        self._update_incarnation()

    def _update_incarnation(self):
        """We need to update our incarnation"""
        self.incarnation_number += 1

    def __str__(self):
        return "Membership(member_id=%s)" % self.member_id

    def start(self):
        """Prepare to check the liveness of remote members"""
        for remote_member in self.expected_remote_members:
            remote_member.start(self)

    def tick(self, time_now):
        """Time is advancing - we should check up on remote nodes
        (time_now should be some sort of monotonic time)"""
        new_node_to_ping = self._select_node_to_ping()
        if new_node_to_ping.is_currently_being_checked():
            # We've already got a ping in progress for this node. Let's
            # wait for that to succeed/fail.
            pass
        else:
            new_node_to_ping.begin_checking_for_failure(time_now)

        # prod each node to see if it needs to change state/time out etc.
        for node in self.expected_remote_members:
            node.on_tick(time_now)

    def _select_node_to_ping(self):
        """Select a node to ping using randomised round-robin as per
        section 4.3 of the paper.
        (helps to provide time bounded strong completeness)"""
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
            nodes_to_ping_req = [node for node in aux
                                 if node.remote_member_id != node_id_to_ping]
        return nodes_to_ping_req[:swimprotocol.SWIM.K]

    def broadcast_message(self, message):
        """ Broadcast a message to all known members """
        for member in self.expected_remote_members:
            self.send_message_to_member_id(message, member.remote_member_id)

    def get_piggyback_data_to_send(self):
        """Construct piggyback data for infection style dissemination
        inspired by section 4.1 of the paper"""
        # Futures: Limit the number of nodes we will report on (to give bounded
        # message size/load as opposed to fastest possible dissemination)

        alive_nodes = [(self.member_id, self.incarnation_number)]
        dead_nodes = []
        for member in self.expected_remote_members:
            if member.state == "alive":
                alive_nodes.append((
                    member.remote_member_id,
                    member.incarnation_number
                ))
            elif member.state == "dead":
                dead_nodes.append((
                    member.remote_member_id, member.incarnation_number
                ))
        piggyback_data = {
            "Alive"  : alive_nodes,
            "Dead"  : dead_nodes,
        }
        return piggyback_data

    def send_message_to_member_id(self, message, remote_member_id):
        """ Send a message to a specific member"""
        if self.enable_infection_dissemination:
            # Infection style dissemination is enabled. Let's inject
            # piggyback data <here>
            piggyback_data = self.get_piggyback_data_to_send()
            # assert message.piggyback_data is None
            message.piggyback_data = piggyback_data

        self.messagerouter.send_message_to(
            remote_member_id,
            message,
            self.member_id
        )

    def _remote_member_from_id(self, member_id):
        """Returns the RemoteMember from expected_remote_members with the
        id matching 'from_sender' if one can be found; None otherwise"""
        logical_from_sender = None
        for member in self.expected_remote_members:
            if member.remote_member_id == member_id:
                logical_from_sender = member
                break
        return logical_from_sender

    def on_incoming_message(self, message, from_sender_id):
        """We've received a message from the sender identified by
        'from_sender_id# """
        self.last_received_message = message
        self.received_messages = self.received_messages + 1
        logical_from_sender = self._remote_member_from_id(from_sender_id)
        if logical_from_sender is None:
            # Could be a message sent by recently added or
            # removed node; log & ignore.
            print "Warning: %s got message from unknown sender '%s':%s" % (
                self.member_id, from_sender_id, message
            )
        else:
            logical_from_sender.handle_incoming_message(message)

    def member_indirectly_reachable(
            self,
            member_id,
            reachable_from_member_id,
            message
        ):
        """We know that a node 'member_id' that wasn't directly reachable
        has been reached indirectly by node 'reachable_from_member_id' on
        our behalf. We must update internal state accordingly. """
        assert message.message_name in ['ping_req_ack']
        # print "%s can reach %s (whereas we=%s cannot) " % (
        #     reachable_from_member_id,
        #     member_id,
        #     self.member_id
        # )
        alive_member = self._remote_member_from_id(member_id)
        if alive_member is None:
            # Could be a message sent by recently added or removed
            # node; log & ignore.
            print "Warning: %s got member_indirectly_reachable for "\
            "unknown member '%s' thanks to '%s' : %s" % (
                self.member_id,
                alive_member,
                reachable_from_member_id,
                message
            )
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
        """Start checking the remote node for failure"""
        self.state = "ping_sent"
        self.owner.send_ping()

    def on_tick(self, time_now):
        """Time is marching on - do we need to time any operations out?
        (time_now should be some sort of monotonic time)"""
        # print "FDT %s on_tick start_time=%s time_now=%s state=%s" % (
        # self.remote_member_id,
        # self.start_time,
        # time_now,
        # self.state
        # )
        if self.state == "ping_sent":
            if time_now > self.start_time + self.response_timeout:
                # Direct ping has failed; let's try indirect ping (ping_req)
                self.state = "ping_req_sent"
                self.owner.send_ping_reqs()
        elif self.state == "ping_req_sent":
            if time_now > self.start_time + (self.response_timeout * 2):
                # print "FailureDetectionTransaction not heard back "\
                #      "regarding %s; assuming failure" % self.remote_member_id
                self.state = "failure_detected"
                self.owner.node_failed()

    def on_ack(self):
        """We pinged a node ourselves and it responded directly to us"""
        self.state = "alive"
        self.owner.node_alive()

    def on_ping_req_ack(self):
        """We have received a ping_req_ack - so we know that a node we
        sent a ping_req to managed, on our behalf, to ping the node
        we're checking."""
        self.state = "alive"
        self.owner.node_alive()


class RemoteMember(object):
    """  Represents a remote member of the distributed process group;
    handles  messages communicated between this node and the remote
    member and maintains 'state' about the remote member's liveness. """
    def __init__(self, remote_member_id):
        """
        """
        self.remote_member_id = remote_member_id
        self.incarnation_number = 0
        self.state = "unknown"
        self.failure_detection_transaction = None
        self.membership = None

    def __str__(self):
        return 'RemoteMember(remote_member_id=%s, state=%s)' % (
            self.remote_member_id, self.state
        )

    def start(self, membership):
        """Prepare to become operational"""
        self.membership = membership

    def on_tick(self, time_now):
        """Time is marching forwards - do we need to time-out any
        operations etc.?"""
        if self.failure_detection_transaction is not None:
            self.failure_detection_transaction.on_tick(time_now)
        else:
            # We're not in the midst of a ping transaction; we
            # need do nothing.
            pass

    def is_currently_being_checked(self):
        """Are we already in the middle of checking the liveness
        of this node?"""
        return self.failure_detection_transaction is not None

    def begin_checking_for_failure(self, time_now):
        """Let's check the liveness of this node"""
        assert self.membership is not None
        assert self.failure_detection_transaction is None
        self.failure_detection_transaction = FailureDetectionTransaction(
            time_now,
            self,
            self.remote_member_id
        )
        self.failure_detection_transaction.start()

    def node_alive(self):
        """This node appears to be alive"""
        self.state = "alive"
        self.failure_detection_transaction = None

    def node_failed(self):
        """This node appears to be failed/unreachable"""
        self.state = "dead"
        self.failure_detection_transaction = None

    def send_ping(self):
        """ Send a ping (along with some piggyback data) to the remote
        member tracked by this object """
        ping_msg = swimmsg.ping()
        self.membership.send_message_to_member_id(
            ping_msg,
            self.remote_member_id
        )

    def send_ping_reqs(self):
        """Our owner wishes to request that this remote member sends
        a ping (along with some piggyback data)to another
        remote member"""
        ping_req_msg = swimmsg.ping_req(
            self.membership.member_id,
            self.remote_member_id
        )
        members_to_ping_req = self.membership.select_nodes_to_ping_req(
            self.remote_member_id
        )
        for member in members_to_ping_req:
            self.membership.send_message_to_member_id(
                ping_req_msg,
                member.remote_member_id
            )

    def handle_incoming_message(self, message):
        """We've received a message from the wire"""
        # FUTURE: if we are "dead" should we treat receipt of a
        # message from a node as meaning that node is alive again?
        if (self.failure_detection_transaction is not None and
                message.message_name in ['ack', 'ping_req_ack']):
            if message.message_name == 'ack':
                self.failure_detection_transaction.on_ack()
            elif message.message_name == 'ping_req_ack':
                self.failure_detection_transaction.on_ping_req_ack()
        else:
            if message.message_name == 'ping':
                # Always respond to a ping with an ack
                self.membership.send_message_to_member_id(
                    swimmsg.ack(meta_data=message.meta_data),
                    self.remote_member_id
                )
            elif message.message_name == 'ping_req':
                # On receipt of a ping_req, attempt to ping the node in question
                member_id_to_ping = message.meta_data.get(
                    "member_id_to_ping",
                    None
                )
                self.membership.send_message_to_member_id(
                    swimmsg.ping(meta_data=message.meta_data),
                    member_id_to_ping
                )
            elif message.message_name == 'ack':
                # if this is an ack for a ping sent in response to a
                # ping_req, we need to send a ping_req_ack to the
                # original requester.
                if message.meta_data is not None:
                    requested_by_member_id = message.meta_data.get(
                        "requested_by_member_id",
                        None
                    )
                    member_id_to_ping = message.meta_data.get(
                        "member_id_to_ping",
                        None
                    )
                    if requested_by_member_id and member_id_to_ping:
                        # Send the ping_req_ack to the member to sent
                        # the ping_req
                        self.membership.send_message_to_member_id(
                            swimmsg.ping_req_ack(
                                requested_by_member_id,
                                member_id_to_ping
                            ),
                            requested_by_member_id
                        )
            elif message.message_name == 'ping_req_ack':
                requested_by_member_id = message.meta_data.get(
                    "requested_by_member_id",
                    None
                )
                member_id_to_ping = message.meta_data.get(
                    "member_id_to_ping",
                    None
                )
                if (requested_by_member_id == self.membership.member_id and
                        member_id_to_ping != self.remote_member_id):
                    if member_id_to_ping is not None:
                        # We've got a ping_req_ack that is of interest
                        # to one of our in-process peer objects - bounce
                        # this over, via our owner
                        self.membership.member_indirectly_reachable(
                            member_id_to_ping,
                            self.remote_member_id,
                            message
                        )
