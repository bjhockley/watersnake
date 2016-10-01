""" Implementation of messages used for a protocol based on
the SWIM protocol described in
http://www.cs.cornell.edu/~asdas/research/dsn02-SWIM.pdf ("the paper") """
# Disable 'Too few public methods'                   pylint: disable=R0903
# Disable 'has no member'                            pylint: disable=E1101

import cjson

class SWIMMessage(object):
    """Class for representing SWIM messages"""
    MESSAGE_NAMES = [u'ping', u'ack', u'ping_req', u'ping_req_ack', u'test']
    def __init__(self, message_name, meta_data=None, piggyback_data=None):
        assert message_name in SWIMMessage.MESSAGE_NAMES, (
            'Invalid message name: %s not in %s' % (message_name,
                                                    SWIMMessage.MESSAGE_NAMES)
        )
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
        return (self.equals_ignoring_piggyback_data(other) and
                self.piggyback_data == other.piggyback_data)

    def equals_ignoring_piggyback_data(self, other):
        """Is this message the  as 'other', if we ignore piggyback data? """
        return (isinstance(other, SWIMMessage) and
                self.message_name == other.message_name and
                self.meta_data == other.meta_data)


class SWIMDeserialisationException(Exception):
    """Exception class raised when a SWIM message cannot be constructed from
    a buffer received from the wire"""
    pass


class SWIMJSONMessageSerialiser(object):
    """A class capable of serialising / deserialising SWIMMessages using
    the JSON format"""
    @staticmethod
    def to_buffer(swim_message):
        """Serialises the swim_message object to a form suitable for sending
        on the wire"""
        message_as_dict = {
            u"message_name" : swim_message.message_name,
            u"meta_data" : swim_message.meta_data,
            u"piggyback_data" : swim_message.piggyback_data
        }
        return cjson.encode(message_as_dict)

    @staticmethod
    def from_buffer(buff):
        """Parses buffer and deserialises the swim_message object and returns
        it if possible;  raises a SWIMDeserialisationException otherwise."""
        try:
            message_as_dict = cjson.decode(buff)
            return SWIMMessage(message_name=message_as_dict["message_name"],
                               meta_data=message_as_dict["meta_data"],
                               piggyback_data=message_as_dict["piggyback_data"])
        except Exception as _:
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


def ping_req(
        requested_by_member_id,
        member_id_to_ping,
        piggyback_data=None
    ):
    """ Factory function to create a ping_req SWIM message """
    meta_data = {"requested_by_member_id" : requested_by_member_id,
                 "member_id_to_ping" : member_id_to_ping}
    return SWIMMessage(message_name="ping_req",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)

def ping_req_ack(
        requested_by_member_id,
        member_id_to_ping,
        piggyback_data=None
    ):
    """ Factory function to create a ping_req_ack SWIM message """
    meta_data = {"requested_by_member_id" : requested_by_member_id,
                 "member_id_to_ping" : member_id_to_ping}
    return SWIMMessage(message_name="ping_req_ack",
                       meta_data=meta_data,
                       piggyback_data=piggyback_data)
