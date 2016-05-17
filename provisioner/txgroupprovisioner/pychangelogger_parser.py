
from twisted.plugin import IPlugin
from zope.interface import implements
from kikimessage import (
    ADD_ACTION,
    DELETE_ACTION,
    MembershipChangeMsg,
)
from txgroupprovisioner.interface import (
    IMessageParserFactory, 
    IMessageParser,
)


class PyChangeloggerMessageParserFactory(object):
    implements(IPlugin, IMessageParserFactory)
    tag = "pychangelogger_parser"

    def generate_message_parser(self, **kwds):
        return PyChangeloggerMessageParser(**kwds)


class PyChangeloggerMessageParser(object):
    implements(IPlugin, IMessageParser)

    def parse_message(self, msg):
        """
        Parse an AMQP message into instructions for creating a
        downstream provisioner message.
        """
        body = msg.content.body
        parts = body.split("\n")
        group = parts[0]
        subject = parts[1]
        action = parts[2]
        if action.startswith("delete"):
            action = DELETE_ACTION
        else:
            action = ADD_ACTION
        parsed = MembershipChangeMsg(
            action,
            group,
            subject)
        return parsed

