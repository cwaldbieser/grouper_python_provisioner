
from twisted.plugin import IPlugin
from zope.interface import implements
import constants
from kikimessage import (
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
        group = parts[0].strip()
        subject = parts[1].strip()
        action = parts[2].strip()
        if action.startswith("delete"):
            action = constants.ACTION_DELETE
        else:
            action = constants.ACTION_ADD
        parsed = MembershipChangeMsg(
            action,
            group,
            subject)
        return parsed

