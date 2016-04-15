
from twisted.plugin import IPlugin
from zope.interface import implements
from kikimessage import Instructions
from txgroupprovisioner.interface import (
    IMessageParserFactory, 
    IMessageParser,
)


class PyChangeloggerMessageParserFactory(object):
    implements(IPlugin, IMessageParserFactory)

    def generate_message_parser(**kwds):
        return PyChangeloggerMessageParser(**kwds)


class PyChangeloggerMessageParser(object):
    implements(IPlugin, IMessageParser)

    def parse_message(self, msg):
        """
        Parse an AMQP message into instructions for creating a
        downstream privisioner message.
        """
        body = msg.content.body
        parts = body.split("\n")
        group = parts[0]
        subject = parts[1]
        action = parts[2]
        if action.startswith("delete"):
            requires_attributes = False
            attributes = None
        else:
            requires_attributes = True
            attributes = {}
        instructions = Instructions(
            subject,
            action,
            requires_attributes,
            attributes)
        return instructions

