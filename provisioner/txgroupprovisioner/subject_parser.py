
from twisted.plugin import IPlugin
from zope.interface import implements
from kikimessage import (
    SubjectChangeMsg,
)
from txgroupprovisioner.interface import (
    IMessageParserFactory, 
    IMessageParser,
)


class SubjectMessageParserFactory(object):
    implements(IPlugin, IMessageParserFactory)
    tag = "subject_parser"

    def generate_message_parser(self, **kwds):
        return SubjectMessageParser(**kwds)


class SubjectMessageParser(object):
    implements(IPlugin, IMessageParser)

    def parse_message(self, msg):
        """
        Parse an AMQP message into instructions for creating a
        downstream provisioner message.
        """
        body = msg.content.body
        subject = body.strip("\n")
        parsed = SubjectChangedMsg(subject)
        return parsed

