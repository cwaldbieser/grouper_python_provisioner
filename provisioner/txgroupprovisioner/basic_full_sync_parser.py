
import json
from twisted.plugin import IPlugin
from zope.interface import implements
from kikimessage import (
    BasicFullSyncMsg,
)
from txgroupprovisioner.interface import (
    IMessageParserFactory, 
    IMessageParser,
)


class BasicFullSyncMessageParserFactory(object):
    implements(IPlugin, IMessageParserFactory)
    tag = "basic_full_sync_parser"

    def generate_message_parser(self, **kwds):
        return BasicFullSyncMessageParser(**kwds)


class BasicFullSyncMessageParser(object):
    implements(IPlugin, IMessageParser)

    def parse_message(self, msg):
        """
        Parse an AMQP message into instructions for creating a
        downstream provisioner message.
        """
        serialized = msg.content.body
        doc = json.loads(serialized)
        group = doc["group"]
        subjects = list(doc["subjects"])
        parsed = BasicFullSyncMsg(
            group,
            subjects)
        return parsed

