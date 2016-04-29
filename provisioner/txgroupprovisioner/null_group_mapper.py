
from __future__ import print_function
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.plugin import IPlugin
from zope.interface import implements
from interface import (
    IGroupMapperFactory,
    IGroupMapper,
)

class NullGroupMapperFactory(object):
    implements(IPlugin, IGroupMapperFactory)
    tag = "null_group_mapper"

    def generate_group_mapper(self, config_parser):
        """
        Create an object that implements IGroupMapper.
        """
        return NullGroupMapper() 


class NullGroupMapper(object):
    implements(IGroupMapper)
    log = None

    @inlineCallbacks
    def get_groups_for_subject(self, subject):
        """
        Return a Deferred that fires with a list of groups for
        which `subject` is a member.
        """
        returnValue([])        
        
