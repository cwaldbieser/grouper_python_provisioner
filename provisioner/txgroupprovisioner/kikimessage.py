
import json
from twisted.internet import defer

ADD_ACTION = "add"
DELETE_ACTION = "delete"
UPDATE_ACTION = "update"


class BaseMsg(object):
    """
    Base message class.
    Implements basic serialization by serializing all non-callable properties
    that don't end in a double underscore.

    Strictly an implementation helper.
    Other message classes don't *need* to inherit from this class.

    Message classes should implement the following protocol:

    * get_groups(group_mapper): Returns a Deferred that fires with a list
      of groups related to the message.  The `group_mapper` argument is an
      object that implements the IGroupMapper interface.  It is intended to
      be used if the message contains a subject but no explicit groups.
    * resolve_attributes(resolver): If this message will be routed to targets
      that require attributes, this method will be called to allow the
      message to resolve attributes for any subject(s) it contains.  The
      `resolver` object implements IAttributeResolver.  This method should
      return a Deferred that fires when all attribute resolution is
      complete.
    * serialize(): Returns a string representation of the message, suitable
      for sending to an exchange and ultimately delivered to a downstream
      provisioner. 
    """

    def serialize(self):
        o = {}
        for prop in dir(self):
            if not prop.endswith("__"):
                value = getattr(self, prop)
                if not hasattr(value, '__call__'):
                    o[prop] = getattr(self, prop)
        return json.dumps(o)

    def get_groups(self, group_mapper):
        """
        Returns a Deferred that fires with a list of groups related to this
        message.
        """
        return defer.fail(NotImplementedError(
            "Override `get_groups()` in subclass {0}.".format(
                self.__class__.__name__)))

    def resolve_attributes(self, resolver):
        """
        Returns a Deferred that fires when attributes have been resolved
        for this message.  The way the attributes are stored in the message
        and serialized later is up to the message class.  The resulting
        message should be understood by downstream provisioners.
        """
        return defer.fail(NotImplementedError(
            "Override `resolve_attributes()` in subclass {0}.".format(
                self.__class__.__name__)))

class MembershipChangeMsg(BaseMsg):
    """
    A message that describes a membership change (either an add or a delete)
    for a group and a single subject.  Attributes may be populated if any 
    provisioning targets are determined to require them.
    """

    def __init__(self, action, group, subject):
        self.action = action
        self.group = group
        self.subject = subject
        self.attributes = {}

    def get_groups(self, group_mapper):
        """
        Returns a Deferred that fires with a list of groups related to this
        message.
        """
        return defer.succeed([self.group])    

    @defer.inlineCallbacks
    def resolve_attributes(self, resolver):
        """
        Returns a Deferred that fires when attributes have been resolved
        for this message.  The way the attributes are stored in the message
        and serialized later is up to the message class.  The resulting
        message should be understood by downstream provisioners.
        """
        attributes = yield resolver.resolve_attributes(self.subject)
        self.attributes.update(attributes)
        

class SubjectChangedMsg(BaseMsg):
    """
    A notification that one or more attrbutes for a subject have changed.
    """
    action = UPDATE_ACTION

    def __init__(self, subject):
        self.subject = subject
        self.attributes = {}

    def get_groups(self, group_mapper):
        """
        Returns a Deferred that fires with a list of groups related to this
        message.
        """
        return group_mapper.get_groups_for_subject(self.subject)

    @defer.inlineCallbacks
    def resolve_attributes(self, resolver):
        """
        Returns a Deferred that fires when attributes have been resolved
        for this message.  The way the attributes are stored in the message
        and serialized later is up to the message class.  The resulting
        message should be understood by downstream provisioners.
        """
        attributes = yield resolver.resolve_attributes(self.subject)
        self.attributes.update(attributes)


class BasicFullSyncMsg(BaseMsg):
    """
    A request that any receiving provisioners examine the complete membership
    list for a group and perform whatever changes are necessary to mirror that
    membership in their provisioning targets.
    """

    def __init__(self, group, subjects):
        self.group = group
        self.subjects = subjects

    def get_groups(self, group_mapper):
        """
        Returns a Deferred that fires with a list of groups related to this
        message.
        """
        return defer.succeed([self.group])    


    def resolve_attributes(self, resolver):
        """
        Returns a Deferred that fires when attributes have been resolved
        for this message.  The way the attributes are stored in the message
        and serialized later is up to the message class.  The resulting
        message should be understood by downstream provisioners.
        """
        msg = (
            "The `BasicFullSyncMsg` message format is not capable of "
            "providing fully resolved subjects to downstream provisioners.  "
            "It is intended to providing complete subject ID memberships to "
            "downstream membership provisioners for a single group."
        )
        return defer.fail(Exception(msg))



