
import json

ADD_ACTION = "add"
DELETE_ACTION = "delete"
UPDATE_ACTION = "update"
MEMBSYNC_ACTION = "memb_sync"


class BaseMsg(object):
    """
    Base message class.
    Implements basic serialization by serializing all non-callable properties
    that don't end in a double underscore.
    Strictly an implementation helper.
    Other message classes don't *need* to inherit from this class.
    """

    def serialize(self):
        o = {}
        for prop in dir(self):
            if not prop.endswith("__"):
                value = getattr(self, prop)
                if not hasattr(value, '__call__'):
                    o[prop] = getattr(self, prop)
        return json.dumps(o)


class MembershipChangeMsg(BaseMsg):
    """
    A message that describes a membership change (either an add or a delete)
    for a group and a single subject.  Attributes may be populated if any 
    provisioning targets are determined to require them.
    """
    msg_type = "membership_change"

    def __init__(self, action, group, subject):
        self.action = action
        self.group = group
        self.subject = subject
        self.attributes = {}


class SubjectChangedMsg(BaseMsg):
    """
    A notification that one or more attrbutes for a subject have changed.
    """
    msg_type = "subject_change"
    action = UPDATE_ACTION

    def __init__(self, subject):
        self.subject = subject
        self.attributes = {}


class FullSyncMsg(BaseMsg):
    """
    A request that any receiving provisioners examine the complete membership
    list for a group and perform whatever changes are necessary to mirror that
    membership in their provisioning targets.
    """
    msg_type = "full_sync"

    def __init__(self, group):
        self.group = group
        self.subjects = []


