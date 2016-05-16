
from collections import namedtuple
import json

ADD_ACTION = "add"
DELETE_ACTION = "delete"
UPDATE_ACTION = "update"
MEMBSYNC_ACTION = "memb_sync"


Instructions = namedtuple(
    'Instructions',
    [
        'action',
        'group',
        'subject', 
        'attributes',
    ])


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
    msg_type = "membership_change"

    def __init__(self, action, group, subject):
        self.action = action
        self.group = group
        self.subject = subject
        self.attributes = {}


class SubjectChangedMsg(BaseMsg):
    msg_type = "subject_change"
    action = UPDATE_ACTION

    def __init__(self, subject):
        self.subject = subject
        self.attributes = {}


class FullSyncMsg(BaseMsg):
    msg_type = "full_sync"

    def __init__(self, group):
        self.group = group
        self.subjects = []

