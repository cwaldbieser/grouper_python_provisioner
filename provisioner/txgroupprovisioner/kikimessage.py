
from collections import namedtuple

ADD_ACTION = "add"
DELETE_ACTION = "delete"
UPDATE_ACTION = "update"


Instructions = namedtuple(
    'Instructions',
    [
        'action',
        'group',
        'subject', 
        'requires_attributes',
        'attributes',
    ])

