
from collections import namedtuple

ADD_ACTION = "add"
DELETE_ACTION = "delete"
UPDATE_ACTION = "update"


Instructions = namedtuple(
    'Instructions',
    [
        'subject', 
        'action',
        'requires_attributes',
        'attributes',
    ])

