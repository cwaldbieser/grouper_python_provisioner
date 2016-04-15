
from collections import namedtuple


Instructions = namedtuple(
    'Instructions',
    [
        'subject', 
        'action',
        'requires_attributes',
        'attributes',
    ])
