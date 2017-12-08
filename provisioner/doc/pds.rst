
============================
Provisioner Delivery Service
============================

The kiki provisioner is actually a :term:`PDS` or a 
"provisioner-provisioner".  It accepts a provisioning message from a source,
possibly looks up some attributes related to the subject, packages the results
in a new message, and sends the new message to an exchange with a new routing
key determined from the old message.

To use this provisioner, set the *provisioner* option under the 
*APPLICATION* section to "kiki".

This provisioner supports the following options in the `PROVISIONER` section:
* *attrib_resolver* (required) - The tag that identifies an attribute
  resolver that will fetch the attributes for a given subject.
* *parser_map* (required) - A configuration file that maps received
  messages to a particular type of message parser (see below).

""""""""""""""""""""""""""""
Provisioning Message Parsing
""""""""""""""""""""""""""""

Because provisioning messages can come from different sources, their message
formats may be wildly different.  The Kiki service determines how to parse
the message into a format it can use based on the characteristics of the
received message.  In practice, this means that the exchange and routing key
used to deliver the message can be mapped to a specific parsing strategy.

Parsers include:

* PyChangeLoggerParser
* SubjectAttributeUpdateParser

Mapping is controlled via the *parser_map* option set in the 
*PROVISIONER* section.  This option should point to a JSON file that
specifies a sequence of exchange and routing key patterns that map to
a specific strategy.  For example::

    [
        {
            "exchange": "test_exchange",
            "route_key": "kiki[.]grouper",
            "parser": "pychangelogger_parser"
        },
        {
            "exchange": "test_exchange",
            "route_key": "kiki[.]entity_change_notifier",
            "parser": "subject_parser"
        }
    ]

The `exchange` and `route_key` keys of each stanza are regular expressions
that must match the actual exchange and route key in order to select that
parser.  The stanzas are tried in order, and the first match is selected.
If no stanzas match, the message will not be parsed, and the message will
be re-queued.

"""""""""""""
Group Mapping
"""""""""""""

Some messages do not include group information.  For example, an entity change
notification system may only indicate that the attributes of a subject have
changed, and it is up to the provisioner delivery system to determine the
groups to which the subject belongs.  This is important for routing to
account provisioner targets (see Routing_ below).

The type of group mapper used is selected by setting the *group_mapper*
option in the *PROVISIONER* section.

;;;;;;;;;;;;;;;;;
Null Group Mapper
;;;;;;;;;;;;;;;;;

The null group mapper is selected with the value `null_group_mapper`.  It maps
subjects to an empty set of groups.  Such messages are discarded by the
provisioner delivery service.  In effect, it only allows the processing of
messages that include group information.

;;;;;;;;;;;;;;;;;;
RDBMS Group Mapper
;;;;;;;;;;;;;;;;;;

The RDBMS group mapper is selected with the value `rdbms_group_mapper`.  It
queries a relational database for the groups which the subject is a member.
The following options may be supplied in the `RDBMS Group Mapper` section:

* *query* (required) - A SQL query that returns rows with a single
  column which is a group to which the subject belongs.
  The query should take a single parameter, which is the subject.
* *driver* (required) - The name of the DBAPI2 driver module name that
  will provide the underlying database connection.
* *named_param* (optional) - Some DB drivers require that parameters be
  provided as mapped keywords rather than positional arguments.  If this is
  the case, this option specifies the key mapped to the subject value.

All other options will be passed directly to the database driver (e.g. `host`
and `port` for a MySQL connection, and `database` for an sqlite3 connection,
etc.).


"""""""
Routing
"""""""

Routing is the process by which the provisioner delivery service decides which
routing keys to apply to a message before delivering it to a target exchange.
A particular router is specified with the *router* option in the 
`PROVISIONER` section.

;;;;;;;;;;;
JSON Router
;;;;;;;;;;;

The JSON router is selected by specifying the `json_router` value.
The `JSON Router` section should contain the option *json_file*
which is a JSON document that describes routes to try in order when 
attempting to match an input message.  The route map format is as
follows::

        [
            {
                "name": "Description for the first route.",
                "stem": "full:path:to:a:stem",
                "recursive": false,
                "include_attributes": false,
                "route_key": "route_key_A"
            },
            {
                "name": "Description for the second route.",
                "group": "full:path:to:a:group",
                "include_attributes": false,
                "route_key": "route_key_B"
            },
            {
                "name": "Description for the third route.",
                "stem": "lc:app:orgsync:exports",
                "include_attributes": true,
                "route_key": "orgsync"
            },
            {
                "name": "Default",
                "group": "*",
                "discard": true
            }
        ]

Each entry is a route that is tested against the group included in a parsed
message or the groups mapped to a subject for messages that have no group.
In the latter case, each group may match a separate route.  In this case, the
route key for the exchange will have multiple fields, one for each route
matched.  The final route key is used when delivering the message to a topic
exchange.

A `stem` match will match all child groups of a stem.  If the `recursive` key
is set to true, all descendants of the stem will match.

In contrast, a `group` match will match only an exact group.  The exception to
this rule is that if the value is '*', then any group will match.  This is
useful for creating default routes.

If a route entry may include the `include attributes` key.  If set to true, the
provisioner delivery service will attempt to look up attributes for the 
subject and include them in the message it delivers.

All route entries must include either a `route_key` or a `discard` key with a
value of true.  If `discard` is set, the group being examined will be dropped
from consideration when forming the final routing key.  Any routing keys
matched will be used as fields of the final routing key.

For example, if 3 groups match 3 routes with route keys 'frobnitz', 'xyzzy',
and 'wumpus', the final routing key will be 'frobnitz.xyzzy.wumpus'.

"""""""""""""""""""
Attribute Resolvers
"""""""""""""""""""

;;;;;;;;;;;;;;;;;;;;;;;;
RDBMS Attribute Resolver
;;;;;;;;;;;;;;;;;;;;;;;;

The RDBMS attribute resolver looks up attributes from a RDBMS using drivers
provided by the standard DBAPI2 interface.  This resolver expects to find
its configuration options located under the `RDBMS Attribute Resolver` section
of the main provisioner configuration file.  The options are as follows:

* *query* (required) - A SQL query that returns rows of attribute
  name-value pairs.  Multi-valued attributes will have a row for each value.
  The query should take a single parameter, which is the subject.
* *driver* (required) - The name of the DBAPI2 driver module name that
  will provide the underlying database connection.
* *named_param* (optional) - Some DB drivers require that parameters be
  provided as mapped keywords rather than positional arguments.  If this is
  the case, this option specifies the key mapped to the subject value.

All other options will be passed directly to the database driver (e.g. `host`
and `port` for a MySQL connection, and `database` for an sqlite3 connection,
etc.).

""""""""""""""""
Message Delivery
""""""""""""""""

Messages delivered to target provisioners are JSON documents that contain 
`subject` and `action` keys, and optionally `group` and `attributes` keys.
The Routing_ configuration should take care to make sure that messages 
from entity change sources are delivered to account provisioning targets.
Likewise, messages from sources that describe group membership changes
should be routed to membership provisioning targets.

A `group` key will appear in a delivered message only if the parsed input
includes a group.  An `attributes` key will only appear in an output message
if the matched route indicates that attributes are required.

