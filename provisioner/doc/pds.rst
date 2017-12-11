
============================
Provisioner Delivery Service
============================

The :term:`Provisioner Delivery Service` modifies messages it receives.
It may look up attributes and group mapping for subjects.  It rewrites
messages into a common format.  It then applies routing labels to messages
and delivers then to an exchange. 

To use this provisioner, set the *provisioner* option under the 
*APPLICATION* section to "kiki".

This provisioner supports the following options in the `PROVISIONER` section:

* **attrib_resolver** (required) - The tag that identifies an attribute
  resolver that will fetch the attributes for a given subject.
* **parser_map** (required) - A configuration file that maps received
  messages to a particular type of message parser (see below).

----------------------------
Provisioning Message Parsing
----------------------------

Because provisioning messages can come from different sources, their message
formats may be wildly different.  The Kiki service determines how to parse
the message into a format it can use based on the characteristics of the
received message.  In practice, this means that the exchange and routing key
used to deliver the message can be mapped to a specific parsing strategy.

Parsers include:

* :py:class:`PyChangeLoggerParser`
* :py:class:`SubjectAttributeUpdateParser`

Additional message parsers can be created by creating classes that
implement the :py:class:`IMessageParser` and :py:class:`IMessageParserFactory`
interfaces.

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

When a stanza is selected, the value mapped to the `parser` key will be used
as the tag name for creating the parser.

-------------
Group Mapping
-------------

Some messages do not include group information.  For example, a source that
produces events when an attribute changes on a subject may only indicate that
non-membership attributes of a subject have changed, and it is up to the 
provisioner delivery system to determine which provisioning targets need to be
notified.  Because routing_ logic (see below) is intimately connected to the
groups to which a subject belongs, the :term:`PDS` must be able to query the
source system for these memberships.  

The type of group mapper used is selected by setting the *group_mapper*
option in the *PROVISIONER* section.

;;;;;;;;;;;;;;;;;
Null Group Mapper
;;;;;;;;;;;;;;;;;

The null group mapper is selected with the value `null_group_mapper`.  It maps
subjects to an empty set of groups.  Such messages are discarded by the
provisioner delivery service.  In effect, it only allows the processing of
messages that include group information.  Messages that indicate other kinds of
attribute changes on subjects would be discarded.

;;;;;;;;;;;;;;;;;;
RDBMS Group Mapper
;;;;;;;;;;;;;;;;;;

The RDBMS group mapper is selected with the value `rdbms_group_mapper`.  It
queries a relational database for the groups which the subject is a member.
The following options may be supplied in the `RDBMS Group Mapper` section:

* **query** (required) - A SQL query that returns rows with a single
  column which is a group to which the subject belongs.
  The query should take a single parameter, which is the subject.
* **driver** (required) - The name of the DBAPI2 driver module name that
  will provide the underlying database connection.
* **named_param** (optional) - Some DB drivers require that parameters be
  provided as mapped keywords rather than positional arguments.  If this is
  the case, this option specifies the key mapped to the subject value.

All other options will be passed directly to the database driver (e.g. `host`
and `port` for a MySQL connection, and `database` for an sqlite3 connection,
etc.).

"""""""
Example
"""""""

.. code-block:: ini

    [RDBMS Group Mapper]
    driver = MySQLdb
    query = SELECT A.GROUP_NAME FROM grouper_memberships_v A WHERE A.SUBJECT_ID = ?
    host = mysqlhost.example.net
    port = 3306
    db = grouper
    user = grouper_db_user
    passwd = DB-PASSWORD-GOES-HERE


-------
Routing
-------

Routing is the process by which the :term:`PDS` decides which routing keys to
apply to a message before delivering it to a target exchange.  A particular
router is specified with the *router* option in the `PROVISIONER` section.

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

"""""""
Example
"""""""

.. code-block:: ini

    [JSON Router]
    json_file = /etc/grouper/provisioners/pds/router.json

-------------------
Attribute Resolvers
-------------------

;;;;;;;;;;;;;;;;;;;;;;;
LDAP Attribute Resolver
;;;;;;;;;;;;;;;;;;;;;;;

The LDAP attribute resolver queries attributes for a subject from an LDAP
service.  This resolver reads its configuration from the section
`LDAP Attribute Resolver`.  The options are as follows:

* **endpoint** (required) - A `Twisted endpoint`_ description for a server.
* **base_dn** (required) - The base DN from which to search the LDAP :term:`DIT`.
* **bind_dn** (required) - The DN used to authenticate to the LDAP service.
* **bind_password** (required) - The password usedto authenticate to the LDAP
  service.
* **filter** (required) - The LDAP filter used to select the subject.  This
  filter should be a template using the 
  `Jinja2 <http://jinja.pocoo.org/docs/2.10/>`_ templating syntax.  The filter
  **escape_filter_chars** is available within the template (see the example
  below).
* **start_tls** (required) - May be true ("1", "yes", "true", "on") or false 
  ("0", "no", "false", "off").  If true, the attribute resolver will connect
  to an unencrypted TCP port and later negotiate TLS as part of the LDAP
  protocol *before* BINDing.  If false, the attribute resolver will not
  initiate StartTLS.  In this case, it is *strongly* recommended that the 
  endpoint (see above) be a TLS connection or some other protected endpoint.
* **attributes** (required) - A space-separated list of attributes that will
  be requested for a subject from the LDAP service.

"""""""
Example
"""""""

.. code-block:: ini

    [LDAP Attribute Resolver]
    endpoint = tcp:ldap.example.edu:389
    base_dn = dc=example,dc=net
    bind_dn = cn=attribute-browser,dc=example,dc=net
    bind_passwd = PASSWORD-GOES-HERE
    filter = (uid={{ subject|escape_filter_chars }})
    start_tls = true
    attributes = uid givenName sn mail displayName

;;;;;;;;;;;;;;;;;;;;;;;;
RDBMS Attribute Resolver
;;;;;;;;;;;;;;;;;;;;;;;;

The RDBMS attribute resolver looks up attributes from a RDBMS using drivers
provided by the standard DBAPI2 interface.  This resolver expects to find
its configuration options located under the `RDBMS Attribute Resolver` section
of the main provisioner configuration file.  The options are as follows:

* **query** (required) - A SQL query that returns rows of attribute
  name-value pairs.  Multi-valued attributes will have a row for each value.
  The query should take a single parameter, which is the subject.
* **driver** (required) - The name of the DBAPI2 driver module name that
  will provide the underlying database connection.
* **named_param** (optional) - Some DB drivers require that parameters be
  provided as mapped keywords rather than positional arguments.  If this is
  the case, this option specifies the key mapped to the subject value.

All other options will be passed directly to the database driver (e.g. `host`
and `port` for a MySQL connection, and `database` for an sqlite3 connection,
etc.).

----------------
Message Delivery
----------------

;;;;;;;;;;;;;;;;;;;;;;
AMQP Exchange Delivery
;;;;;;;;;;;;;;;;;;;;;;

Messages delivered to target provisioners are JSON documents that contain 
`subject` and `action` keys, and optionally `group` and `attributes` keys.
The Routing_ configuration should take care to make sure that messages 
that describe attribute changes to subjects are delivered to provisioning targets
that have the capability to update remote accounts.

Likewise, messages from sources that describe membership changes
should be routed to membership provisioning targets.

A `group` key will appear in a delivered message only if the parsed input
includes a group.  An `attributes` key will only appear in an output message
if the matched route indicates that attributes are required.

The :term:`PDS` requires a section used to describe how messages will be
delivered to an AMQP exchange.  The section is called *AMQP_TARGET*, and it
may have the following options:

* **endpoint** (required) - A `Twisted endpoint`_ description for the AMQP service.
* **exchange** (required) - The name of the exchange to which a message will be delivered.
* **vhost** (required) - The virtual host (logical grouping of resources).
* **user** (required) - The AMQP user used to authenticate.
* **passwd** (required) - The AMQP password used to authenticate.

"""""""
Example
"""""""

.. code-block:: ini

    [AMQP_TARGET]
    endpoint = tls:host=broker.example.edu:port=5671:trustRoots=/etc/grouper/tls/ca:endpoint=tcp\:localhost\:5671
    exchange = provisioner_exchange
    vhost = /
    user = amqp_user
    passwd = AMQP-PASSWORD


.. _Twisted endpoint: https://twistedmatrix.com/documents/current/core/howto/endpoints.html#servers

