##################
txgroupprovisioner
##################

.. highlight:: console

========
Overview
========

The Twisted Group Provisioner (*txgroupprovisioner*) reads group membership
messages from an AMQP message queue and passes them to a back end provisioner
to be acted upon.

There are 2 broad kinds of provisioners.  Membership provisioners reflect group
memberships in their targets.  An example of this kind of provisioner is the 
LDAPProvisioner back end.  Account provisioners create, modify, and remove
accounts in their targets based on the messages they receive.

The Provisioner Delivery Service (Kiki) is a special kind of provisioner that
can accept messages from different kinds of sources, perfrom group and
attribute lookups, compose standard messages, and route them to the intended
provisioners.

The general architecture for this provisioner system looks like a pipline
that flows from event sources to a provisioner delivery service and finally to
the provisioners.  There may be multiple pipelines.  For example, there may be
separate pipelines for membership provisioners and account provisioners.

============
Installation
============
The Python requirements are listed in :file:`requirements.txt`.  The variaous shell scripts
assume that you have installed these in a Python virtual environment (ala `virtualenv`) in
the same folder called :file:`pyenv`.

Example::

    $ virtualenv pyenv
    $ . ./pyenv/bin/activate
    (pyenv)$ pip install -r ./requirements.txt


OS dependencies may vary slightly across distributions:

* python-devel
* libffi-devel

=============
Configuration
=============

The main configuration files are :file:`/etc/grouper/txgroupprovisioner.cfg`, 
:file:`$HOME/.txgroupprovisioner.cfg`, and :file:`./txgroupprovisioner.cfg`.  
Local file settings override user homedir settings, which override system 
settings.  A specific file may be specified on the command line which overrides
all other settings.

The configuration sections and options are below.

* *APPLICATION*
    * **log_level**: Set the global log level for the service.  May be overriden
      in other sections.
    * **provisioner**: The tag used to identify the provisioner back end to use.
      Possible backends include:
        * *ldap*(default): The LDAP provisioner backend
* *AMQP*
    * **log_level**
    * **endpoint**: Client endpoint connection string.
      See https://twistedmatrix.com/documents/current/core/howto/endpoints.html#clients
      Examples:
        * tcp:host=localhost:port=5672
        * ssl:host=localhost:port=5672:caCertsDir=/etc/ssl/certs
    * **vhost**: The virtual host on the AMQP host to which this service connects.
    * **user**: The username credential used to access the AMQP service.
    * **passwd**: The password credential used to access the AMQP service.
    * **queue**: The queue from which the provisioner reads messages.
* *SSH*
    * **endpoint**: Service endpoint string for SSH admin service.  E.g. `tcp:2023`.
    * **group**: Local group an account must belong to in order to access
      the administrative interface.
* *PROVISIONER* (see below)

=====================
Provisioner Back Ends
=====================

The selected provisioner is configured under the *PROVISIONER* section.
The options vary depending on the provisioner.

----------------
Kiki Provisioner
----------------

The Kiki provisioner is actually a provisioner delivery service or a 
"provisioner-provisioner".  It accepts a provisioning message from a source,
possibly looks up some attributes related to the subject, packages the results
in a new message, and sends the new message to an exchange with a new routing
key determined from the old message.

To use this provisioner, set the :option:`provisioner` option under the 
`APPLICATION` section to "kiki".

This provisioner supports the following options in the `PROVISIONER` section:
* :option:`attrib_resolver` (required) - The tag that identifies an attribute
  resolver that will fetch the attributes for a given subject.
* :option:`parser_map` (required) - A configuration file that maps received
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

Mapping is controlled via the :option:`parser_map` option set in the 
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

The type of group mapper used is selected by setting the :option:`group_mapper`
option in the `PROVISIONER` section.

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

* :option:`query` (required) - A SQL query that returns rows with a single
  column which is a group to which the subject belongs.
  The query should take a single parameter, which is the subject.
* :option:`driver` (required) - The name of the DBAPI2 driver module name that
  will provide the underlying database connection.
* :option:`named_param` (optional) - Some DB drivers require that parameters be
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
A particular router is specified with the :option:`router` option in the 
`PROVISIONER` section.

;;;;;;;;;;;
JSON Router
;;;;;;;;;;;

The JSON router is selected by specifying the `json_router` value.
The `JSON Router` section should contain the option :option:`json_file`
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

* :option:`query` (required) - A SQL query that returns rows of attribute
  name-value pairs.  Multi-valued attributes will have a row for each value.
  The query should take a single parameter, which is the subject.
* :option:`driver` (required) - The name of the DBAPI2 driver module name that
  will provide the underlying database connection.
* :option:`named_param` (optional) - Some DB drivers require that parameters be
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

----------------
LDAP Provisioner
----------------

The LDAPProvisioner back end stores messages in batches and reflects the changes in an LDAP
DIT at regular intervals.  This can minimize writes to LDAP group entries that might require
repeated modification in a short time span.  The LDAP provisioner can be configured to
provision LDAP groups and user entries.  A single provisioner can modify both simultaneously
or only groups or only user entries.  This can be optimal for LDAP DIT implementations that
require user entries and groups to be updated independently of one another (e.g. OpenLDAP).

The options for the LDAP provisioner are:

* **log_level**: This option can override the global log level for events
  logged by this back-end.
* **sqlite_db**: The path to an sqlite3 database file used to store messages
  for batch processing.  If the file does not exist, it will be created.
* **group_map**: This JSON configuration file maps Grouper groups or stems
  to LDAP group names or templates.  If a group message does not match an
  entry in this configuration, it will be ignored.
* **url**: The LDAP service URL.  E.g. `ldaps://127.0.0.1:389`.
* **start_tls**: After connecting to the LDAP service, request StartTLS encryption.
* **base_dn**: The base DN used in searches when looking up group and user entries.
* **bind_dn**: BIND as this DN prior to searching the DIT or modifying its entries.
* **passwd**: Password for the **bind_dn** option.
* **empty_dn**: A DN used to populate a group if it would otherwise be empty.  This
  is useful for LDAP groups with the `groupOfNames` schema, as it is a schema
violation to remove the `member` attribute entirely.  If all members would be removed
from the group, the **empty_dn** value is used instead.  
E.g. `cn=nobody,ou=nowhere,dc=example,dc=org`.

"""""""""""""""""""""""
Group Map Configuration
"""""""""""""""""""""""

The group map is a JSON file that maps fully qualified Grouper group names to
LDAP group identifiers (e.g. a CN).  It can also map a Grouper stem to a template.
Grouper stems are denoted with a trailing colon (':').

Valid targets for a group can be an LDAP group name (string) or an LDAP group
configuration (dictionary) consisting of the following keys:

* **group**: (string) The LDAP group.
* **create_group**: (boolean) Create the group in the DIT if it cannot be found 
  by searching.
* **create_context**: (string) The parent DN under which the group should be 
  created if the **create_group** option is set to `true`.

Alternatively, the inbound membership can be used to provision an attribute by
providing a dictionary with the following keys:

* **attribute**: (string) The LDAP attribute name.
* **value**: (string) The LDAP attribute value to add/remove.
* **multi_valued**: (boolean) A flag that indicates whether this attribute can have
  multiple values.

Valid targets for a stem may be either a template (string) or an LDAP template
configuration (dictionary) consiting of the following keys:

* **template**: (string) A Jinja2 template that will undergo substitutions with
  the following variables:
    * `group`: The base Grouper group name (no stem).
    * `stem`: The stem of the Grouper group.
    * `fqgroup`: The fully qualified Grouper group name (includes stem).
* **create_group**: (boolean) Create the group in the DIT if it cannot be found 
  by searching.
* **create_context**: (string) The parent DN under which the group should be 
  created if the **create_group** option is set to `true`.

=======
Running
=======

A single instance of the provisioner may be invoked as a twisted plugin::

    $ ./twistd.sh --syslog provisioner

Other options for the `provisioner` plugin or the `twistd` program itself
are available.  Try using the `--help` option for more information.

Alternatively, specific configurations for multiple provisioners may be placed
in a `conf.d` folder in the main application folder.  The scripts `provision`
and `stop-provisioning` can be used to collectively start and stop multiple configured
provisioners.

