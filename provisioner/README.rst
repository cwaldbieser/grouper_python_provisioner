##################
txgroupprovisioner
##################

.. highlight:: console

The Twisted Group Provisioner (*txgroupprovisioner*) reads group membership messages from an
AMQP message queue and passes them to a back end provisioner to be acted upon.

The LDAPProvisioner back end stores messages in batches and reflects the changes in an LDAP
DIT at regular intervals.  This can minimize writes to LDAP group entries that might require
repeated modification in a short time span.  The LDAP provisioner can be configured to
provision LDAP groups and user entries.  A single provisioner can modify both simultaneously
or only groups or only user entries.  This can be optimal for LDAP DIT implementations that
require user entries and groups to be updated independently of one another (e.g. OpenLDAP).

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
    * **exchange**: The service configures an exchange that will route
      messages to one or more queues.
    * **vhost**: The virtual host on the AMQP host to which this service connects.
    * **user**: The username credential used to access the AMQP service.
    * **passwd**: The password credential used to access the AMQP service.
    * **queue**: The queue from which the provisioner reads messages.
    * **route_map**: A JSON config file that specifies bindings the exchange uses
      to route messages to queues.
* *PROVISIONER* (see below)

-----------------------
Route Map Configuration
-----------------------

The file :file:`queuemap.json.example` is an example route map configuration.
The file must be a JSON file that maps queues to route keys.  Since a queue
may be mapped to more than one route key, the format is a list of queue to
route key pairs.

Bindings may be declared for queues other than the one from which the service 
reads.  This is useful if multiple provisioner services share a common route map
configuration.

=====================
Provisioner Back Ends
=====================

The selected provisioner is configured under the *PROVISIONER* section.
The options vary depending on the provisioner.

----------------
LDAP Provisioner
----------------

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
Grouper stemps are denoted with a trailing colon (':').

Valid targets for a group can be an LDAP group name (string) or an LDAP group
configuration (dictionary) consisting of the following keys:

* **group**: (string) The LDAP group.
* **create_group**: (boolean) Create the group in the DIT if it cannot be found 
  by searching.
* **create_context**: (string) The parent DN under which the group should be 
  created if the **create_group** option is set to `true`.

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

