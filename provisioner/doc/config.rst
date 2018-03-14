
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
    * **provisioner**: The tag used to identify the provisioner back end to use.  Possible backends include:
        * *kiki*: The :term:`PDS` provisioner.
        * *ldap*: The LDAP provisioner backend
* *AMQP*
    * **log_level**
    * **endpoint**: Client endpoint connection string.
      See https://twistedmatrix.com/documents/current/core/howto/endpoints.html#clients
      Examples:
      * tcp:host=localhost:port=5672
      * tls:host=grouper.example.edu:port=5671:trustRoots=/etc/grouper/ssl/ca:endpoint=tcp\:localhost\:5671
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

