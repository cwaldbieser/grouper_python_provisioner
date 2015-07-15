==========================
Grouper Changelog Consumer
==========================

This Grouper changelog consumer is a Jython script that sends Grouper
changelog events of interest to an AMQP exchange.  The routing key
attached to each event is determined by a configuration based on the
location of the group within the Grouper hierarchy.

------------
Installation
------------

A working `jython` interpreter (v2.5.3+) is required to run the script.

The `RABBITMQ_JAR` environment variable in `changelogger.sh` needs to be set to the
location of the rabbitmq Java client JAR.

------------------------------
Running the Changelog Consumer
------------------------------

Run the changelog consumer::

    $ ./changelogger.sh

.. note::

    The changelogger does not daemonize itself.  If you want to run it as a service, use
    a tool like `supervisor`.

----------------------
Configuring the Script
----------------------

The changelogger looks for its configuration file(s) in the following places:

* `/etc/grouper/process_changelog.cfg`
* `~/.process_changelog.cfg`
* `./process_changelog.cfg`

Settings in the user config override the settings in the system wide config.
Likewise, settings in the local config override settings in the user config.

The main configuration has the following sections and options:

* *APPLICATION*
    * `changefile`: Path to the file that contains the last processed sequence 
      number form the changelog.
    * `routemap`: Path to a route map file (see below).
* *AMQP*
    * `host`: Host where the exchange resides.
    * `port`: Port on which the exchange is listening.
    * `vhost`: Virtual host where the exchange is located.
    * `user`: User to connect to the exchange as.
    * `password`: Password to connect to the exchange with.
    * `exchange`: The name of the exchange.
    * `keystore`: (Optional) path to the trust store containing trusted CA certificates.
    * `keystore_passphrase`: (Optional) passphrase used to access the trust store.

'''''''''''''
The Route Map
'''''''''''''

The `routemap` option of the main configuration points to a file that describes 
mappings from stems (folders) or groups to routing keys.  The changelog consumer 
connects to the exchange to deliver a message containing a group membership 
change.  A route key is provided with the message which will be used by the 
exchange to determine to which queue(s) the message should be delivered (if any).

Each line of the route map file contains a path (a stem or group) followed by 
"=" and a route key.  Groups are represented by thier fully qualified Grouper 
paths.  A stem is represented by the Grouper path to the stem, followed by   
a colon and an asterisk.  This indicates that any path that falls under this
stem will be mapped to the indicated route key unless another more specific 
path overrides it.

There is also a special path, `_default` that will be used to assign a route
key if no path in the file matches the group path.

Typical usage would be to create a stem in Grouper under which groups that are 
to be exported to a particular provisioning agent reside.  A stem entry in the 
route map will assign a unique route key to these groups.  The exchange will
presumably be configured to map the route keys to the appropriate provisioning
agent(s).

-----------------------------------
Connecting to the exchange with TLS
-----------------------------------
In order to ensure identity, integrity, and confidentiality of the data
being placed in the exchange, the changelog consumer can connect to the
exchange using TLS.  To do this, a Trust Store (a Java keystore that holds
CA certs you trust) must used.

If you have the CA certs in PEM format (typical for Apache/Nginx and OpenSSL),
you can create a trust store using Java's `keytool`::

    $ keytool -import -alias $FRIENDLY_NAME -file /path/to/cacert.pem -keystore /path/to/trust_store.jks

The trust store will have a passphrase that is required to access it.  The path to
the trust store and the passphrase must be included in the changelog consumer's configuration
in order to use TLS.

Also make sure that the port you specify on the exchange host is using TLS.  If it is
a plain TCP connection, an error will occur (hopefully mentioning something along the
lines of "Maybe a non-SSL connection?").

