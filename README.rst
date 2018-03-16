==============
pychangelogger
==============

This Grouper changelog consumer is a Jython script that sends Grouper
changelog events of interest to an AMQP exchange.  The routing key
attached to each event is determined by a configuration based on the
location of the group within the Grouper hierarchy.

------------
Installation
------------

A working `jython` interpreter (v2.7) is required to run the script.

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

* `/etc/pychangelogger/process_changelog.cfg`
* `~/.process_changelog.cfg`

Settings in the user config override the settings in the system wide config.

The main configuration has the following sections and options:

* *APPLICATION*
    * `changefile`: Path to the file that contains the last processed sequence 
      number form the changelog.
* *AMQP*
    * `host`: Host where the exchange resides.
    * `port`: Port on which the exchange is listening.
    * `vhost`: Virtual host where the exchange is located.
    * `user`: User to connect to the exchange as.
    * `password`: Password to connect to the exchange with.
    * `exchange`: The name of the exchange.
    * `keystore`: (Optional) path to the trust store containing trusted CA certificates.
    * `keystore_passphrase`: (Optional) passphrase used to access the trust store.
    * `route_key`: (Optional) the routing key to use when delivering AMQP
      messages to the exchange (default 'kiki.grouper').

Example config file, `/etc/pychangelogger/process_changelog.cfg`:

.. code:: ini

    [APPLICATION]
    changefile = /var/run/pychangelogger/last_change_id.txt
    routemap = /etc/pychangelogger/routemap.cfg

    [AMQP]
    host = localhost
    port = 5671
    #;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    # For TLS, set `port` to a TLS port on the exchange.
    # A TLS connection requires the following options:
    # # The trust store (The CA certs you trust).
    # keystore = /path/to/keystore.jks
    # # The passphrase used to access the trust store.
    # keystore_passphrase = password_for_the_keystore
    #;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    keystore = /etc/grouper/ssl/ca/changelogger_truststore.jks
    keystore_passphrase = KEYSTORE-PASSWORD
    vhost = /
    user = AMQP-SERVICE-ACCOUNT
    password = AMQP-EXCHANGE-PASSWD
    exchange = EXCHANGE-NAME

-----------------------------------
Connecting to the exchange with TLS
-----------------------------------
In order to ensure authenticity, integrity, and confidentiality of the data
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

