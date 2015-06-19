==========================
Grouper Changelog Consumer
==========================

This Grouper changelog consumer is a Jython script that sends Grouper
changelog events of interest to an AMQP exchange.  The routing key
attached to each event is determined by a configuration based on the
location of the group within the Grouper hierarchy.

-----
Notes
-----

The `RABBITMQ_JAR` environment variable in `changelogger.sh` needs to be set to the
location of the rabbitmq Java client JAR.

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
a plain TCP connection, an error will occur (hopefully memntioning something along the
lines of "Maybe a non-SSL connection?").

