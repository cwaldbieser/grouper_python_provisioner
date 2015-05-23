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

