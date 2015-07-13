

# Standard library
import argparse
from ConfigParser import SafeConfigParser
import datetime
import hmac
from operator import itemgetter
import os
import os.path
from textwrap import dedent
import time
import socket
from string import Template
import StringIO
import sys

# Application modules
from jython_grouper import *

# External modules
from edu.internet2.middleware.grouper.misc import GrouperDAOFactory
from edu.internet2.middleware.grouper.changeLog import ChangeLogConsumer
from edu.internet2.middleware.grouper.changeLog import ChangeLogEntry, ChangeLogLabels
from edu.internet2.middleware.grouper.util import GrouperUtil
from edu.internet2.middleware.grouper.app.loader.db import Hib3GrouperLoaderLog
from edu.internet2.middleware.grouper.app.loader import GrouperLoaderStatus
from edu.internet2.middleware.grouper.app.loader import GrouperLoader      
from edu.internet2.middleware.grouper.app.loader import GrouperLoaderType
from com.rabbitmq.client import ConnectionFactory              
from com.rabbitmq.client import Connection       
from com.rabbitmq.client import Channel                        
from com.rabbitmq.client import QueueingConsumer     
from java.io import FileInputStream
from java.lang import String                         
from java.lang import Boolean
from java.security import KeyStore
from javax.net.ssl import SSLContext, TrustManagerFactory
from sortedcollection import SortedCollection

# AMQP functions
def get_amqp_conn(host, port, vhost, user, passwd, ssl_ctx=None):
    factory = ConnectionFactory()
    factory.setHost(host)
    factory.setPort(port)
    factory.setUsername(user)
    factory.setPassword(passwd)
    factory.setVirtualHost(vhost)
    if ssl_ctx is not None:
        factory.useSslProtocol(ssl_ctx)
    conn = factory.newConnection()
    return conn

def send_message(channel, exchange, route_key, msg):
    """
    Send a message to an exchange routed by `route_key`.
    """
    # Send message
    channel.confirmSelect()
    exchange = String(exchange)
    message = String(msg)
    routing_key = String(route_key)
    props = None
    channel.basicPublish(exchange, routing_key, props, message.getBytes())
    channel.waitForConfirmsOrDie()

def send_group_mod(channel, exchange, router, group, action_name, subject_id):
    route_key = router.get_key(group, subject_id, action_name)
    msg = "%s\n%s\n%s" % (group, subject_id, action_name)
    send_message(channel, exchange, route_key, msg)

def load_route_map(fname):
    routes = {}
    f = open(fname, "r")
    for n, line in enumerate(f):
        line = line.strip()
        if line.startswith("#"):
            continue
        if line == "":
            continue
        if not '=' in line:
            warn("Invalid route on line %s." % (n+1))
            warn("route => '%s'" % line)
            continue
        parts = line.split("=", 1)
        key = parts[0].strip()
        value = parts[1].strip()
        routes[key] = value
    return routes


class Router(object):
    def __init__(self, route_map):
        self.direct_mappings = {}
        self.patterns = SortedCollection(key=itemgetter(0))
        self.default_key = ''
        for k, v in route_map.iteritems():
            if k == '_default':
                self.default_key = v
            elif k.endswith('*'):
                self.patterns.insert((k[:-1], Template(v)))
            else:
                self.direct_mappings[k] = v
    
    def get_key(self, group, subject_id, action_name):
        route_key = self.direct_mappings.get(group, None)
        if route_key is not None:
            return route_key
        try:
            result = self.patterns.find_le(group)
        except ValueError:
            return self.default_key
        pattern, route_key = result
        if not group.startswith(pattern):
            return self.default_key
        parts = group.split(':')
        grp = parts[-1]
        stem = ':'.join(parts[:-1])
        rk = route_key.substitute(stem=stem, group=grp, subject=subject_id, action=action_name)
        return rk
            

# Logging functions
def info(msg):
    sys.stderr.write("[INFO] %s\n" % msg)

def debug(msg):
    sys.stderr.write("[DEBUG] %s\n" % msg)

def warn(msg):
    sys.stderr.write("[WARNING] %s\n" % msg)

# Changelogger functions
def get_last_sequence(changefile):
    if os.path.exists(changefile):
        try:
            f = open(changefile, "r")
            data = f.read().strip()
            f.close()
            pos = long(data)
            return pos
        except (Exception,), ex:
            warn("Could not get last sequence number.\n%s\n" % str(ex))
            return None
    return None
        
def update_last_sequence(changefile, n):
    f = open(changefile, "w")
    f.write(str(n))
    f.close()

def load_config(config_name):
    defaults = dedent("""\
        [APPLICATION]
        changefile = %s
        routemap = %s

        [AMQP]
        host = locahost
        port = 5672
        vhost = /
        user = guest
        password = guest
        exchage = grouper_exchange
        """) % (
            os.path.join(os.curdir, "last_change_id.txt"),
            os.path.join(os.curdir, "routes.cfg"))
    scp = SafeConfigParser()
    buf = StringIO.StringIO(defaults)
    scp.readfp(buf)
    if config_name is not None:
        files = scp.read([config_name])
    else:
        files = scp.read([
            "/etc/grouper/process_changelog.cfg", 
            os.path.expanduser("~/.process_changelog.cfg"),
            os.path.join(os.path.dirname(__file__), "process_changelog.cfg"),
            os.path.join(os.path.abspath(os.curdir), "process_changelog.cfg")])
    info("Read configuration from: %s" % (', '.join(files)))
    return scp

def getSSLContext(trust_store, passphrase, tls_protocol="TLSv1.1"):
    tstore = KeyStore.getInstance("JKS")
    tstore.load(FileInputStream(trust_store), passphrase)
    tmf = TrustManagerFactory.getInstance("SunX509")
    tmf.init(tstore)
    c = SSLContext.getInstance(tls_protocol)
    c.init(None, tmf.getTrustManagers(), None)
    return c    

def main(args):
    scp = load_config(args.config)
    host = args.host
    if host is None:
        host = scp.get("AMQP", "host")
    port = args.port
    if port is None:
        port = scp.getint("AMQP", "port")
    vhost = args.vhost
    if vhost is None:
        vhost = scp.get("AMQP", "vhost")
    user = args.user
    if user is None:
        user = scp.get("AMQP", "user")
    passwd_file = args.passwd_file
    if passwd_file is None:
        passwd = scp.get("AMQP", "password")
    else:
        passwd = passwd_file.read().strip()
    exchange = args.exchange
    if exchange is None:
        exchange = scp.get("AMQP", "exchange")
    if scp.has_option("AMQP", "keystore"):
        keystore = scp.get("AMQP", "keystore")
    else:
        keystore = None
    if scp.has_option("AMQP", "keystore_passphrase"):
        keystore_passphrase = scp.get("AMQP", "keystore_passphrase")
    else:
        keystore_passphrase = None
    if scp.has_option("AMQP", "tls_version"):
        tls_version = scp.get("AMQP", "tls_version")
    else:
        tls_version = "TLSv1.1"
    changefile = args.change_file
    if changefile is None:
        changefile = scp.get("APPLICATION", "changefile")
    routefile = args.route_file
    if routefile is None:
        routefile = scp.get("APPLICATION", "routemap")
    debug("AMQP host => '%s'" % host)
    debug("AMQP port => '%s'" % port)
    debug("AMQP vhost => '%s'" % vhost)
    debug("AMQP user => '%s'" % user)
    debug("AMQP exchange => '%s'" % exchange)
    debug("AMQP changefile => '%s'" % changefile)
    debug("AMQP routemap => '%s'" % routefile)
    if (keystore is not None) and (keystore_passphrase is not None):
        debug("AMQP keystore => '%s'" % keystore)
    else:
        debug("AMQP not configured for TLS (missing keystore or passphrase).")
    # Read routes.
    routes = load_route_map(routefile)
    router = Router(routes)
    # Connect to message queue.
    if keystore is not None and keystore_passphrase is not None:
        ssl_ctx = getSSLContext(keystore, keystore_passphrase, tls_version)
    else:
        ssl_ctx = None
    amqp = get_amqp_conn(host, port, vhost, user, passwd, ssl_ctx=ssl_ctx)
    channel = amqp.createChannel()                 
    # END connecto to message queue.
    session = getRootSession()
    factory = GrouperDAOFactory.getFactory()
    consumer = factory.getChangeLogConsumer()
    c = ChangeLogConsumer()
    d = datetime.datetime.now()
    job_name = "CustomJob_%s" % d.strftime("%Y-%m-%dT%H:%M:%S")
    c.setName(job_name)
    consumer.saveOrUpdate(c)
    # Prime the last sequence number.
    last_sequence = get_last_sequence(changefile)
    if last_sequence is None:
        last_sequence = ChangeLogEntry.maxSequenceNumber(True)
    info("Last sequence number is %d.\n" % last_sequence)
    c.setLastSequenceProcessed(GrouperUtil.defaultIfNull(last_sequence, 0L).longValue()) 
    consumer.saveOrUpdate(c)
    # Initialize the consumer job.
    hib3 = Hib3GrouperLoaderLog()
    hib3.setHost(GrouperUtil.hostname())
    hib3.setJobName(job_name)
    hib3.setStatus(GrouperLoaderStatus.RUNNING.name())
    # Begin the main loop.
    attempt_num_entries = 100
    while True:
        GrouperLoader.runOnceByJobName(session, GrouperLoaderType.GROUPER_CHANGE_LOG_TEMP_TO_CHANGE_LOG)
        last_sequence = c.getLastSequenceProcessed()
        l = factory.getChangeLogEntry().retrieveBatch(last_sequence, attempt_num_entries)
        num_entries_retrieved = len(l)
        debug("Retrieved %d entries to process ..." % num_entries_retrieved)
        for n, entry in enumerate(l):
            action_name =  entry.getChangeLogType().getActionName()
            if action_name != u'addMembership' and action_name != u'deleteMembership':
                continue
            if action_name == u'addMembership':
                subject_id = entry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId)
                group = entry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName)
            elif action_name == u'deleteMembership':
                subject_id = entry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.subjectId)
                group = entry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.groupName)
            while True:
                debug("Attempting to send message: exchange='%s', group='%s', action='%s', subject='%s'" % (
                    exchange, group, action_name, subject_id))
                try:
                    send_group_mod(channel, exchange, router, group, action_name, subject_id)
                except (Exception, ), ex:
                    warn("Could not send message.\n%s\n" % str(ex))
                    time.sleep(10)
                    continue
                except (java.net.SocketException, com.rabbitmq.client.AlreadyClosedException), ex:
                    while True:
                        time.sleep(20)
                        # Try to reconnect.
                        try:
                            amqp = get_amqp_conn(host, port, vhost, user, passwd, ssl_ctx=ssl_ctx)
                            channel = amqp.createChannel()                 
                        except (KeyboardInterrupt,), ex:
                            raise
                        except:
                            warn("Could not reconnect to exchange.  Will retry.")
                            continue
                        break
                break
            update_last_sequence(changefile, n+last_sequence+1)

        c.setLastSequenceProcessed(c.getLastSequenceProcessed() + num_entries_retrieved)
        consumer.saveOrUpdate(c)
        if num_entries_retrieved != attempt_num_entries:
            time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Grouper change log.")
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        help="Config file to use.")
    parser.add_argument(
        "--host",
        action="store",
        help="The host where the exchange is located.  Default 'localhost'.")
    parser.add_argument(
        "-P",
        "--port",
        action="store",
        type=int,
        help="The port on which the exchange is listening.  Default 5672.")
    parser.add_argument(
        "-e",
        "--exchange",
        action="store",
        default="grouper_exchange",
        help="The exchange to which the message is sent.  Default 'grouper_exchange'.")
    parser.add_argument(
        "--vhost",
        action="store",
        help="The virtual host.  Default '/'.")
    parser.add_argument(
        "-u",
        "--user",
        action="store",
        help="The username used to connect.  Default 'guest'.")
    parser.add_argument(
        "-p",
        "--passwd-file",
        action="store",
        type=argparse.FileType("r"),
        help="A file containing the password.  If not specified, 'guest' will be used as the password.")
    parser.add_argument(
        "--change-file",
        action="store",
        help="A file used to record the last change processed.")
    parser.add_argument(
        "--route-file",
        action="store",
        help="A file containing the routing patterns.")
    args = parser.parse_args()
    main(args)

