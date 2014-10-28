


# Standard library
import argparse
from ConfigParser import SafeConfigParser
import datetime
import hmac
import os
import os.path
from textwrap import dedent
import time
import socket
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


def info(msg):
    """
    """
    sys.stderr.write("[INFO] %s\n" % msg)

def debug(msg):
    """
    """
    sys.stderr.write("[DEBUG] %s\n" % msg)

def warn(msg):
    """
    """
    sys.stderr.write("[WARNING] %s\n" % msg)

def get_last_sequence(changefile):
    """
    """
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
    """
    """
    f = open(changefile, "w")
    f.write(str(n))
    f.close()

def send_group_mod(host, port, hmac_key, group, action, subject):
    """
    """
    h = hmac.new(hmac_key)
    h.update(group)
    h.update(action)
    h.update(subject)
    hexdigest = h.hexdigest()
    
    data = """group:%s\r\naction:%s\r\nsubject:%s\r\nhmac:%s\r\n""" % (
        group,
        action,
        subject,
        hexdigest)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        sock.sendall(data)
        debug("Sent data.")
        buf = []
        while True:
            received = sock.recv(1024)
            if received is None or len(received) == 0:
                break
            buf.append(received)
        result = ''.join(buf)
        result = result.strip()
        if result != "OK":
            raise Exception("Socket server responded with: %s" % received)
    finally:
        sock.close()

def load_config(config_name):
    """
    """
    defaults = dedent("""\
        [APPLICATION]
        host=localhost
        port=9600
        changefile=%s
        """) % os.path.join(os.curdir, "last_change_id.txt")
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
        

def main(args):
    """
    """
    scp = load_config(args.config)

    host = args.host
    if host is None:
        host = scp.get("APPLICATION", "host")

    port = args.port
    if port is None:
        port = scp.getint("APPLICATION", "port")

    hmac_key = scp.get("APPLICATION", "hmac_key")
    assert hmac_key is not None, "HMAC key has not been set."

    changefile = None
    if changefile is None:
        changefile = scp.get("APPLICATION", "changefile")

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
    hib3 = Hib3GrouperLoaderLog()
    hib3.setHost(GrouperUtil.hostname())
    hib3.setJobName(job_name)
    hib3.setStatus(GrouperLoaderStatus.RUNNING.name())

    attempt_num_entries = 100
    while True:
        GrouperLoader.runOnceByJobName(session, GrouperLoaderType.GROUPER_CHANGE_LOG_TEMP_TO_CHANGE_LOG)
        last_sequence = c.getLastSequenceProcessed()
        l = factory.getChangeLogEntry().retrieveBatch(last_sequence, attempt_num_entries)
        num_entries_retrieved = len(l)
        for n, entry in enumerate(l):
            action_name =  entry.getChangeLogType().getActionName()
            if action_name != u'addMembership' and action_name != u'deleteMembership':
                continue
                
            subject_id = entry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId)
            group = entry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName)
            #print "action_name", action_name
            #print "subject_id", subject_id
            #print "group", group
            #print
            #print entry.toStringReport(True)
            #print
            while True:
                try:
                    send_group_mod(host, port, hmac_key, group, action_name, subject_id)
                except (Exception, ), ex:
                    warn("Could not send message.\n%s\n" % str(ex))
                    time.sleep(10)
                    continue
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
        help="The host to which change log events are sent.")
    parser.add_argument(
        "-p",
        "--port",
        action="store",
        type=int,
        help="The port to which change log events are transmitted.")
    args = parser.parse_args()
    main(args)
