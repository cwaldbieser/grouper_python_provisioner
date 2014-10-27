


# Standard library
import datetime
import hmac
import os.path
import time
import socket
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

HMAC_KEY = "HMAC key goes here."
ENDPOINT=('127.0.0.1', 9600)
CHANGEFILE=os.path.join(
    os.path.dirname(__file__),
    "last_change_id.txt")

def get_last_sequence():
    """
    """
    global CHANGEFILE
    if os.path.exists(CHANGEFILE):
        try:
            f = open(CHANGEFILE, "r")
            data = f.read().strip()
            f.close()
            pos = long(data)
            return pos
        except (Exception,), ex:
            print "[WARNING] Could not get last sequence number."
            print str(ex)
            print
            return None
    return None
        
def update_last_sequence(n):
    """
    """
    global CHANGEFILE
    
    f = open(CHANGEFILE, "w")
    f.write(str(n))
    f.close()

def send_group_mod(group, action, subject):
    """
    """
    #print "[DEBUG] Entered `send_group_mod()`."
    global HMAC_KEY
    
    h = hmac.new(HMAC_KEY)
    h.update(group)
    h.update(action)
    h.update(subject)
    hexdigest = h.hexdigest()
    #print "[DEBUG] Computed HMAC."
    
    data = """group:%s\r\naction:%s\r\nsubject:%s\r\nhmac:%s\r\n""" % (
        group,
        action,
        subject,
        hexdigest)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(ENDPOINT)
        sock.sendall(data)
        print "[DEBUG] Sent data."
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

session = getRootSession()
factory = GrouperDAOFactory.getFactory()
consumer = factory.getChangeLogConsumer()
c = ChangeLogConsumer()
d = datetime.datetime.now()
job_name = "CustomJob_%s" % d.strftime("%Y-%m-%dT%H:%M:%S")
c.setName(job_name)
consumer.saveOrUpdate(c)
# Prime the last sequence number.
last_sequence = get_last_sequence()
if last_sequence is None:
    last_sequence = ChangeLogEntry.maxSequenceNumber(True)
print "Last sequence number is %d." % last_sequence
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
                send_group_mod(group, action_name, subject_id)
            except (Exception, ), ex:
                print "[WARNING] Could not send message."
                print str(ex)
                print
                time.sleep(10)
                continue
            break
        update_last_sequence(n+last_sequence+1)

    c.setLastSequenceProcessed(c.getLastSequenceProcessed() + num_entries_retrieved)
    consumer.saveOrUpdate(c)
    if num_entries_retrieved != attempt_num_entries:
        time.sleep(10)


