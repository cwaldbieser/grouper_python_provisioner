
#export CLASSPATH=/opt/jyson-1.0.2/lib/jyson-1.0.2.jar

# Standard library
import datetime
from pickle import dumps
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


ENDPOINT=('127.0.0.1', 9600)


def send_group_mod(group, action_name, subject_id):
    """
    """
    d = dict(group=group, action=action_name, member=subject_id)
    serialized = dumps(d) 
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(ENDPOINT)
        sock.sendall(serialized + '\n')
        received = sock.recv(1024)
        if received != "OK":
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
c.setLastSequenceProcessed(GrouperUtil.defaultIfNull(ChangeLogEntry.maxSequenceNumber(True), 0).longValue()) 
consumer.saveOrUpdate(c)
hib3 = Hib3GrouperLoaderLog()
hib3.setHost(GrouperUtil.hostname())
hib3.setJobName(job_name)
hib3.setStatus(GrouperLoaderStatus.RUNNING.name())

attempt_num_entries = 100
while True:
    GrouperLoader.runOnceByJobName(session, GrouperLoaderType.GROUPER_CHANGE_LOG_TEMP_TO_CHANGE_LOG)
    l = factory.getChangeLogEntry().retrieveBatch(c.getLastSequenceProcessed(), attempt_num_entries)
    num_entries_retrieved = len(l)
    gmods = [e for e in l 
        if e.getChangeLogType().getActionName() == u'addMembership' 
            or e.getChangeLogType().getActionName() == u'deleteMembership']  
    for entry in gmods:                 
        action_name =  entry.getChangeLogType().getActionName()
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

    c.setLastSequenceProcessed(c.getLastSequenceProcessed() + num_entries_retrieved)
    consumer.saveOrUpdate(c)
    if num_entries_retrieved != attempt_num_entries:
        time.sleep(20)


