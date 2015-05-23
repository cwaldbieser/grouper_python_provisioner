#! /usr/bin/env python

# Standard library
from __future__ import print_function
from ConfigParser import SafeConfigParser
from  cStringIO import StringIO
import contextlib
import datetime
import hmac
from json import load
import os
import os.path
import sqlite3
from textwrap import dedent

# External modules
# - python-ldap
#import ldap
#import ldap.dn
#import ldap.modlist
#from ldap.filter import escape_filter_chars as escape_fltr
# - Twisted
from twisted.application import service
from twisted.application.service import Service
from twisted.internet import reactor, task, threads
from twisted.internet.defer import inlineCallbacks
from twisted.internet.endpoints import serverFromString
from twisted.internet.protocol import ClientCreator, Factory, Protocol, connectionDone
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver
from twisted.logger import Logger
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
import txamqp.spec
# - application
from logging import make_syslog_observer

def load_config(config_file=None):
    if config_file is None:
        basefile="txgroupprovisioner.cfg"
        syspath = os.path.join("/etc/grouper", basefile)
        homepath = os.path.expanduser("~/.{0}".format(basefile))
        apppath = os.path.join(os.path.dirname(__file__), basefile)
        curpath = os.path.join(os.curdir, basefile)
        files = [syspath, homepath, apppath, curpath]
    else:
        files = [config_file]
    spec_dir = os.path.join(os.path.split(os.path.split(__file__)[0])[0], "spec")
    spec_path = os.path.join(spec_dir, "amqp0-9-1.stripped.xml")
    defaults = dedent("""\
        [APPLICATION]
        log_level = DEBUG
        sqlite_db = groups.db
        group_map = groupmap.json
        
        [LDAP]
        log_level = WARN
        url = ldap://127.0.0.1:389/
        start_tls = 1

        [AMQP]
        log_level = INFO
        host = localhost
        port = 5672
        exchange = grouper_exchange
        vhost = /
        spec = {spec_path}
        user = guest
        passwd = guest
        """.format(spec_path=spec_path))
    buf = StringIO(defaults)
    scp = SafeConfigParser()
    scp.readfp(buf)
    scp.read(files)
    return scp
    
def section2dict(scp, section):
    d = {}
    for option in scp.options(section):
        d[option] = scp.get(section, option)
    return d
        
def init_db(db_str):
    with sqlite3.connect(db_str) as db:
        commands = []
        sql = dedent("""\
            CREATE TABLE intake(
                grp TEXT NOT NULL,
                member TEXT NOT NULL,
                op TEXT NOT NULL
            );
            """)
        commands.append(sql)
        sql = dedent("""\
            CREATE TABLE groups(
                grp TEXT NOT NULL
            );
            """)
        commands.append(sql)
        sql = dedent("""\
            CREATE TABLE member_ops(
                member TEXT NOT NULL,
                op TEXT NOT NULL,
                grp INTEGER NOT NULL
            );
            """)
        commands.append(sql)
        sql = """CREATE UNIQUE INDEX ix0 ON groups (grp);"""
        commands.append(sql)
        sql = """CREATE UNIQUE INDEX ix1 ON member_ops (member, grp);"""
        commands.append(sql)
        for sql in commands:
            try:
                c = db.cursor()
                c.execute(sql)
                db.commit()
            except sqlite3.OperationalError as ex:
                if not str(ex).endswith(" already exists"):
                    raise
        
def load_group_map(gm):
    with open(gm, "r") as f:
        o = load(f)
    return o
        

class ServiceState(object):
    db_str = None
    last_update = None
    amqp_info = None
    read_from_queue = False

class GroupProvisionerService(Service):
    log = None

    def __init__(
            self, 
            a_reactor=None, 
            ws_endpoint_str=None, 
            use_syslog=False, 
            syslog_prefix=None,
            logfile=None):
        """
        Initialize the service.
        
        :param a_reactor: Override the reactor to use.
        """
        if a_reactor is None:
            a_reactor = reactor
        self._reactor = a_reactor
        self._port = None
        self.service_state = ServiceState()
        self.use_syslog = use_syslog
        self.syslog_prefix = syslog_prefix
        
    def startService(self):
        """
        Start the service.
        """
        scp = load_config()
        app_info = section2dict(scp, "APPLICATION")
        log_level = app_info.get("log_level", "INFO")
        self.log = Logger(
            observer=make_syslog_observer(
                log_level, 
                prefix=self.syslog_prefix))
        db_str = scp.get("APPLICATION", "sqlite_db")
        group_map = load_group_map(scp.get("APPLICATION", "group_map"))
        ldap_info = section2dict(scp, 'LDAP')
        amqp_info = section2dict(scp, "AMQP")
        amqp_log_level = amqp_info.get("log_level", log_level) 
        self.amqp_log = Logger(
            observer=make_syslog_observer(
                amqp_log_level, 
                prefix=self.syslog_prefix))
        service_state = self.service_state 
        service_state.db_str = db_str
        init_db(db_str)
        service_state.amqp_info = amqp_info
        service_state.last_update = None
        self.start_amqp_client(amqp_info)
        #processor = LoopingCall(process_requests, db_str, ldap_info, group_map, service_state)
        #processor.start(10)

    def start_amqp_client(self, amqp_info):
        log = self.amqp_log
        host = amqp_info['host']
        port = int(amqp_info['port'])
        exchange = amqp_info['exchange']
        vhost = amqp_info['vhost']
        spec_path = amqp_info['spec']
        queue_name = amqp_info['queue']
        route_file = amqp_info['route_map']
        user = amqp_info['user']
        passwd = amqp_info['passwd']
        creds = (user, passwd)
        bindings = self.parse_bindings(route_file)
        queue_names = set([q for q, rk in bindings])
        queue_names.add(queue_name)
        log.debug(
            "host='{host}', port={port}, exchange='{exchange}', vhost='{vhost}', user='{user}, spec={spec}'",
            host=host, port=port, exchange=exchange, vhost=vhost, user=user, spec=spec_path)
        for q in sorted(queue_names):
            log.debug("Declared: queue='{queue}'", queue=q)
        for q, rk in bindings:
            log.debug("Binding: queue='{queue}', route_key='{route_key}'", queue=q, route_key=rk)
        delegate = TwistedDelegate()
        spec = txamqp.spec.load(spec_path)
        d = ClientCreator(self._reactor, AMQClient, delegate=delegate, vhost=vhost, spec=spec).connectTCP(host, port)
        d.addCallback(self.on_amqp_connect, exchange, queue_name, queue_names, bindings, creds)

        def onError(err):
            if reactor.running:
                log.failure(err)
                reactor.stop()

        d.addErrback(onError)
        self.service_state.read_from_queue = True

    def parse_bindings(self, fname):
        """
        Queue map should be a JSON list of (queue_name, route_key) mappings.
        """
        with open(fname, "r") as f:
            o = load(f)
            return o

    @inlineCallbacks
    def on_amqp_connect(self, conn, exchange, queue_name, queue_names, bindings, creds):
        log = self.amqp_log
        service_state = self.service_state
        db_str = service_state.db_str
        log.info("Connected.")
        user, passwd = creds
        yield conn.authenticate(user, passwd)
        log.info("Authenticated.")
        channel = yield conn.channel(1)
        yield channel.channel_open()
        log.info("Channel opened.")
        for name in queue_names:
            yield channel.queue_declare(queue=name, durable=True)
        log.info("Queues declared.")
        yield channel.exchange_declare(exchange=exchange, type='topic')
        log.info("Exchange declared.")
        for qname, route_key in bindings:
            yield channel.queue_bind(exchange=exchange, queue=qname, routing_key=route_key)
        log.info("Routings have been mapped.")
        yield channel.basic_consume(queue=queue_name, consumer_tag="mytag")
        queue = yield conn.queue("mytag") 
        delay = 0
        reactor = self._reactor
        while service_state.read_from_queue:
            msg = yield queue.get()
            log.debug('Received: "{msg}" from channel # {channel}.', msg=msg.content.body, channel=channel.id)
            parts = msg.content.body.split("\n")
            try:
                group = parts[0]
                subject = parts[1]
                action = parts[2]
            except IndexError:
                log.warn("Skipping invalid message: {msg!r}", msg=msg.content.body)
                yield channel.basic_ack(delivery_tag=msg.delivery_tag)
                continue
            try:
                yield task.deferLater(reactor, delay, record_group_action, group, action, subject, db_str)
            except Exception as ex:
                log.error("Could not record message from queue.  Error was: {error}", error=ex)
                delay = min(600, max(delay+20, delay*2))
            else:
                delay = 0    
                yield channel.basic_ack(delivery_tag=msg.delivery_tag)
                log.debug("Message from queue recorded.")
        log.info("Closing AMQP channel ...")
        yield channel.channel_close()
        log.info("AMQP Channel closed.")
        log.info("Closing AMQP connection ...")
        yield conn.connection_close()
        log.info("AMQP Connection closed.")


    def set_listening_port(self, port):
        self._port = port
        
    def stopService(self):
        """
        Stop the service.
        """
        if self._port is not None:
            return self._port.stopListening()

#=======================================================================
# Record incoming requests.
#=======================================================================
def record_group_action(group, action, subject, db_str):
    d = threads.deferToThread(add_action_to_intake, group, action, subject, db_str)
    return d
    

def add_action_to_intake(group, action, member, db_str):
    """
    !!! Blocking API !!!
    Adds a member add/delete to the intake table.
    SQLite3 guaruntees the ROWID of the table will be monotomically increasing
    as long as the max ROWID value is not hit and as long as the max value in the 
    table is never deleted.  If the table is empty, a ROWID of 1 is used.
    See: https://www.sqlite.org/autoinc.html
    """
    with sqlite3.connect(db_str) as db:
        sql = dedent("""\
            INSERT INTO intake(grp, member, op) VALUES(?, ?, ?);
            """)
        c = db.cursor()
        c.execute(sql, [group, member, action])
        db.commit()

#=======================================================================
# Process stored requests.
#=======================================================================
def process_requests(db_str, ldap_info, group_map, service_state):

    def set_last_update(result, service_state):
        """
        Set the last-updated time on the service state.
        """
        service_state.last_update = datetime.datetime.today()
        return result
        
    d = threads.deferToThread(blocking_process_requests, db_str, ldap_info, group_map)
    d.addCallback(set_last_update, service_state)
    #If there is an error, log it, but keep on looping.
    d.addErrback(log.err)
    return d
    
def group_to_ldap_group(g, group_map):
    """
    """
    result = group_map.get(g, None)
    if result is not None:
        result = result.lower()
    return result
    
########################################################################
# Blocking functions
########################################################################
def blocking_process_requests(db_str, ldap_info, group_map):
    """
    """
    ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
    with sqlite3.connect(db_str) as db:
        # Transfer intake table to normalized batch tables.
        transfer_intake_to_batch(db)
        # Process the normalized batch.
        base_dn = ldap_info['base_dn']
        with connect_to_directory(ldap_info['url']) as lconn:
            lconn.start_tls_s()
            lconn.simple_bind(ldap_info['bind_dn'], ldap_info['passwd'])
        
            group_sql = """SELECT rowid, grp FROM groups ORDER BY grp ASC;"""
            memb_add_sql = """SELECT member FROM member_ops WHERE grp = ? AND op = 'addMembership' ORDER BY member ASC;""" 
            memb_del_sql = """SELECT member FROM member_ops WHERE grp = ? AND op = 'deleteMembership' ORDER BY member ASC;""" 
            subj_sql = """SELECT DISTINCT member FROM member_ops ORDER BY member ASC;"""
            subj_add_sql = dedent("""\
                SELECT DISTINCT groups.grp 
                FROM groups
                    INNER JOIN member_ops
                        ON groups.rowid = member_ops.grp
                WHERE member = ?
                AND op = 'addMembership'
                ORDER BY groups.grp ASC
                ;
                """)
            subj_del_sql = dedent("""\
                SELECT DISTINCT groups.grp 
                FROM groups
                    INNER JOIN member_ops
                        ON groups.rowid = member_ops.grp
                WHERE member = ?
                AND op = 'deleteMembership'
                ORDER BY groups.grp ASC
                ;
                """)
            c = db.cursor()
            c.execute(group_sql)
            mapped_groups = {}
            for groupid, group in list(fetch_batch(c)):
                ldap_group = group_to_ldap_group(group, group_map)
                if ldap_group is None:
                    #print "[DEBUG] Group '{group}' is not a target group.  Skipping ...".format(group=ldap_group)
                    c = db.cursor()
                    c.execute("""DELETE FROM member_ops WHERE grp = ?;""", [groupid])
                    c = db.cursor()
                    c.execute("""DELETE FROM groups WHERE grp = ?;""", [group])
                    db.commit()
                    continue
                c = db.cursor()
                c.execute(memb_add_sql, [groupid])
                add_membs = set([r[0] for r in list(fetch_batch(c))])
                c = db.cursor()
                c.execute(memb_del_sql, [groupid])
                del_membs = set([r[0] for r in list(fetch_batch(c))])
                if len(add_membs) > 0 or len(del_membs) > 0:
                    #print "[DEBUG] Applying changes to group {group} ...".format(group=ldap_group)
                    #print "- Adds -"
                    #print '\n  '.join(sorted(add_membs))
                    #print "- Deletes -"
                    #print '\n  '.join(sorted(del_membs))
                    group_dn = apply_changes_to_ldap_group(ldap_group, add_membs, del_membs, base_dn, lconn, ldap_info)
                    mapped_groups[ldap_group] = group_dn
            c = db.cursor()
            c.execute(subj_sql)
            for subject_id in list(r[0] for r in fetch_batch(c)): 
                c = db.cursor()
                c.execute(subj_add_sql, [subject_id])
                add_membs = set(mapped_groups[group_to_ldap_group(r[0], group_map)] for r in fetch_batch(c))
                c = db.cursor()
                c.execute(subj_del_sql, [subject_id])
                del_membs = set(mapped_groups[group_to_ldap_group(r[0], group_map)] for r in fetch_batch(c))
                if len(add_membs) > 0 or len(del_membs) > 0:
                    #print "[DEBUG] Applying changes to subject {subject} ...".format(subject=subject_id)
                    #print "- Adds -"
                    #print '\n  '.join(sorted(add_membs))
                    #print "- Deletes -"
                    #print '\n  '.join(sorted(del_membs))
                    
                    apply_changes_to_ldap_subj(subject_id, add_membs, del_membs, base_dn, lconn)
                
            sql = """DELETE FROM groups;"""
            c = db.cursor()
            c.execute(sql)
            sql = """DELETE FROM member_ops;"""
            c = db.cursor()
            c.execute(sql)
            db.commit()
            
def transfer_intake_to_batch(db):
    """
    Transfer the intake table to the batch tables.
    This algorithm depends on the behavior of the SQLite3 ROWID-- specifically
    its properties relating to monotomically increasing values, and its reset
    to 1 if the table is empty.
    
    Ref: https://www.sqlite.org/autoinc.html
    """
    sql = dedent("""\
        SELECT rowid, grp, member, op
        FROM intake
        ORDER BY rowid ASC
        ;
        """)
    c = db.cursor()
    c.execute(sql)
    intake = list(fetch_batch(c))
    del c
    for rowid, group, member, action in intake:
        groupid = get_group_id(group, db)
        sql = dedent("""\
            SELECT op, member
            FROM member_ops 
            WHERE grp = ?
            AND member = ?
            ;
            """)
        c = db.cursor()
        c.execute(sql, [groupid, member])
        result = c.fetchone()
        if result is None:
            sql = """INSERT INTO member_ops(op, member, grp) VALUES(?, ?, ?);"""
            c = db.cursor()
            c.execute(sql, [action, member, groupid])
        else:
            sql = """UPDATE member_ops SET op = ? WHERE grp=? AND member=?;"""
            c = db.cursor()
            c.execute(sql, [action, groupid, member])
        sql = """DELETE FROM intake WHERE rowid = ? ;"""
        c = db.cursor()
        c.execute(sql, [rowid])
        db.commit()
            
    #print "[DEBUG] Action has been batched."

      
def fetch_batch(cursor, batch_size=None, **kwds):
    """
    Generator fetches batches of rows from the database cursor.
    """
    if batch_size is None:
        batch_size = cursor.arraysize
    while True:
        rows = cursor.fetchmany(batch_size)
        if len(rows) == 0:
            break
        for row in rows:
            yield row
      
@contextlib.contextmanager
def connect_to_directory(url):
    conn = ldap.initialize(url)
    yield conn
    conn.unbind()
            
def apply_changes_to_ldap_group(group, adds, deletes, base_dn, conn, ldap_info):
    empty_dn = ldap_info.get("empty_dn", None)
    fq_adds = set(x[1] for x in load_subject_dns(adds, base_dn, conn))
    fq_deletes = set(x[1] for x in load_subject_dns(deletes, base_dn, conn))
    group_dn, attribs = lookup_group(group, base_dn, conn) 
    memb_set = set([m.lower() for m in attribs['member']])
    memb_set = memb_set.union(fq_adds)
    memb_set = memb_set - fq_deletes
    if empty_dn is not None:
        if len(memb_set) == 0:
            memb_set.add(empty_dn)
        if len(memb_set) > 1 and empty_dn in memb_set:
            memb_set.remove(empty_dn)
    members = list(memb_set)
    members.sort()
    new_attribs = dict(member=members)
    ml = ldap.modlist.modifyModlist(attribs, new_attribs)
    try:
        conn.modify_s(group_dn, ml)
    except ldap.LDAPError as ex:
        log.msg("[ERROR] Error while attempting to modify LDAP group: {0}".format(group_dn)) 
        raise
    return group_dn.lower()
    
def apply_changes_to_ldap_subj(subject_id, fq_adds, fq_deletes, base_dn, conn):
    """
    """
    subj_dns = list(load_subject_dns([subject_id], base_dn, conn))
    if len(subj_dns) == 0:
        log.msg("[WARN] No DN found for subject ID '{0}.  Skipping ...'".format(subject_id))
        return
    assert not len(subj_dns) > 1, "Multiple DNs found for subject ID '{0}'".format(subject_id)
    subj_dn = subj_dns[0][1]
    membs = load_subject_memberships(subj_dn, conn)
    memb_set = set([m.lower() for m in membs])
    memb_set = memb_set.union(fq_adds)
    memb_set = memb_set - fq_deletes
    members = list(memb_set)
    members.sort()
    attribs = dict(memberOf=membs)
    new_attribs = dict(memberOf=members)
    ml = ldap.modlist.modifyModlist(attribs, new_attribs)
    try:
        conn.modify_s(subj_dn, ml)
    except ldap.LDAPError as ex:
        log.msg("[ERROR] Error while attempting to modify LDAP subject: {0}".format(subj_dn)) 
        raise
    
def load_subject_dns(subject_ids, base_dn, conn):
    for subject_id in subject_ids:
        fltr = "cn={0}".format(escape_fltr(subject_id))
        try:
            results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['cn']) 
        except ldap.LDAPError as ex:
            log.msg("[ERROR] Error while searching for LDAP subject: {0}".format(subject_id)) 
            raise
        for result in results:
            yield (subject_id, result[0].lower())

def lookup_group(group_name, base_dn, conn):
    fltr = "cn={0}".format(escape_fltr(group_name))
    try:
        results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['member']) 
    except ldap.LDAPError as ex:
        log.msg("[ERROR] Error while searching for LDAP group: {0}".format(group_name)) 
        raise
    assert len(results) > 0, "Could not find group, '{0}'.".format(group_name)
    return results[0]

def load_subject_memberships(dn, conn):
    try:
        results = conn.search_s(dn, ldap.SCOPE_BASE, attrlist=['memberOf']) 
    except ldap.LDAPError as ex:
        log.msg("[ERROR] Error while fetching memberships for LDAP subject: {0}".format(dn)) 
        raise
    result = results[0]
    return result[1].get('memberOf', [])
    
def get_group_id(group, db):
    sql = """SELECT rowid FROM groups WHERE grp = ?;"""
    c = db.cursor()
    c.execute(sql, [group])
    result = c.fetchone()
    if result is None:
        sql = """INSERT INTO groups (grp) VALUES (?);"""
        c = db.cursor()
        c.execute(sql, [group])
        group_id = c.lastrowid
        db.commit()
        return group_id
    else:
        return result[0] 
    
########################################################################
########################################################################
        
def main():
    service = GroupProvisionerService()
    service.startService()
    reactor.run()

if __name__ == "__main__":
    main()
else:
    application = service.Application("Twisted Group Provisioner")
    service = GroupProvisionerService()
    service.setServiceParent(application)
    
    
    
