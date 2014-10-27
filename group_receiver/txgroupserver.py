#! /usr/bin/env python

# Standard library
import contextlib
import hmac
import pprint
import sqlite3
from textwrap import dedent

# External modules
# - python-ldap
import ldap
import ldap.dn
import ldap.modlist
from ldap.filter import escape_filter_chars as escape_fltr
# - Twisted
from twisted.internet import reactor, threads
from twisted.internet.endpoints import serverFromString
from twisted.internet.protocol import Protocol, connectionDone
from twisted.internet.protocol import Factory
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver

#=======================================================================
# Protocol
#=======================================================================
class GroupReceiver(LineReceiver):
    
    max_members = 10000
    actions = ('addMembership', 'deleteMembership')
    hmac_key = 'HMAC key goes here.'
    
    def connectionMade(self):
        LineReceiver.connectionMade(self)
        self.group = None
        self.action = None
        self.subject = None

    def lineReceived(self, line):
        #print "[DEBUG] Line received: %s" % line
        parts = line.split(':', 1)
        if len(parts) != 2:
            self.sendLine("Command missing argument.")
            self.transport.loseConnection()
            return
            
        if self.group is None:
            if parts[0] != 'group':
                self.sendLine("Expected 'group'.")
                self.transport.loseConnection()
                return
            self.group = parts[1]
        elif self.action is None:
            if parts[0] != "action":
                self.sendLine("Expected 'action'.")
                self.transport.loseConnection()
                return
            action = parts[1]
            if action not in self.actions:
                self.sendLine("Action must be one of: %s" % str(', '.join(self.actions)))
                self.transport.loseConnection()
                return
            self.action = action
        elif self.subject is None:
            if parts[0] != 'subject':
                self.sendLine("Expected 'subject'.")
                self.transport.loseConnection()
                return
            self.subject = parts[1]
        elif parts[0] == 'hmac':
            presented_digest = parts[1]
            h = hmac.new(self.hmac_key)
            h.update(self.group)
            h.update(self.action)
            h.update(self.subject)
            computed_digest = h.hexdigest()
            if hasattr(hmac, 'compare_digest'):
                hcmp = hmac.compare_digest
            else:
                hcmp = self.compare_digests
                
            if hcmp(presented_digest, computed_digest):
                #print "[DEBUG] Creating deferred to record ..."
                self.deferred = record_group_action(self.group, self.action, self.subject, self.factory.db_str)
                self.deferred.addCallback(self.message_stored)
                self.deferred.addErrback(self.message_not_stored)
                #print "[DEBUG] Deferred created."
                return
            else:
                self.sendLine("Invalid HMAC.")
                self.transport.loseConnection()
                return
        else:
            self.sendLine("Expected 'hmac'.")
            self.transport.loseConnection()
            return
            
    def compare_digests(self, a, b):
        """
        """
        asize = len(a)
        bsize = len(b)
        if asize < bsize:
            a = a + ' '*(bsize-asize)
        if bsize < asize:
            b = b + ' '*(asize-bsize)
        result = True
        for achar, bchar in zip(a, b):
            if achar != bchar:
                result = False
        return result
        
    def message_stored(self, result):
        """
        """
        self.sendLine("OK")
        self.transport.loseConnection()
        
    def message_not_stored(self, err):
        """
        """
        self.sendLine("ERROR: " + str(err))
        self.transport.loseConnection()

#=======================================================================
# Protocol factory
#=======================================================================
class GroupReceiverFactory(Factory):
    protocol = GroupReceiver

#=======================================================================
# Record incoming requests.
#=======================================================================
def record_group_action(group, action, subject, db_str):
    """
    """
    d = threads.deferToThread(add_action_to_batch, group, action, subject, db_str)
    return d
    

def add_action_to_batch(group, action, member, db_str):
    """
    """
    #print "[DEBUG] Batching action ..."
    with sqlite3.connect(db_str) as db:
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
            db.commit()
        else:
            sql = """UPDATE member_ops SET op = ? WHERE grp=? AND member=?;"""
            c = db.cursor()
            c.execute(sql, [action, groupid, member])
            db.commit()
            
    #print "[DEBUG] Action has been batched."

#=======================================================================
# Process stored requests.
#=======================================================================
def process_requests(db_str, ldap_info):
    """
    """
    return threads.deferToThread(blocking_process_requests, db_str, ldap_info)
    
def group_to_ldap_group(g):
    """
    """
    parts = g.split(":")
    group = parts[-1]
    if group in ['vpn', 'orkz']:
        return group.lower()
    else:
        return None
    
########################################################################
# Blocking functions
########################################################################
def blocking_process_requests(db_str, ldap_info):
    """
    """
    ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
    with sqlite3.connect(db_str) as db:
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
            group_map = {}
            for groupid, group in list(fetch_batch(c)):
                ldap_group = group_to_ldap_group(group)
                if ldap_group is None:
                    print "[DEBUG] Group '{group}' is not a target group.  Skipping ...".format(group=ldap_group)
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
                    print "[DEBUG] Applying changes to group {group} ...".format(group=ldap_group)
                    print "- Adds -"
                    print '\n  '.join(sorted(add_membs))
                    print "- Deletes -"
                    print '\n  '.join(sorted(del_membs))
                    group_dn = apply_changes_to_ldap_group(ldap_group, add_membs, del_membs, base_dn, lconn)
                    group_map[ldap_group] = group_dn
            c = db.cursor()
            c.execute(subj_sql)
            for subject_id in list(r[0] for r in fetch_batch(c)): 
                c = db.cursor()
                c.execute(subj_add_sql, [subject_id])
                add_membs = set(group_map[group_to_ldap_group(r[0])] for r in fetch_batch(c))
                c = db.cursor()
                c.execute(subj_del_sql, [subject_id])
                del_membs = set(group_map[group_to_ldap_group(r[0])] for r in fetch_batch(c))
                if len(add_membs) > 0 or len(del_membs) > 0:
                    print "[DEBUG] Applying changes to subject {subject} ...".format(subject=subject_id)
                    print "- Adds -"
                    print '\n  '.join(sorted(add_membs))
                    print "- Deletes -"
                    print '\n  '.join(sorted(del_membs))
                    
                    apply_changes_to_ldap_subj(subject_id, add_membs, del_membs, base_dn, lconn)
                
            sql = """DELETE FROM groups;"""
            c = db.cursor()
            c.execute(sql)
            sql = """DELETE FROM member_ops;"""
            c = db.cursor()
            c.execute(sql)
            db.commit()
      
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
    """
    """
    conn = ldap.initialize(url)
    yield conn
    conn.unbind()
            
def apply_changes_to_ldap_group(group, adds, deletes, base_dn, conn):
    """
    """
    fq_adds = set(x[1] for x in load_subject_dns(adds, base_dn, conn))
    fq_deletes = set(x[1] for x in load_subject_dns(deletes, base_dn, conn))
    group_dn, attribs = lookup_group(group, base_dn, conn) 
    memb_set = set([m.lower() for m in attribs['member']])
    memb_set = memb_set.union(fq_adds)
    memb_set = memb_set - fq_deletes
    members = list(memb_set)
    members.sort()
    new_attribs = dict(member=members)
    ml = ldap.modlist.modifyModlist(attribs, new_attribs)
    conn.modify_s(group_dn, ml)
    return group_dn.lower()
    
def apply_changes_to_ldap_subj(subject_id, fq_adds, fq_deletes, base_dn, conn):
    """
    """
    subj_dns = list(load_subject_dns([subject_id], base_dn, conn))
    assert not len(subj_dns) == 0, "No DN found for subject ID '{0}'".format(subject_id)
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
    conn.modify_s(subj_dn, ml)
    
def load_subject_dns(subject_ids, base_dn, conn):
    """
    """
    for subject_id in subject_ids:
        fltr = "cn={0}".format(escape_fltr(subject_id))
        results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['cn']) 
        for result in results:
            yield (subject_id, result[0].lower())

def lookup_group(group_name, base_dn, conn):
    """
    """
    fltr = "cn={0}".format(escape_fltr(group_name))
    results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['member']) 
    assert len(results) > 0, "Could not find group, '{0}'.".format(group_name)
    return results[0]

def load_subject_memberships(dn, conn):
    """
    """
    results = conn.search_s(dn, ldap.SCOPE_BASE, attrlist=['memberOf']) 
    result = results[0]
    return result[1]['memberOf']
    
def get_group_id(group, db):
    """
    """
    sql = """SELECT rowid FROM groups WHERE grp = ?;"""
    c = db.cursor()
    print "sql:", sql
    print "group", group
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
        
#=======================================================================
#=======================================================================
def main():
    """
    """
    ldap_info = {
        'url': 'ldap://ldap-dev2.dev.lafayette.edu',
        'start_tls': True,
        'bind_dn': 'cn=manager,o=lafayette',
        'passwd': 'bullsh0t',
        'base_dn': 'o=lafayette'}
    db_str = "groups.db"
    
    conn = serverFromString(reactor, "tcp:9600")
    factory = GroupReceiverFactory()
    factory.db_str = db_str
    conn.listen(factory) 
    processor = LoopingCall(process_requests, db_str, ldap_info)
    processor.start(10)
    reactor.run()

if __name__ == "__main__":
    main()

