#! /usr/bin/env python -u

# Standard Library
import argparse
import contextlib
from cPickle import loads
import datetime
import socket
import SocketServer
import sqlite3
import sys
from textwrap import dedent

# External modules
import ldap
import ldap.dn
import ldap.modlist
from ldap.filter import escape_filter_chars as escape_fltr


class MyTCPHandler(SocketServer.StreamRequestHandler):
    """
    """
    def handle(self):
        """
        """
        #serialized = self.rfile.readline().strip()
        serialized = self.request.recv(1024)
        chunks = [serialized]
        while True:
            try:
                chunk = self.request.recv(1024, socket.MSG_DONTWAIT)
            except socket.error as ex:
                break
            chunks.append(chunk)
        serialized = ''.join(chunks)
        self.data = loads(serialized)
        print "Received: ", str(self.data)
        self.process()
        self.wfile.write("OK")

    def process(self):
        o = self.data
        group = o['group']
        action = o['action']
        member = o['member']
        print "Group: {0}".format(group)
        print "Action: {0}".format(action)
        print "Member: {0}".format(member)
        print
        add_action_to_batch(group, action, member, self.server.db)

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

def add_action_to_batch(group, action, member, db):
    """
    """
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

@contextlib.contextmanager
def connect_to_directory(url):
    """
    """
    conn = ldap.initialize(url)
    yield conn
    conn.unbind()

def lookup_group(group_name, base_dn, conn):
    """
    """
    fltr = "cn={0}".format(escape_fltr(group_name))
    results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['member']) 
    assert len(results) > 0, "Could not find group, '{0}'.".format(group_name)
    return results[0]

def update_group_memberships(group_dn, ml, conn):
    """
    """
    conn.modify_s(group_dn, ml)    

def get_head(dn):
    """
    """
    parts = ldap.dn.explode_dn(dn, notypes=True)
    return parts[0]

def load_members(group_dn, base_dn, conn):
    """
    """
    fltr = "memberOf={0}".format(escape_fltr(group_dn))
    results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['cn']) 
    for result in results:
        yield result[0]

def remove_group_from_subject(dn, group_dn, conn):
    """
    """
    memberships = load_subject_memberships(dn, conn)
    mset = set(g.lower() for g in memberships)
    group_dn = group_dn.lower()
    if group_dn in mset:
        mset.remove(group_dn)
        new_memberships = list(mset)
        new_memberships.sort()
        ml = ldap.modlist.modifyModlist(dict(memberOf=memberships), dict(memberOf=new_memberships))
        conn.modify_s(dn, ml) 

def add_group_to_subject(dn, group_dn, conn):
    """
    """
    memberships = load_subject_memberships(dn, conn)
    group_dn = group_dn.lower()
    new_memberships = set([m.lower() for m in memberships])
    new_memberships.add(group_dn)
    new_memberships = list(new_memberships)
    new_memberships.sort()
    ml = ldap.modlist.modifyModlist(dict(memberOf=memberships), dict(memberOf=new_memberships))
    conn.modify_s(dn, ml) 

def load_subject_memberships(dn, conn):
    """
    """
    results = conn.search_s(dn, ldap.SCOPE_BASE, attrlist=['memberOf']) 
    result = results[0]
    return result[1]['memberOf']

def load_subject_dns(subject_ids, base_dn, conn):
    """
    """
    for subject_id in subject_ids:
        fltr = "cn={0}".format(escape_fltr(subject_id))
        results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['cn']) 
        for result in results:
            yield (subject_id, result[0].lower())

class CustomTCPServer(SocketServer.TCPServer):
    """
    """
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

    def handle_timeout(self):
        pass

def init_db(db):
    """
    """
    commands = []
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
        print "sql", sql
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

def group_to_ldap_group(g):
    """
    """
    parts = g.split(":")
    group = parts[-1]
    if group in ['vpn', 'orkz']:
        return group.lower()
    else:
        return None

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

def process_requests(db, conn_info):
    """
    """
    base_dn = conn_info['base_dn']
    with connect_to_directory(conn_info['url']) as lconn:
        lconn.start_tls_s()
        lconn.simple_bind(conn_info['bind_dn'], conn_info['passwd'])
    
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
        
def elapsed_seconds(now, before):
    """
    """
    delta = now - before
    total_seconds = delta.seconds + 24*3600*delta.days
    return total_seconds 

def main(args):
    """
    """
    ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)

    conn_info = {
        'url': 'ldap://ldap-dev2.dev.lafayette.edu',
        'start_tls': True,
        'bind_dn': 'cn=manager,o=lafayette',
        'passwd': 'bullsh0t',
        'base_dn': 'o=lafayette'}

    with sqlite3.connect(args.db) as db:
        if args.init_db:
            init_db(db)
    
    server = CustomTCPServer(('127.0.0.1', 9600), MyTCPHandler)
    server.allow_reuse_address = True
    server.timeout = 10 
    server.db = db
    last_processed = None
    while True:
        now = datetime.datetime.today()
        if last_processed is None or elapsed_seconds(now, last_processed) > 30:
            print "[DEBUG] Processing requests ..."
            process_requests(db, conn_info)
            last_processed = now
        server.handle_request()
        print "handle_request() returned."


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON data into LDAP group memberships.")
    parser.add_argument(
        "-c", 
        "--config",
        action="store", 
        help="The config file to use.")
    parser.add_argument(
        "--init-db",
        action="store_true", 
        help="Initialize the database.")
    parser.add_argument(
        "-d",
        "--db",
        action="store", 
        default="groups.db",
        help="The SQLite database file to use (default 'groups.db').")
    args = parser.parse_args()
    main(args)





