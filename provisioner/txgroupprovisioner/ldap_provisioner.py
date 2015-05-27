
import ldap
import ldap.dn
import ldap.modlist
from ldap.filter import escape_filter_chars as escape_fltr
from twisted.plugin import IPlugin
from zope.interface import implements
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor, task, threads
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from txgroupprovisioner.interface import IProvisionerFactory, IProvisioner
import contextlib
import datetime
from json import load
import os
import os.path
import sqlite3
from textwrap import dedent
from config import load_config, section2dict
from logging import make_syslog_observer

class LDAPProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)

    tag = "ldap"
    opt_help = "Provisions an LDAP DIT with group membership changes."
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        return LDAPProvisioner()


class LDAPProvisioner(object):
    implements(IPlugin, IProvisioner)

    service_state = None
    log = None
    config = None
    batch_time = 10

    @inlineCallbacks
    def load_config(self, config_file=None, default_log_level='info', syslog_prefix=None):
        scp = load_config(config_file, defaults=self.getConfigDefaults())
        config = section2dict(scp, "PROVISIONER")
        self.config = config
        log_level = config.get('log_level', default_log_level)
        log = Logger(
            observer=make_syslog_observer(
                log_level, 
                prefix=syslog_prefix))
        self.log = log
        group_map = load_group_map(config['group_map'])
        self.group_map = group_map
        base_dn = config.get('base_dn', None)
        if base_dn is None:
            log.error("Must provide option `PROVISIONER.base_dn`.")
            sys.exit(1)
        provision_user = bool(config.get('provision_user', False))
        provision_group = bool(config.get('provision_group', False))
        if (not provision_user) and (not provision_group):
            log.error("Must provision enable at least one of 'PROVISIONER.provision_user' or `PROVISIONER.provision_group`.")
            sys.exit(1) 
        yield threads.deferToThread(self.init_db)
        processor = LoopingCall(self.process_requests)
        processor.start(self.batch_time)
        self.processor = processor

    def getConfigDefaults(self):
        return dedent("""\
        [PROVISIONER]
        sqlite_db = groups.db
        group_map = groupmap.json
        url = ldap://127.0.0.1:389/
        start_tls = 1
        provision_group = 1
        provision_user = 1
        group_attribute = member
        user_attribute = memberOf
        group_value_type = dn
        user_value_type = dn
        """)

    def init_db(self):
        """
        WARNING: Blocking call.
        """
        db_str = self.config['sqlite_db']
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

    def provision(self, group, subject, action):
        db_str = self.config['sqlite_db']
        d = threads.deferToThread(add_action_to_intake, group, action, subject, db_str)
        return d

    def process_requests(self):
        log = self.log
        log.debug("LDAP provisioner processing queued requests ...")
        service_state = self.service_state
        db_str = self.config['sqlite_db']
        ldap_info = self.config
        group_map = self.group_map

        def set_last_update(result, service_state):
            """
            Set the last-updated time on the service state.
            """
            service_state.last_update = datetime.datetime.today()
            log.debug("LDAP provisioner last process loop successful.")
            return result
            
        d = threads.deferToThread(self.blocking_process_requests, db_str, ldap_info, group_map)
        d.addCallback(set_last_update, service_state)
        #If there is an error, log it, but keep on looping.
        d.addErrback(self.log.failure, "Error while processing provisioning request(s): ")
        return d
    
    def group_to_ldap_group(self, g, group_map):
        result = group_map.get(g, None)
        if result is not None:
            result = result.lower()
        return result
        
    def blocking_process_requests(self, db_str, ldap_info, group_map):
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
        with sqlite3.connect(db_str) as db:
            # Transfer intake table to normalized batch tables.
            self.transfer_intake_to_batch(db)
            # Process the normalized batch.
            base_dn = ldap_info['base_dn']
            with self.connect_to_directory(ldap_info['url']) as lconn:
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
                    ldap_group = self.group_to_ldap_group(group, group_map)
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
                        group_dn = self.apply_changes_to_ldap_group(ldap_group, add_membs, del_membs, base_dn, lconn, ldap_info)
                        mapped_groups[ldap_group] = group_dn
                c = db.cursor()
                c.execute(subj_sql)
                for subject_id in list(r[0] for r in fetch_batch(c)): 
                    c = db.cursor()
                    c.execute(subj_add_sql, [subject_id])
                    add_membs = set(mapped_groups[self.group_to_ldap_group(r[0], group_map)] for r in fetch_batch(c))
                    c = db.cursor()
                    c.execute(subj_del_sql, [subject_id])
                    del_membs = set(mapped_groups[self.group_to_ldap_group(r[0], group_map)] for r in fetch_batch(c))
                    if len(add_membs) > 0 or len(del_membs) > 0:
                        #print "[DEBUG] Applying changes to subject {subject} ...".format(subject=subject_id)
                        #print "- Adds -"
                        #print '\n  '.join(sorted(add_membs))
                        #print "- Deletes -"
                        #print '\n  '.join(sorted(del_membs))
                        
                        self.apply_changes_to_ldap_subj(subject_id, add_membs, del_membs, base_dn, lconn, ldap_info)
                    
                sql = """DELETE FROM groups;"""
                c = db.cursor()
                c.execute(sql)
                sql = """DELETE FROM member_ops;"""
                c = db.cursor()
                c.execute(sql)
                db.commit()
                
    def transfer_intake_to_batch(self, db):
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
            groupid = self.get_group_id(group, db)
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

    @contextlib.contextmanager
    def connect_to_directory(self, url):
        conn = ldap.initialize(url)
        yield conn
        conn.unbind()
                
    def apply_changes_to_ldap_group(self, group, adds, deletes, base_dn, conn, ldap_info):
        provision_group = bool(ldap_info.get('provision_group', False))
        empty_dn = ldap_info.get("empty_dn", None)
        fq_adds = set(x[1] for x in self.load_subject_dns(adds, base_dn, conn))
        fq_deletes = set(x[1] for x in self.load_subject_dns(deletes, base_dn, conn))
        group_dn, attribs = self.lookup_group(group, base_dn, conn) 
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
        if provision_group:
            new_attribs = dict(member=members)
            ml = ldap.modlist.modifyModlist(attribs, new_attribs)
            try:
                conn.modify_s(group_dn, ml)
            except ldap.LDAPError as ex:
                self.log.error("Error while attempting to modify LDAP group: {0}".format(group_dn)) 
                raise
        return group_dn.lower()
        
    def apply_changes_to_ldap_subj(self, subject_id, fq_adds, fq_deletes, base_dn, conn, ldap_info):
        provision_user = bool(ldap_info.get('provision_user', False))
        user_attribute = ldap_info['user_attribute']
        subj_dns = list(self.load_subject_dns([subject_id], base_dn, conn))
        if len(subj_dns) == 0:
            self.log.warn("No DN found for subject ID '{0}.  Skipping ...'".format(subject_id))
            return
        assert not len(subj_dns) > 1, "Multiple DNs found for subject ID '{0}'".format(subject_id)
        subj_dn = subj_dns[0][1]
        membs = self.load_subject_memberships(subj_dn, conn)
        memb_set = set([m.lower() for m in membs])
        memb_set = memb_set.union(fq_adds)
        memb_set = memb_set - fq_deletes
        members = list(memb_set)
        members.sort()
        attribs = {user_attribute: membs}
        if provision_user:
            new_attribs = {user_attribute: members}
            ml = ldap.modlist.modifyModlist(attribs, new_attribs)
            try:
                conn.modify_s(subj_dn, ml)
            except ldap.LDAPError as ex:
                self.log.error("Error while attempting to modify LDAP subject: {0}".format(subj_dn)) 
                raise
        
    def load_subject_dns(self, subject_ids, base_dn, conn):
        for subject_id in subject_ids:
            fltr = "cn={0}".format(escape_fltr(subject_id))
            try:
                results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=['cn']) 
            except ldap.LDAPError as ex:
                self.log.error("Error while searching for LDAP subject: {0}".format(subject_id)) 
                raise
            for result in results:
                yield (subject_id, result[0].lower())

    def lookup_group(self, group_name, base_dn, conn, group_attrib='member'):
        fltr = "cn={0}".format(escape_fltr(group_name))
        try:
            results = conn.search_s(base_dn, ldap.SCOPE_SUBTREE, fltr, attrlist=[group_attrib]) 
        except ldap.LDAPError as ex:
            self.log.error("Error while searching for LDAP group: {0}".format(group_name)) 
            raise
        assert len(results) > 0, "Could not find group, '{0}'.".format(group_name)
        return results[0]

    def load_subject_memberships(self, dn, conn, user_attrib='memberOf'):
        try:
            results = conn.search_s(dn, ldap.SCOPE_BASE, attrlist=[user_attrib]) 
        except ldap.LDAPError as ex:
            self.log.error("Error while fetching memberships for LDAP subject: {0}".format(dn)) 
            raise
        result = results[0]
        return result[1].get(user_attrib, [])
        
    def get_group_id(self, group, db):
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
    

def load_group_map(gm):
    with open(gm, "r") as f:
        o = load(f)
    return o
        
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

def add_action_to_intake(group, action, member, db_str):
    """
    WARNING: Blocking call.
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

