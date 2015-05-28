
from ldaptor.protocols.ldap import ldapclient, ldapsyntax, ldapconnector
from ldaptor.protocols import pureldap
from twisted.plugin import IPlugin
from zope.interface import implements
from twisted.enterprise import adbapi
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor, task, threads
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from txgroupprovisioner.interface import IProvisionerFactory, IProvisioner
import contextlib
import datetime
from json import load
import os
import os.path
from textwrap import dedent
from config import load_config, section2dict
from logging import make_syslog_observer
import urlparse

class LDAPProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)

    tag = "ldap"
    opt_help = "Provisions an LDAP DIT with group membership changes."
    opt_usage = "This plugin does not support any options."
    subject_id_attribute = 'uid'
    group_id_attribute = 'cn'

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
    batch_time = 30

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
        log.debug("Initialized logging for LDAP provisioner.")
        group_map = load_group_map(config['group_map'])
        log.debug("Loaded group map for LDAP provisioner.")
        self.group_map = group_map
        base_dn = config.get('base_dn', None)
        if base_dn is None:
            log.error("Must provide option `PROVISIONER.base_dn`.")
            sys.exit(1)
        self.base_dn = base_dn
        self.group_attribute = config['group_attribute']
        self.user_attribute = config['user_attribute']
        ldap_url = config['url']
        p = urlparse.urlparse(ldap_url)
        netloc = p.netloc
        host_port = netloc.split(':', 1)
        self.ldap_host = host_port[0]
        if len(host_port) > 1:
            self.ldap_port = int(host_port[1])
        else:
            self.ldap_port = 389
        log.debug(
            "LDAP host: {ldap_host}, LDAP port: {ldap_port}", 
            ldap_host=self.ldap_host,
            ldap_port=self.ldap_port)
        self.start_tls = bool(int(config.get('start_tls', 0)))
        provision_user = bool(int(config.get('provision_user', 0)))
        provision_group = bool(int(config.get('provision_group', 0)))
        if (not provision_user) and (not provision_group):
            log.error("Must provision enable at least one of 'PROVISIONER.provision_user' or `PROVISIONER.provision_group`.")
            sys.exit(1) 
        self.provision_user = provision_user
        self.provision_group = provision_group
        self.batch_time = int(config['batch_interval'])
        db_str = config['sqlite_db']
        self.dbpool = adbapi.ConnectionPool("sqlite3", db_str, check_same_thread=False)
        yield self.init_db
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
        batch_interval = 30
        """)

    @inlineCallbacks
    def init_db(self):
        dbpool = self.dbpool
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
                yield dbpool.runOperation(sql)
            except sqlite3.OperationalError as ex:
                if not str(ex).endswith(" already exists"):
                    raise

    def provision(self, group, subject, action):
        db_str = self.config['sqlite_db']
        d = yield self.add_action_to_intake(group, action, subject)

    def process_requests(self):
        log = self.log
        log.debug("LDAP provisioner processing queued requests ...")
        service_state = self.service_state

        def set_last_update(result, service_state):
            """
            Set the last-updated time on the service state.
            """
            service_state.last_update = datetime.datetime.today()
            log.debug("LDAP provisioner last process loop successful.")
            return result
            
        d = self.provision_ldap()
        d.addCallback(set_last_update, service_state)
        #If there is an error, log it, but keep on looping.
        d.addErrback(self.log.failure, "Error while processing provisioning request(s): ")
        return d
    
    def group_to_ldap_group(self, g, group_map):
        result = group_map.get(g, None)
        if result is not None:
            result = result.lower()
        return result
        
    @inlineCallbacks
    def provision_ldap(self):
        log = self.log
        group_map = self.group_map
        config = self.config
        dbpool = self.dbpool
        # Transfer intake table to normalized batch tables.
        yield self.transfer_intake_to_batch()
        # Process the normalized batch.
        base_dn = config['base_dn']
        start_tls = self.start_tls
        ldap_host = self.ldap_host
        ldap_port = self.ldap_port
        bind_dn = config.get('bind_dn', None)
        bind_passwd = config.get('passwd', None)
        c = ldapconnector.LDAPClientCreator(reactor, ldapclient.LDAPClient)
        overrides = {base_dn: (ldap_host, ldap_port)}
        client = yield c.connect(base_dn, overrides=overrides)
        try:
            if start_tls:
                yield client.startTLS()
            if bind_dn and bind_passwd:
                yield client.bind(bind_dn, bind_passwd)
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
            results = yield dbpool.runQuery(group_sql)
            mapped_groups = {}
            for groupid, group in results:
                ldap_group = self.group_to_ldap_group(group, group_map)
                if ldap_group is None:
                    log.debug("Group '{group}' is not a target group.  Skipping ...", group=ldap_group)
                    yield dbpool.runOperation("""DELETE FROM member_ops WHERE grp = ?;""", [groupid])
                    yield dbpool.runOperation("""DELETE FROM groups WHERE grp = ?;""", [group])
                    continue
                results = yield dbpool.runQuery(memb_add_sql, [groupid])
                add_membs = set([r[0] for r in results])
                results = yield dbpool.runQuery(memb_del_sql, [groupid])
                del_membs = set([r[0] for r in results])
                if len(add_membs) > 0 or len(del_membs) > 0:
                    log.debug("Applying changes to group {group} ...", group=ldap_group)
                    group_dn = yield self.apply_changes_to_ldap_group(
                        ldap_group, add_membs, del_membs, client)
                    mapped_groups[ldap_group] = group_dn
            results = yield dbpool.runQuery(subj_sql)
            for subject_id in results: 
                add_results = yield dbpool.runQuery(subj_add_sql, [subject_id])
                add_membs = set(mapped_groups[self.group_to_ldap_group(r[0], group_map)] 
                    for r in add_results)
                del_results = yield dbpool.runQuery(subj_del_sql, [subject_id])
                del_membs = set(mapped_groups[self.group_to_ldap_group(r[0], group_map)] 
                    for r in fetch_batch(c))
                if len(add_membs) > 0 or len(del_membs) > 0:
                    log.debug(
                        "Applying changes to subject {subject} ...",
                        subject=subject_id)
                    yield self.apply_changes_to_ldap_subj(
                        subject_id, add_membs, del_membs, client)
                
            sql = """DELETE FROM groups;"""
            yield dbpool.runOperation(sql)
            sql = """DELETE FROM member_ops;"""
            yield dbpool.runOperation(sql)
        finally:
            client.unbind()
                
    @inlineCallbacks
    def transfer_intake_to_batch(self):
        """
        Transfer the intake table to the batch tables.
        This algorithm depends on the behavior of the SQLite3 ROWID-- specifically
        its properties relating to monotomically increasing values, and its reset
        to 1 if the table is empty.
        
        Ref: https://www.sqlite.org/autoinc.html
        """
        dbpool = self.dbpool
        sql = dedent("""\
            SELECT rowid, grp, member, op
            FROM intake
            ORDER BY rowid ASC
            ;
            """)
        intake = yield dbpool.runQuery(sql)
        for rowid, group, member, action in intake:
            groupid = self.get_group_id(group)
            sql = dedent("""\
                SELECT op, member
                FROM member_ops 
                WHERE grp = ?
                AND member = ?
                ;
                """)
            results = yield dbpool.runQuery(sql, [groupid, member])
            if len(results) == 0:
                sql = """INSERT INTO member_ops(op, member, grp) VALUES(?, ?, ?);"""
                yield dbpool.runOperation(sql, [action, member, groupid])
            else:
                result = results[0]
                sql = """UPDATE member_ops SET op = ? WHERE grp=? AND member=?;"""
                yield dbpool.runOperation(sql, [action, groupid, member])
            sql = """DELETE FROM intake WHERE rowid = ? ;"""
            yield dbpool.runOperation(sql, [rowid])

    @inlineCallbacks 
    def apply_changes_to_ldap_group(self, group, adds, deletes, client):
        subject_id_attribute = self.subject_id_attribute
        base_dn = self.base_dn
        group_attribute = self.group_attribute
        provision_group = self.provision_group
        empty_dn = ldap_info.get("empty_dn", None)
        results = yield self.load_subject(adds, client, attribs=[subject_id_attribute])
        fq_adds = set(x[1].dn.lower() for x in results)
        results = yield self.load_subject(deletes, client, attribs=[subject_id_attribute])
        fq_deletes = set(x[1].dn.lower() for x in results)
        group_entry = yield self.lookup_group(group, client) 
        if group_entry is None:
            returnValue(None) 
        memb_set = set([m.lower() for m in group_entry[group_attribute]])
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
            try:
                group_entry[group_attribute] = members
                yield group_entry.commit()
            except Exception as ex:
                self.log.error("Error while attempting to modify LDAP group: {group}", group=group_dn) 
                raise
        returnValue(group_enty.dn.lower())
       
    @inlineCallbacks 
    def apply_changes_to_ldap_subj(self, subject_id, fq_adds, fq_deletes, client):
        provision_user = self.provision_user
        if not provision_user:
            returnValue(None)
        base_dn = self.base_dn
        user_attribute = self.user_attribute
        subjects = yield self.load_subjects([subject_id], client, attribs=[user_attribute])
        if len(subjects) == 0:
            self.log.warn(
                "No DN found for subject ID '{subject_id}.  Skipping ...'",
                subject_id=subject_id)
            returValue(None)
        assert not len(subjects) > 1, "Multiple DNs found for subject ID '{0}'".format(subject_id)
        subject_entry = subjects[0]
        membs = subject_entry[user_attribute]
        memb_set = set([m.lower() for m in membs])
        memb_set = memb_set.union(fq_adds)
        memb_set = memb_set - fq_deletes
        members = list(memb_set)
        members.sort()
        try:
            subject_entry[user_attribute] = members
            yield subject_entry.commit()    
        except ldap.LDAPError as ex:
            self.log.error("Error while attempting to modify LDAP subject: {0}".format(subj_dn)) 
            raise
       
    @inlineCallbacks 
    def load_subjects(self, subject_ids, client, attribs=()):
        base_dn = self.base_dn
        rval = []
        for subject_id in subject_ids:
            fltr = "uid={0}".format(escape_filter_chars(subject_id))
            o = ldapsyntax.LDAPEntry(client, base_dn)
            try:
                results = yield o.search(filterText=query, attributes=attribs) 
            except Exception as ex:
                self.log.error(
                    "Error while searching for LDAP subject: {subject_id}", 
                    subject_id=subject_id) 
                raise
            for result in results:
                rval.append((subject_id, result))
        returnValue(rval)

    @inlineCallbacks
    def lookup_group(self, group_name, client):
        group_id_attribute = self.group_id_attribute
        base_dn = self.base_dn
        group_attrib = self.group_attribute
        fltr = "{0}={1}".format(group_id_attribute, escape_filter_chars(group_name))
        o = ldapsyntax.LDAPEntry(client, base_dn)
        try:
            results = yield o.search(filterText=fltr, attributes=[group_attrib]) 
        except Exception as ex:
            self.log.error(
                "Error while searching for LDAP group: {group}",
                group=group_name) 
            raise
        if len(results) == 0:
            self.log.warn("Could not find group, '{group}'.", group=group_name)
            returnValue(None)
        else:
            returnValue(results[0])

    @inlineCallbacks        
    def get_group_id(self, group):
        dbpool = self.dbpool
        sql = """SELECT rowid FROM groups WHERE grp = ?;"""
        result = yield dbpool.runQuery(sql, [group])
        if result is None:
            sql = """INSERT INTO groups (grp) VALUES (?);"""
            yield dbpool.runOperation(sql, [group])
            group_id = dbpool.lastrowid
            returnValue(group_id)
        else:
            returnValue(result[0])

    @inlineCallbacks
    def add_action_to_intake(group, action, member):
        dbpool = self.dbpool
        sql = """INSERT INTO intake(grp, member, op) VALUES(?, ?, ?);"""
        yield dbpool.runOperation(sql, [group, member, action])
    

def load_group_map(gm):
    with open(gm, "r") as f:
        o = load(f)
    return o
        
def escape_filter_chars(assertion_value,escape_mode=0):
    """
    This function shamelessly copied from python-ldap module.
    
    Replace all special characters found in assertion_value
    by quoted notation.

    escape_mode
      If 0 only special chars mentioned in RFC 2254 are escaped.
      If 1 all NON-ASCII chars are escaped.
      If 2 all chars are escaped.
    """
    if escape_mode:
        r = []
        if escape_mode==1:
            for c in assertion_value:
                if c < '0' or c > 'z' or c in "\\*()":
                    c = "\\%02x" % ord(c)
                r.append(c)
        elif escape_mode==2:
            for c in assertion_value:
                r.append("\\%02x" % ord(c))
        else:
          raise ValueError('escape_mode must be 0, 1 or 2.')
        s = ''.join(r)
    else:
        s = assertion_value.replace('\\', r'\5c')
        s = s.replace(r'*', r'\2a')
        s = s.replace(r'(', r'\28')
        s = s.replace(r')', r'\29')
        s = s.replace('\x00', r'\00')
    return s

