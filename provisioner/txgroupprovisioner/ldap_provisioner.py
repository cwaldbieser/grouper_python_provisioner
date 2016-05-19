
from __future__ import print_function
from collections import Mapping, namedtuple
import contextlib
import datetime
import exceptions
from json import load, loads
import os
import os.path
from textwrap import dedent
from config import load_config, section2dict
import sqlite3
import sys
import urlparse
from jinja2 import Template
from ldaptor.protocols import pureldap
from ldaptor.protocols.ldap import ldapclient, ldapsyntax, ldapconnector
from ldaptor.protocols.ldap.distinguishedname import DistinguishedName, RelativeDistinguishedName
from ldaptor.protocols.ldap.distinguishedname import unescape as unescapeDN
from twisted.plugin import IPlugin
from zope.interface import implements
from twisted.enterprise import adbapi
from twisted.internet.defer import gatherResults, inlineCallbacks, returnValue
from twisted.internet import task, threads
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from txgroupprovisioner.interface import IProvisionerFactory, IProvisioner
from txgroupprovisioner import constants


LDAPGroupTarget = namedtuple("LDAPTargetGroup", ['group', 'create_group', 'create_context'])


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
    subject_id_attribute = 'uid'
    group_attrib_type = 'cn'
    subject_chunk_size = 500
    commit_batch_size = 50
    provision_user = 0
    provision_group = 0
    provisioner_state = None
    IDLE = 0
    PROCESSING_REQUESTS = 1
    MEMBERSHIP_SYNC = 2

    @inlineCallbacks
    def load_config(self, config_file, default_log_level, logObserverFactory):
        scp = load_config(config_file, defaults=self.getConfigDefaults())
        config = section2dict(scp, "PROVISIONER")
        self.config = config
        log_level = config.get('log_level', default_log_level)
        log = Logger(observer=logObserverFactory(log_level))
        self.log = log
        log.debug("Initialized logging for LDAP provisioner.", 
            event_type='init_provisioner_logging')
        self.load_group_map(config['group_map'])
        log.debug("Loaded group map for LDAP provisioner.", 
            event_type='loaded_provisioner_group_map')
        base_dn = config.get('base_dn', None)
        if base_dn is None:
            log.error("Must provide option `{section}:{option}`.", 
                event_type='provisioner_config_error', 
                section='PROVISIONER',
                option='base_dn')
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
        self.start_tls = bool(int(config.get('start_tls', 0)))
        provision_user = bool(int(config.get('provision_user', 0)))
        provision_group = bool(int(config.get('provision_group', 0)))
        if (not provision_user) and (not provision_group):
            log.error("Must provision enable at least one of 'PROVISIONER.provision_user' or `PROVISIONER.provision_group`.")
            sys.exit(1) 
        self.provision_user = provision_user
        self.provision_group = provision_group
        try:
            self.subject_chunk_size = int(config.get('subject_chunk_size', self.subject_chunk_size))
        except ValueError as ex:
            log.error("Provisioner `subject_chunk_size` must be a positive integer.")
            sys.exit(1)
        if self.subject_chunk_size < 1:
            log.error("Provisioner `subject_chunk_size` must be a positive integer.")
            sys.exit(1)
        log.debug(
            "Provisioner LDAP settings: host={ldap_host}, port={ldap_port}, "
            "start_tls={start_tls}, base_dn={base_dn}, "
            "group_attrib={group_attrib} user_attrib={user_attrib}", 
            ldap_host=self.ldap_host,
            ldap_port=self.ldap_port,
            start_tls=self.start_tls,
            base_dn=base_dn,
            group_attrib=self.group_attribute,
            user_attrib=self.user_attribute)
        self.batch_time = int(config['batch_interval'])
        db_str = config['sqlite_db']
        self.dbpool = adbapi.ConnectionPool("sqlite3", db_str, check_same_thread=False)
        log.debug("Initializing database ...",
            event_type='before_init_db')
        try:
            yield self.dbpool.runInteraction(self.init_db)
        except Exception as ex:
            log.error("Error initializing database: {error}",
                event_type='db_error',
                error=ex)
        else:
            log.debug("Database initialized ...",
                event_type='after_init_db')
        processor = LoopingCall(self.process_requests)
        processor.start(self.batch_time)
        self.processor = processor
        self.provisioner_state = self.IDLE

    def getConfigDefaults(self):
        return dedent("""\
        [PROVISIONER]
        sqlite_db = groups.db
        group_map = groupmap.json
        url = ldap://127.0.0.1:389/
        start_tls = 1
        provision_group = 0
        provision_user = 0
        group_attribute = member
        user_attribute = memberOf
        group_value_type = dn
        user_value_type = dn
        batch_interval = 30
        """)

    @inlineCallbacks
    def runDBCommand(self, cmd, params=None, is_query=True):
        log = self.log
        dbpool = self.dbpool
        cmd_str = cmd.replace("\n", " ")
        args = [cmd]
        if params is not None:
            args.append(params)
        try:
            if is_query:
                results = yield dbpool.runQuery(*args)
            else:
                results = yield dbpool.runOperation(*args)
        except Exception as ex:
            params_str = ''
            msg_parts = ['DB error: cmd={cmd}']
            if params is not None:
                msg_parts.append('params={params!r}')
            msg_parts.append('error={error}')
            msg = ', '.join(msg_parts)
            log.error(
                msg,
                error=str(ex),
                cmd=cmd_str,
                params=params)
            raise
        msg_parts = ["Ran DB command: "]
        result_count = None
        if is_query:
            msg_parts.append("result_count={result_count}, ")
            result_count = len(results)
        msg_parts.append("cmd={cmd}")
        msg = ''.join(msg_parts)
        log.debug(msg, cmd=cmd_str, result_count=result_count) 
        returnValue(results)

    def init_db(self, txn):
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
                txn.execute(sql)
            except sqlite3.OperationalError as ex:
                if not str(ex).endswith(" already exists"):
                    raise

    @inlineCallbacks
    def provision(self, msg):
        """
        Provision an entry based on an AMQP message.
        """
        log = self.log
        serialized = msg.content.body
        doc = loads(serialized)
        try:
            action = doc["action"]
            add_delete_actions = (
                constants.ACTION_ADD,
                constants.ACTION_DELETE,
            )
            if action == constants.ACTION_MEMBERSHIP_SYNC:
                subjects = doc["subjects"]
                group = doc["group"]
            elif action in add_delete_actions:
                group = doc["group"]
                subject = doc["subject"]
            else:
                log.warn(
                    "Unknown action '{action}'.  Discarding.  Message was:\n{msg}",
                    msg=serialized)
                returnValue(None)
        except KeyError as ex:
            log.warn("Invalid message received.  Discarding.  Message was {msg}.", 
                event_type="invalid_message_error",
                msg=serialized)
            returnValue(None)
        if action == constants.ACTION_MEMBERSHIP_SYNC:
            yield self.perform_membership_sync(group, subjects) 
        elif action in add_delete_actions:
            db_str = self.config['sqlite_db']
            try:
                yield self.add_action_to_intake(group, action, subject)
            except Exception as ex:
                log.failure("Error adding provision request to intake.")
                raise
            log.debug(
                "Recorded provision request.",
                event_type='provision_request_scheduled',
                group=group,
                subject=subject,
                action=action)

    @inlineCallbacks
    def perform_membership_sync(self, group, subjects):
        log = self.log
        log.debug("Initializing group membership sync.")
        delay_time = 10
        while self.provisioner_state != self.IDLE:
            log.debug("Normal operations in progress.  Delaying group sync.")
            yield delay(self.reactor, delay_time)
        self.provisioner_state = self.MEMBERSHIP_SYNC
        try:
            log.debug("Performing membership sync for group '{group}'.", 
                group=group)
            log.debug("{count} subjects provided for sync.",
                count=len(subjects))
            target = self.group_to_ldap_group(group)
            client = yield self.get_ldap_client()
            try:
                log.debug("Applying changes to the LDAP group ...")
                group_dn, members = yield self.apply_changes_to_ldap_group(
                    target=target, adds=subjects, deletes=None, client=client)
                log.debug("Applying changes to LDAP members ...")
                yield self.sync_members_to_ldap_group(client, str(group_dn), members)
            except Exception as ex:
                log.error("Fatal error:\n{error}", error=ex)
                raise
            finally:
                if client.connected:
                    client.unbind()
                log.debug("Disconnected from LDAP.")
        finally:
            self.provisioner_state = self.IDLE
            log.debug("Provisioner state set back to IDLE.")

    @inlineCallbacks
    def alter_ldap_membership(self, client, entry_dn, group_dn, delete=True):
        """
        Load the membership attribute for `entry`, add/remove the `group_dn`
        value, and save the entry.
        """
        log = self.log
        user_attribute = self.user_attribute
        entry = ldapsyntax.LDAPEntry(client, entry_dn)
        yield entry.fetch(user_attribute)
        groups = set([m.lower() for m in entry.get(user_attribute, [])])
        orig_group_size = len(groups)
        if delete:
            groups.discard(group_dn)
        else:
            groups.add(group_dn)
        groups = list(groups)
        groups.sort()
        new_group_size = len(groups)
        entry[user_attribute] = groups
        log.debug(
            "'{entry_dn}' group count went from {old_count} => {new_count}",
            entry_dn=entry_dn,
            old_count=orig_group_size,
            new_count=new_group_size)
        try:
            yield entry.commit()
        except Exception as ex:
            log.failure(
                "Error altering LDAP membership for entry {entry_dn}",
                entry_dn=entry_dn)
            raise
        returnValue(True)

    def check_results(self, result_list, entry_indicies):
        """
        Test the results in `result_list`.
        Return a list of the entry indicies for which the results indicate
        a failure.
        """
        failures = [entry_indicies[n]
            for n, result in enumerate(result_list)
                if not result]
        return failures

    @inlineCallbacks
    def sync_members_to_ldap_group(self, client, group_dn, ldap_members):
        """
        Retrieve all LDAP entries with a subject membership entry for
        `group_dn`.  Remove the entry for those members that don't
        appear in `ldap_members`.
        Add the membership to entries that do appear in `ldap_members`.
        """
        log = self.log
        provision_user = self.provision_user
        log.debug("`provision_user` == {provision_user}", provision_user=provision_user)
        if not provision_user:
            returnValue(None)
        user_attribute = self.user_attribute
        subject_chunk_size = self.subject_chunk_size
        commit_batch_size = self.commit_batch_size
        base_dn = self.base_dn
        log.debug("base_dn='{base_dn}', commit_batch_size={commit_batch_size}",
            base_dn=base_dn,
            subject_chunk_size=subject_chunk_size)
        # Fetch all entries that are members of the group DN.
        template_string = "({0}={1})".format(user_attribute, "{0}")
        fltr = template_string.format(escape_filter_chars(group_dn))
        o = ldapsyntax.LDAPEntry(client, base_dn)
        log.debug("Searching LDAP with base='{ldap_base}', filter='{ldap_filter}'",
            ldap_base=base_dn,
            ldap_filter=fltr,
            ldap_attributes=None)
        try:
            results = yield o.search(filterText=fltr, attributes=None) 
        except Exception as ex:
            log.error(
                "Error while searching for LDAP entries with {user_attribute}={group_dn}",
                user_attribute=user_attribute,
                group_dn=group_dn) 
            raise
        log.debug("{result_count} LDAP account entries have been retrieved.",
            result_count=len(results))
        # Remove the group from entries that aren't on the subject list.
        # If an entry is on the list, keep track of it so we don't need to
        # update it later.
        log.debug("Removing group DN from entries that are not in the subject list ...")
        batch = []
        entry_indices = []
        processed = set([])
        for n, entry in enumerate(results): 
            entry_dn = str(entry.dn).lower()
            log.debug("Processing entry number {n}, '{entry_dn}'.", n=n, entry_dn=entry_dn)
            if not (entry_dn in ldap_members):
                log.debug("Scheduling entry to have group DN removed ...") 
                batch.append(
                    self.alter_ldap_membership(
                        client, entry_dn, group_dn, delete=True))
                entry_indices.append(n)
                if len(batch) >= commit_batch_size:
                    log.debug("Committing scheduled batch ...")
                    commits = yield gatherResults(batch, consumeErrors=True)
                    failed = self.check_results(commits, entry_indicies)
                    if len(failed) > 0:
                        log.error("Batch update failed for the following entries: {failed}",
                            failed=failed)
                    log.debug("Committed batch.")
                    batch = []
                    entry_indices = []
            else:
                log.debug("Entry belongs in group.")
                processed.add(str(entry_dn))
        log.debug("Checkpoint A")
        if len(batch) > 0:
            log.debug("Committing scheduled batch ...")
            commits = yield gatherResults(batch, consumeErrors=False)
            failed = self.check_results(commits, entry_indicies)
            if len(failed) > 0:
                log.error("Batch update failed for the following entries: {failed}",
                    failed=failed)
            log.debug("Committed batch.")
        # Finally, update all the subject entries that ought to be one the list
        # but weren't.
        log.debug(
            "Count of already processed subjects => {processed_count}",
            processed_count=len(processed))
        ldap_members = set(ldap_members) - processed
        log.debug(
            "Count of members to process => {count}",
            count=len(ldap_members))
        log.debug("Adding group to members that are missing it ...")
        batch = []
        entry_indicies = []
        for n, entry_dn in enumerate(ldap_members):
            log.debug("Processing entry number {n}, '{entry_dn}'.", n=n, entry_dn=entry_dn)
            o = ldapsyntax.LDAPEntry(client, entry_dn)
            log.debug("Scheduling entry to have group DN added ...") 
            batch.append(
                self.alter_ldap_membership(
                    client, entry_dn, group_dn, delete=False))
            entry_indicies.append(n)
            if len(batch) >= commit_batch_size:
                log.debug("Committing scheduled batch ...")
                commits = yield gatherResults(batch, consumeErrors=False)            
                failed = self.check_results(commits, entry_indicies)
                if len(failed) > 0:
                    log.error("Batch update failed for the following entries: {failed}",
                        failed=failed)
                batch = []
        if len(batch) > 0:
            log.debug("Committing scheduled batch ...")
            commits = yield gatherResults(batch)            
            failed = self.check_results(commits, entry_indicies)
            if len(failed) > 0:
                log.error("Batch update failed for the following entries: {failed}",
                    failed=failed)
        log.debug("LDAP sync complete.")

    @inlineCallbacks
    def process_requests(self):
        """
        Process the requests recorded in the provisioner's internal database.
        """
        log = self.log
        if not self.provisioner_state == self.IDLE:
            returnValue(None)
        self.provisioner_state = self.PROCESSING_REQUESTS
        try:
            log.debug("LDAP provisioner processing queued requests ...",    
                event_type='provisioner_begin_process_requests')
            service_state = self.service_state
            try:
                yield self.provision_ldap()
            except Exception as ex:
                log.failure("Error while processing provisioning request(s)")
            else:
                service_state.last_update = datetime.datetime.today()
                log.debug("LDAP provisioner last process loop successful.")
        finally:
            self.provisioner_state = self.IDLE
    
    def group_to_ldap_group(self, g):
        """
        Return an `LDAPGroupTarget` named tuple from the full path to a
        Grouper group.
        """
        log = self.log
        log.debug(
            "Looking up group, '{group}' in groupmap ...",
            event_type='groupmap_lookup',
            group=g)
        group_map = self.group_map
        stem_map = self.stem_map
        result = group_map.get(g, None)
        if result is not None:
            ldap_group = result['group']
            create_group = result['create_group']
            create_context = result.get('create_context', None)
            log.debug(
                "Group '{group}' mapped to '{ldap_group}'",
                event_type='groupmap_match',
                group=g,
                ldap_group=ldap_group) 
            return LDAPGroupTarget(ldap_group, create_group, create_context)
        else:
            parts = g.split(':')
            stem_parts = parts[:-1]
            stem = ':'.join(stem_parts) + ':'
            group_only = parts[-1]
            log.debug(
                "Attempting to match stem, '{stem}' ...",
                event_type='stemmap_lookup',
                stem=stem)
            result = stem_map.get(stem, None)
            if result is not None:
                template = Template(result['template'])
                ldap_group = template.render(group=group_only, stem=stem, fqgroup=g)
                create_group = result['create_group']
                create_context = result.get('create_context', None)
                log.debug(
                    "Group '{group}' stem-mapped to '{ldap_group}'",
                    event_type='groupmap_match',
                    group=g,
                    ldap_group=ldap_group) 
                return LDAPGroupTarget(ldap_group, create_group, create_context)
            else:
                return None 
       
    @inlineCallbacks
    def get_ldap_client(self):
        """
        Returns a Deferred that fires with an asynchronous LDAP client.
        """
        log = self.log
        config = self.config
        base_dn = config['base_dn']
        start_tls = self.start_tls
        ldap_host = self.ldap_host
        ldap_port = self.ldap_port
        bind_dn = config.get('bind_dn', None)
        bind_passwd = config.get('passwd', None)
        c = ldapconnector.LDAPClientCreator(self.reactor, ldapclient.LDAPClient)
        overrides = {base_dn: (ldap_host, ldap_port)}
        client = yield c.connect(base_dn, overrides=overrides)
        try:
            log.debug(
                "LDAP client connected to server: host={ldap_host}, port={ldap_port}",
                event_type='ldap_connect',
                ldap_host=ldap_host,
                ldap_port=ldap_port)
            if start_tls:
                yield client.startTLS()
                log.debug("LDAP client initiated StartTLS.", event_type='ldap_starttls')
            if bind_dn and bind_passwd:
                yield client.bind(bind_dn, bind_passwd)
                log.debug(
                    "LDAP client BIND as '{bind_dn}'.",
                    event_type='ldap_bind',
                    bind_dn=bind_dn)
        except:
            if client.connected:
                client.unbind()
            raise
        returnValue(client)
 
    @inlineCallbacks
    def provision_ldap(self):
        log = self.log
        group_map = self.group_map
        # Transfer intake table to normalized batch tables.
        yield self.transfer_intake_to_batch()
        # Process the normalized batch.
        client = yield self.get_ldap_client()
        try:
            group_sql = "SELECT rowid, grp FROM groups ORDER BY grp ASC;"
            memb_add_sql = "SELECT member FROM member_ops WHERE grp = ? AND op = ? ORDER BY member ASC;" 
            memb_del_sql = "SELECT member FROM member_ops WHERE grp = ? AND op = ? ORDER BY member ASC;" 
            subj_sql = "SELECT DISTINCT member FROM member_ops ORDER BY member ASC;"
            subj_add_sql = dedent("""\
                SELECT DISTINCT groups.grp 
                FROM groups
                    INNER JOIN member_ops
                        ON groups.rowid = member_ops.grp
                WHERE member = ?
                AND op = ? 
                ORDER BY groups.grp ASC
                ;
                """)
            subj_del_sql = dedent("""\
                SELECT DISTINCT groups.grp 
                FROM groups
                    INNER JOIN member_ops
                        ON groups.rowid = member_ops.grp
                WHERE member = ?
                AND op = ?
                ORDER BY groups.grp ASC
                ;
                """)
            results = yield self.runDBCommand(group_sql)
            mapped_groups = {}
            for groupid, group in results:
                target = self.group_to_ldap_group(group)
                if target is None:
                    log.debug(
                        "Group '{group}' is not a targetted group.  Skipping ...", 
                        event_type='log',
                        group=group)
                    yield self.runDBCommand(
                        '''DELETE FROM member_ops WHERE grp = ?;''', [groupid], is_query=False)
                    yield self.runDBCommand(
                        '''DELETE FROM groups WHERE grp = ?;''', [group], is_query=False)
                    continue
                memb_add_results = yield self.runDBCommand(memb_add_sql, [groupid, constants.ACTION_ADD])
                add_membs = set([r[0] for r in memb_add_results])
                del memb_add_results
                memb_del_results = yield self.runDBCommand(memb_del_sql, [groupid, constants.ACTION_DELETE])
                del_membs = set([r[0] for r in memb_del_results])
                del memb_del_results
                if len(add_membs) > 0 or len(del_membs) > 0:
                    log.debug(
                        "Applying changes to group {group} ...", 
                        event_type='log',
                        group=target.group)
                    group_dn, ldap_group_members = yield self.apply_changes_to_ldap_group(
                        target, add_membs, del_membs, client)
                    log.debug(
                        "Applied changes to LDAP group {ldap_group}.",
                        event_type='ldap_group_change',
                        ldap_group=group_dn)
                    mapped_groups[target.group] = group_dn
            results = yield self.runDBCommand(subj_sql)
            for (subject_id,) in results: 
                add_results = yield self.runDBCommand(subj_add_sql, [subject_id, constants.ACTION_ADD])
                add_membs = [self.group_to_ldap_group(r[0]) for r in add_results]
                del add_results
                add_membs = set(mapped_groups[t.group] for t in add_membs if t is not None)
                del_results = yield self.runDBCommand(subj_del_sql, [subject_id, constants.ACTION_DELETE])
                del_membs = [self.group_to_ldap_group(r[0]) for r in del_results]
                del del_results
                del_membs = set(mapped_groups[t.group] for t in del_membs if t is not None)
                if len(add_membs) > 0 or len(del_membs) > 0:
                    log.debug(
                        "Applying changes to subject {subject} ...",
                        subject=subject_id)
                    yield self.apply_changes_to_ldap_subj(subject_id, add_membs, del_membs, client)
                    log.debug(
                        "Applied changes to LDAP subject '{subject_id}'.",
                        event_type='ldap_user_change',
                        subject_id=subject_id)
            sql = "DELETE FROM groups;"
            yield self.runDBCommand(sql, is_query=False)
            sql = "DELETE FROM member_ops;"
            yield self.runDBCommand(sql, is_query=False)
        finally:
            if client.connected:
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
        log = self.log
        sql = dedent("""\
            SELECT rowid, grp, member, op
            FROM intake
            ORDER BY rowid ASC
            ;
            """)
        intake = yield self.runDBCommand(sql)
        for rowid, group, member, action in intake:
            groupid = yield self.get_group_id(group)
            sql = dedent("""\
                SELECT op, member
                FROM member_ops 
                WHERE grp = ?
                AND member = ?
                ;
                """)
            results = yield self.runDBCommand(sql, [groupid, member])
            if len(results) == 0:
                sql = "INSERT INTO member_ops(op, member, grp) VALUES(?, ?, ?);"
                yield self.runDBCommand(sql, [action, member, groupid], is_query=False)
            else:
                result = results[0]
                sql = "UPDATE member_ops SET op = ? WHERE grp=? AND member=?;"
                yield self.runDBCommand(sql, [action, groupid, member], is_query=False)
            sql = "DELETE FROM intake WHERE rowid = ? ;"
            yield self.runDBCommand(sql, [rowid], is_query=False)

    @inlineCallbacks
    def xform_subjects_to_members(self, subjects, client):
        """
        Transforms subjects to LDAP DNs.
        Returns a Deferred that fires with the LDAP DNs.
        """
        log = self.log
        log.debug("Transforming subjects to members ...")
        results = []
        subject_id_attribute = self.subject_id_attribute
        chunk_size = self.subject_chunk_size
        q = list(subjects)
        log.debug("{q_size} subjects to transform.", q_size=len(q))
        while len(q) > 0:
            chunk = q[:chunk_size]
            q = q[chunk_size:]
            log.debug(
                "Transforming {chunk_size} subjects, {q_size} remaining.",
                chunk_size=chunk_size,
                q_size=len(q))
            lst =  yield self.load_subjects(set(chunk), client, attribs=None)
            results.extend(lst)
            log.debug("{result_count} results have been accumulated.", result_count=len(results))
        fq_subjects = set(str(x[1].dn).lower() for x in results)
        returnValue(fq_subjects)

    @inlineCallbacks 
    def apply_changes_to_ldap_group(self, target, adds, deletes, client):
        """
        Applies subject `adds` and `deletes` to an existing LDAP group's membership.
        Alternatively. if `adds` and `deletes` are both None, `replacements` may be an
        iterable of subjects used to replace the LDAP group's entire membership.

        :param:`target`: An `LDAPGroupTarget` named tuple.
        :param:`adds`: An iterable of subject IDs to add to the membership.
        :param:`deletes`: An iterable of subject IDs to delete from the 
        membership.  If `deletes` is set to None, all the members not in `adds`
        will be removed from the group.
        :param:`client`: The Ldaptor client.

        :returns: A Deferred that fires with a tuple of (the `DistinguishedName`
        of the LDAP group entry, a list of the LDAP member values) or None if
        the group does not exist and cannot be created due to configuration
        settings.
        """
        log = self.log
        config = self.config
        group_attribute = self.group_attribute
        provision_group = self.provision_group
        empty_dn = config.get("empty_dn", None)
        group_attrib_type = self.group_attrib_type
        chunk_size = self.subject_chunk_size
        log.debug("Transforming subject additions to LDAP members.")
        fq_adds = yield self.xform_subjects_to_members(adds, client)
        if deletes is not None:
            log.debug("Transforming subject deletions to LDAP members.")
            fq_deletes = yield self.xform_subjects_to_members(deletes, client)
        else:
            fq_deletes = set([])
        log.debug("Looking up LDAP group ...")
        group_entry = yield self.lookup_group(target.group, client) 
        log.debug("Looked up LDAP group.")
        needs_create = False
        if group_entry is None:
            if target.create_group:
                log.debug(
                    "Creating LDAP group {ldap_group},{ldap_context} ...",
                    event_type='create_ldap_group',
                    ldap_group=target.group,
                    ldap_context=target.create_context)
                memb_set = set([])
                needs_create = True
            else: 
                returnValue(None) 
        elif deletes is not None:
            memb_set = set([m.lower() for m in group_entry[group_attribute]])
        else:
            memb_set = set([])
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
            if needs_create:
                log.debug("Creating LDAP group ...")
                o = ldapsyntax.LDAPEntry(client, target.create_context)
                attribs = {
                    'objectClass': ['top', 'groupOfNames'],
                    'member': members}
                rdn = RelativeDistinguishedName("{0}={1}".format(group_attrib_type, target.group))
                try:
                    group_entry = yield o.addChild(rdn, attribs)
                except Exception as ex:
                    log.error(
                        "Error while trying create LDAP group '{rdn},{ldap_context}'.",
                        event_type='ldap_error',
                        rdn=str(rdn),
                        ldap_context=target.create_context)
                    raise
            log.debug("Modifying LDAP group.")
            try:
                group_entry[group_attribute] = members
                yield group_entry.commit()
            except Exception as ex:
                log.error(
                    "Error while attempting to modify LDAP group: {dn}", 
                    event_type='ldap_error',
                    dn=str(group_entry.dn)) 
                raise
            log.debug("LDAP group modified.")
        returnValue((normalize_dn(group_entry.dn), members))
       
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
            returnValue(None)
        assert not len(subjects) > 1, "Multiple DNs found for subject ID '{0}'".format(subject_id)
        subject_id, subject_entry = subjects[0]
        membs = subject_entry.get(user_attribute, [])
        memb_set = set([normalize_dn(DistinguishedName(m)) for m in membs])
        memb_set = memb_set.union(fq_adds)
        memb_set = memb_set - fq_deletes
        members = list(memb_set)
        members.sort()
        try:
            if len(members) == 0:
                if user_attribute in subject_entry:
                    del subject_entry[user_attribute]
            else:
                subject_entry[user_attribute] = members
            yield subject_entry.commit()    
        except Exception as ex:
            self.log.error(
                "Error while attempting to modify LDAP subject: dn={dn}, attribs={attribs}",
                    event_type='ldap_error',
                    dn=subject_entry.dn,
                    attribs=subject_entry.items()) 
            raise
       
    @inlineCallbacks 
    def load_subjects(self, subject_ids, client, attribs=()):
        base_dn = self.base_dn
        rval = []
        dlist = []
        subject_id_attribute = self.subject_id_attribute
        template_string = "({0}={1})".format(subject_id_attribute, "{0}")
        for subject_id in subject_ids:
            fltr = template_string.format(escape_filter_chars(subject_id))
            o = ldapsyntax.LDAPEntry(client, base_dn)
            dlist.append(o.search(filterText=fltr, attributes=attribs))
        try:
            results = yield gatherResults(dlist) 
        except Exception as ex:
            self.log.error(
                "Error while searching for LDAP subjects", 
                event_type='error_load_ldap_subjects') 
            raise
        for subject_id, resultset in zip(subject_ids, results):
            for result in resultset:
                rval.append((subject_id, result))
        returnValue(rval)

    @inlineCallbacks
    def lookup_group(self, group_name, client):
        group_attrib_type = self.group_attrib_type
        base_dn = self.base_dn
        group_attrib = self.group_attribute
        fltr = "({0}={1})".format(group_attrib_type, escape_filter_chars(group_name))
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
        sql = "SELECT rowid FROM groups WHERE grp = ?;"
        result = yield self.runDBCommand(sql, [group])

        def interaction(txn, sql, group):
            txn.execute(sql, [group])
            return txn.lastrowid

        if result is None or len(result) == 0:
            sql = "INSERT INTO groups (grp) VALUES (?);"
            group_id = yield dbpool.runInteraction(interaction, sql, group)
            returnValue(group_id)
        else:
            returnValue(result[0][0])

    @inlineCallbacks
    def add_action_to_intake(self, group, action, member):
        sql = "INSERT INTO intake(grp, member, op) VALUES(?, ?, ?);"
        yield self.runDBCommand(sql, [group, member, action], is_query=False)
        self.log.debug(
            "Added provision request to intake: group={group}, subject={subject}, action={action}",
            event_type='request_to_intake',
            group=group,
            subject=member,
            action=action)
    

    def load_group_map(self, gm):
        log = self.log
        try:
            with open(gm, "r") as f:
                try:
                    o = load(f)
                except Exception as ex:
                    log.failure("Error reading group mapping.")
                    d = self.reactor.callLater(0, self.reactor.stop)
                    raise
        except exceptions.IOError as ex:
            log.failure("Error opening group map file '{0}': {1}".format(gm, ex))
            d = self.reactor.callLater(0, self.reactor.stop)
            raise 
        direct_map = {}
        stem_map = {}
        for group, value in o.iteritems():
            is_folder = group.endswith(":")
            if not is_folder:
                if isinstance(value, basestring):
                    direct_map[group] = {'group': value, 'create_group': False}
                elif isinstance(value, Mapping):
                    if not 'group' in value:
                        log.warn(
                            "Group mapping for '{group}' does not have a valid target (no 'group' option).",
                            event_type='groupmap_parse_error',
                            group=group)
                        continue
                    ldap_group = value['group']
                    if not isinstance(ldap_group, basestring):
                        log.warn(
                            "Group mapping for '{group}' does not have a valid target ('group' must be a name).",
                            event_type='groupmap_parse_error',
                            group=group)
                        continue
                    create_group = bool(value.get('create_group', False))
                    create_context = value.get('create_context', None)
                    if create_context is None and create_group:
                        log.warn(
                            "Create group option is set for group '{group}' but no 'create_context' was specified.",
                            event_type='groupmap_parse_error',
                            group=group)
                        create_group = False
                    props = {'group': ldap_group, 'create_group': create_group}
                    if create_group:
                        props['create_context'] = create_context
                    direct_map[group] = props
                else:
                    log.warn(
                        "Invalid target for group mapping '{group}'.",
                        event_type='groupmap_parse_error',
                        group=group)
                    continue
            else:
                if not isinstance(value, Mapping):
                    log.warn(
                        "Invalid target for stem '{stem}'.",
                        event_type='groupmap_parse_error',
                        stem=group)
                    continue
                template = value.get('template', None)
                create_group = bool(value.get('create_group', False))
                create_context = value.get('create_context', None)
                if template is None:
                    log.warn(
                        "Invalid target for stem mapping '{stem}' (no 'template' option).",
                        event_type='groupmap_parse_error',
                        stem=group)
                    continue
                if not isinstance(template, basestring): 
                    log.warn(
                        "Invalid target for stem mapping '{stem}' ('template' must be string-like).",
                        event_type='groupmap_parse_error',
                        stem=group)
                    continue
                if create_context is None and create_group:
                    log.warn(
                        "Create group option is set for stem '{stem}' but no 'create_context' was specified.",
                        event_type='groupmap_parse_error',
                        stem=group)
                    create_group = False
                props = {'template': template, 'create_group': create_group}
                if create_group:
                    props['create_context'] = create_context
                stem_map[group] = props
            self.group_map = direct_map
            self.stem_map = stem_map
        log.debug(
            "Created group maps: group_map={group_map!r} stem_map={stem_map!r}",
            event_type='groupmap_parsed',
            group_map=direct_map,
            stem_map=stem_map)
        
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

def normalize_dn(dn):
    """
    Transforms a `DistinguishedName` into a `DistinguishedName` with the casing normalized.
    """
    return DistinguishedName(tuple(RelativeDistinguishedName(str(r).lower()) for r in dn.listOfRDNs))

@inlineCallbacks            
def delay(reactor, seconds):
    """
    A Deferred that fires after `seconds` seconds.
    """
    yield task.deferLater(reactor, seconds, lambda x: None)

