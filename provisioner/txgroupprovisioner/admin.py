
from twisted.cred.portal import IRealm
from twisted.conch.interfaces import IConchUser
from zope.interface import implements
from txsshadmin.cred_base import SSHBaseAvatar, SSHBaseRealm
from txsshadmin.proto_dispatcher import (
    makeSSHDispatcherProtocolFactory,
    BaseHandler)
from txsshadmin.service import SSHServiceBase
import grp
import pwd


class DelegatingHandler(BaseHandler):
    def onConnect(self, dispatcher):
        self.avatar.onConnect(dispatcher)

    def handle_start(self, dispatcher):
        """
        Consume messages and pass them on to the provisioner.
        """
        terminal = dispatcher.terminal
        self.avatar.groupService.startAMQPMessageLoop()
        terminal.write("Starting to process messages.")
        terminal.nextLine()

    def handle_stop(self, dispatcher):
        """
        Stop reading AMQP messages.
        """
        terminal = dispatcher.terminal
        self.avatar.groupService.stopAMQPMessageLoop()
        terminal.write("Stopped reading messages from the queue.")
        terminal.nextLine()

    def handle_status(self, dispatcher):
        """
        Report on processing status.
        """ 
        terminal = dispatcher.terminal
        service_state = self.avatar.groupService.service_state
        running = service_state.read_from_queue
        terminal.write("Processing messages: {0}".format(running))
        terminal.nextLine()
        last_update = service_state.last_update
        terminal.write("Last successful provisioning attempt: {0}".format(
            last_update.strftime("%Y-%m-%dT%H:%M:%S")))
        terminal.nextLine()

    def handle_shutdown(self, dispatcher):
        """
        Shutdown this service.
        """
        terminal = dispatcher.terminal
        terminal.write("Shutting down ...")
        terminal.nextLine()
        from twisted.internet import reactor
        reactor.stop()

    def handle_show_config(self, dispatcher):
        """
        Show the current configuration settings.
        """
        terminal = dispatcher.terminal
        scp = self.avatar.groupService.scp
        sections = scp.sections()
        sections.sort()
        terminal.write("## Current Settings ##")
        terminal.nextLine()
        for section in sections:
            options = scp.options(section)
            options.sort()
            for option in options:
                value = scp.get(section, option)
                if 'passwd' in option or 'password' in option:
                    value = '*******'
                terminal.write("{section}.{option}: {value}".format(
                    section=section,
                    option=option,
                    value=value))
                terminal.nextLine()

    def handle_loglevel(self, dispatcher, logname=None, level=None):
        """
        Set log levels.
        Usage: loglevel <LOGNAME> <LEVEL>
        Valid logs: root, amqp, provisioner
        Valid levels: debug, info, warn, error, critical
        """
        terminal = dispatcher.terminal
        groupService = self.avatar.groupService
        logObserverFactory = groupService.logObserverFactory
        logs = {
            'root': groupService.log,
            'amqp': groupService.amqp_log,
            'provisioner': groupService.provisioner.log}
        levels = set(['debug', 'info', 'warn', 'error', 'critical'])
        if logname is not None and level is not None:
            if level in levels:
                log = logs.get(logname, None)
                if log is not None:
                    log.observer = logObserverFactory(level)
                else:
                    terminal.write("Invalid log, '{0}'.".format(logname))
                    terminal.nextLine()
            else:
                terminal.write("Invalid log level, '{0}'.".format(level))
                terminal.nextLine()
        else:
            terminal.write("Usage: loglevel <LOGNAME> <LEVEL>")
            terminal.nextLine()


SSHDelegatingProtocolFactory = makeSSHDispatcherProtocolFactory(DelegatingHandler)


class SSHUnauthorizedAvatar(SSHBaseAvatar):
    protocolFactory = SSHDelegatingProtocolFactory

    def __init__(self, avatarId):
        SSHBaseAvatar.__init__(self, avatarId)

    def onConnect(self, dispatcher):
        terminal = dispatcher.terminal
        terminal.write(
            "You are not authorized to use this service.")
        #TODO: Log unauthorized access attempt.
        terminal.nextLine()
        terminal.loseConnection() 

class SSHAdminAvatar(SSHBaseAvatar):
    protocolFactory = SSHDelegatingProtocolFactory
    groupService = None

    def __init__(self, avatarId):
        SSHBaseAvatar.__init__(self, avatarId)

    def onConnect(self, dispatcher):
        terminal = dispatcher.terminal
        terminal.write(
            "Welcome to the admin interface, {0}.".format(self.avatarId))
        #TODO: Log unauthorized access attempt.
        terminal.nextLine()


class SSHAdminRealm(object):
    implements(IRealm)
   
    adminGroup = 'txgroupadmins' 
    avatarFactory = SSHAdminAvatar
    groupService = None

    def requestAvatar(self, avatarId, mind, *interfaces):
        if IConchUser in interfaces:
            log = self.groupService.log
            avatar = SSHUnauthorizedAvatar(avatarId)
            adminGroup = self.adminGroup
            try:
                user_info = pwd.getpwnam(avatarId)
            except KeyError:
                log.debug("Could not find user info for '{avatarId}'.",
                    avatarId=avatarId) 
            else:
                gid = user_info.pw_gid
                try:
                    grp_info = grp.getgrgid(gid)
                except KeyError:
                    log.debug("Could not find group info for {avatarId} primary group '{gid}'.",
                        avatarId=avatarId, gid=gid) 
                else:
                    group_name = grp_info.gr_name
                    is_admin = False
                    if group_name == adminGroup:
                        is_admin = True
                    else:
                        log.debug((
                            "Avatar {avatarId} primary group {group} "
                            "is not admin group {admin_group}."),
                            avatarId=avatarId,
                            group=group_name,
                            admin_group=adminGroup)
                        try:
                            grp_info = grp.getgrnam(adminGroup)
                        except KeyError:
                            log.debug("Could not find group info for '{admin_group}'.",
                                admin_group=adminGroup) 
                        else:
                            members = set(grp_info.gr_mem)
                            if avatarId in members:
                                is_admin = True
                            else:
                                log.debug("Avatar '{avatarId}' not a member of '{adminGroup}'.",
                                    avatarId=avatarId,
                                    adminGroup=adminGroup)
                    if is_admin:
                        log.info(
                            "Avatar ID {avatarId} successfully logged onto admin service.", 
                            avatarId=avatarId)
                        avatar = SSHAdminAvatar(avatarId)
                        avatar.groupService = self.groupService
                    else:
                        avatar = SSHUnauthorizedAvatar(avatarId)
                        log.info(
                            "Avatar ID {avatarId} attempted unauthorized access to the admin service.", 
                            avatarId=avatarId)
            return IConchUser, avatar, lambda: None
        else:
            raise Exception("No supported interfaces found.")

class SSHAdminService(SSHServiceBase):
    realm = SSHAdminRealm()

