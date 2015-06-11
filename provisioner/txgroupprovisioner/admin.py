
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

    def handle_start(self, dispatcher, target=None):
        terminal = dispatcher.terminal
        if target == 'queue':
            self.avatar.groupService.startAMQPMessageLoop()
            terminal.write("Starting to read from message queue.")
            terminal.nextLine()
        elif target is None: 
            terminal.write("Usage: start queue")
            terminal.nextLine()
        else:
            terminal.write("Unrecognized target, '{0}'".format(target))
            terminal.nextLine()

    def handle_stop(self, dispatcher, target=None):
        terminal = dispatcher.terminal
        if target == 'queue':
            self.avatar.groupService.stopAMQPMessageLoop()
            terminal.write("Stopped reading from message queue.")
            terminal.nextLine()
        elif target is None: 
            terminal.write("Usage: stop queue")
            terminal.nextLine()
        else:
            terminal.write("Unrecognized target, '{0}'".format(target))
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
            avatar = SSHUnauthorizedAvatar(avatarId)
            adminGroup = self.adminGroup
            try:
                user_info = pwd.getpwnam(avatarId)
            except KeyError:
                pass
            else:
                gid = user_info.pw_gid
                try:
                    grp_info = grp.getgrgid(gid)
                except KeyError:
                    pass
                else:
                    group_name = grp_info.gr_name
                    is_admin = False
                    if group_name == adminGroup:
                        is_admin = True
                    else:
                        try:
                            grp_info = grp.getgrnam(adminGroup)
                        except KeyError:
                            pass
                        else:
                            members = set(grp_info.gr_mem)
                            if avatarId in members:
                                is_admin = True
                    if is_admin:
                        avatar = SSHAdminAvatar(avatarId)
                        avatar.groupService = self.groupService
                    else:
                        avatar = SSHUnauthorizedAvatar(avatarId)
            return IConchUser, avatar, lambda: None
        else:
            raise Exception("No supported interfaces found.")

class SSHAdminService(SSHServiceBase):
    realm = SSHAdminRealm()

