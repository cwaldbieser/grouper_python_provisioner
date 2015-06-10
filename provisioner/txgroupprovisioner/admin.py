
from zope.interface import implements
from txsshadmin.cred_base import SSHBaseAvatar, SSHBaseRealm
from txsshadmin.proto_dispatcher import (
    makeSSHDispatcherProtocolFactory,
    BaseHandler)
from txsshadmin.service import SSHServiceBase


class AdminHandler(BaseHandler):
    def onConnect(self, dispatcher):
        terminal = dispatcher.terminal
        terminal.write(
            "Welcome to the Admin Service, {0}".format(self.avatar.avatarId))
        terminal.nextLine()


SSHAdminProtocolFactory = makeSSHDispatcherProtocolFactory(AdminHandler)


class SSHAdminAvatar(SSHBaseAvatar):
    protocolFactory = SSHAdminProtocolFactory

    def __init__(self, avatarId):
        SSHBaseAvatar.__init__(self, avatarId)


class SSHAdminRealm(SSHBaseRealm):
   avatarFactory = SSHAdminAvatar 

class SSHAdminService(SSHServiceBase):
    realm = SSHAdminRealm()

