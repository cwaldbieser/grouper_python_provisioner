
from __future__ import print_function
import argparse
import sys
from txgroupprovisioner.admin import SSHAdminService
from txgroupprovisioner.config import load_config, section2dict
from txgroupprovisioner.service import GroupProvisionerService
from txgroupprovisioner.web import WebService
from twisted.application import internet
from twisted.application.service import IServiceMaker, MultiService
from twisted.plugin import getPlugins, IPlugin
from twisted.python import usage
from zope.interface import implements


class Options(usage.Options):
    optParameters = [
        ["config", "c", None, 
            "Options in a specific configuration override options in any other "
            "configurations."],
        ['ssh-endpoint', 's', None, 
            "Endpoint string for SSH admin interface.  E.g. 'tcp:2022'"],    
        ['admin-group', 'a', None, 
            "Administrative access group.  Default 'txgroupadmins'"],    
        ['ssh-private-key', 'k', None, 
            "SSH admin private key.  Default 'keys/id_rsa'."],    
        ['ssh-public-key', 'p', None, 
            "SSH admin public key.  Default 'keys/id_rsa.pub'."],    
        ['web-endpoint', 'w', None, 
            "Endpoint string for web service."],    
    ]


class TwistdOpts(usage.Options):
    optFlags = [
        ["syslog", None, "Log to syslog."],
    ]
    optParameters = [
        ['logfile', 'l', "Log to file.", None],
        ['prefix', None, "Prefix when logging to syslog (default 'twisted').", "twisted"],    
    ]
    def parseArgs(self, *args):
        pass


class MyServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "provisioner"
    description = "Group Membership Provisioner"
    options = Options

    def makeService(self, options):
        """
        Construct a server from a factory.
        """
        config = options['config']
        ssh_endpoint_str = options['ssh-endpoint']
        admin_group = options['admin-group']
        ssh_private_key = options['ssh-private-key']
        ssh_public_key = options['ssh-public-key']
        web_endpoint_str = options['web-endpoint']
        # Parse the original `twistd` command line for logging options.
        parser = argparse.ArgumentParser("twistd argument parser")
        parser.add_argument(
            '--syslog',
            action='store_true')
        parser.add_argument(
            '-l',
            '--logfile',
            action='store')
        parser.add_argument(
            '--prefix',
            action='store',
            default='twisted')
        args, unknown = parser.parse_known_args()
        # Read the SSH config.
        scp = load_config(config)
        if scp.has_section("SSH"):
            ssh_cfg = section2dict(scp, "SSH")
            if ssh_endpoint_str is None:
                ssh_endpoint_str = ssh_cfg.get('endpoint', None)
            if admin_group is None:
                admin_group = ssh_cfg.get('admin_group', None)
            if ssh_private_key is None:
                ssh_private_key= ssh_cfg.get('ssh_private_key', None)
            if ssh_public_key is None:
                ssh_public_key = ssh_cfg.get('ssh_public_key', None)
        if scp.has_section("WEB"):
            web_cfg = section2dict(scp, "WEB")
            if web_endpoint_str is None:
                web_endpoint_str = web_cfg.get('endpoint', None)
        # Final defaults.
        if admin_group is None:
            admin_group = 'txgroupadmins'
        if ssh_private_key is None:
            ssh_private_key = 'keys/id_rsa'
        if ssh_public_key is None:
            ssh_public_key = 'keys/id_rsa.pub'
        # Create the service.
        try:
            groupService = GroupProvisionerService(
                config=config, 
                use_syslog=args.syslog, 
                syslog_prefix=args.prefix,
                logfile=args.logfile)
        except Exception as ex:
            print("Unable to create the group provisioner service: {0}".format(
                ex), file=sys.stderr)
            sys.exit(1)
        if ssh_endpoint_str is None and web_endpoint_str is None:
            rootService = groupService
        else:
            rootService = MultiService()
            groupService.setServiceParent(rootService)
            if ssh_endpoint_str is not None:
                sshService = SSHAdminService()
                sshService.endpointStr = ssh_endpoint_str
                sshService.realm.adminGroup = admin_group
                sshService.realm.groupService = groupService
                sshService.servicePrivateKey = ssh_private_key
                sshService.servicePublicKey = ssh_public_key
                sshService.setServiceParent(rootService)
            if web_endpoint_str is not None:
                webService = WebService()
                webService.endpointStr = web_endpoint_str
                webService.groupService = groupService
                webService.setServiceParent(rootService)
        return rootService


# Now construct an object which *provides* the relevant interfaces
# The name of this variable is irrelevant, as long as there is *some*
# name bound to a provider of IPlugin and IServiceMaker.
serviceMaker = MyServiceMaker()
