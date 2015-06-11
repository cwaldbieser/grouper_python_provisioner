
from zope.interface import Interface, Attribute

class IProvisionerFactory(Interface):
    tag = Attribute('String used to identify the plugin factory.')
    opt_help = Attribute('String description of the plugin.')
    opt_usage = Attribute('String describes how to provide arguments for factory.')

    def generateProvisioner(argstring=""):
        """
        Create an object that implements IProvisioner
        """


class IProvisioner(Interface):
    service_state = Attribute("Shared service state.")

    def load_config(config_file, default_log_level):
        """
        Load the configuration for this provisioner and initialize it.
        """

    def provision(group, subject, action):
        """
        Add a group, subject, and action to the provisioning pipeline.
        """
