
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
    reactor = Attribute("The reactor used by the provisioner.")
    log = Attribute("A logger used by the provisioner.")

    def load_config(config_file, default_log_level, logObserverFactory):
        """
        Load the configuration for this provisioner and initialize it.
        This method is called *before* the main event reactor is started.
        Returns a Deferred that fires when configuration is complete.
        """

    def provision(message):
        """
        Provision an entry based on an AMQP message.
        Returns a Deferred that fires when provisioning is complete.
        """


class IMessageParserFactory(Interface):
    tag = Attribute("String used to identify the plugin factory.")

    def generate_message_parser(**kwds):
        """
        Create an object that implements IMessageParser.
        """


class IMessageParser(Interface):

    def parse_message(message):
        """
        Parse a message and return a Kiki Instructions
        object.
        """


class IAttributeResolverFactory(Interface):
    tag = Attribute('String used to identify the plugin factory.')

    def generate_attribute_resolver(config_parser):
        """
        Create an object that implements IAttributeResolver.
        """
    

class IAttributeResolver(Interface):
    log = Attribute('A logger.')
    reactor = Attribute('A reactor instance.')

    def resolve_attributes(subject):
        """
        Return a Deferred that fires with the attributes for a subject.
        Attributes are a mapping of keys to a list of values.
        """


class IGroupMapperFactory(Interface):
    tag = Attribute('String used to identify the plugin factory.')

    def generate_group_mapper(config_parser):
        """
        Create an object that implements IGroupMapper.
        """
    

class IGroupMapper(Interface):
    log = Attribute('A logger.')

    def get_groups_for_subject(subject):
        """
        Return a Deferred that fires with a list of groups for
        which `subject` is a member.
        """


class IRouterFactory(Interface):
    tag = Attribute('String used to identify the plugin factory.')

    def generate_router(config_parser):
        """
        Create an object that implements IRouter.
        """
    

class IRouter(Interface):
    log = Attribute('A logger.')

    def get_route(instructions, groups):
        """
        Return a Deferred that fires with a RouteInfo
        object or raises a NoMatchingRoute exception.
        If a message should be discarded, the route should
        map to None.
        """


