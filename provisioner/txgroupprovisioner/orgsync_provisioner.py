
from __future__ import print_function
from collections import namedtuple
import json
from textwrap import dedent
import treq
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.logger import Logger
from twisted.plugin import IPlugin
from zope.interface import implements
from config import load_config, section2dict
from errors import (
    OptionMissingError,
)
from interface import (
    IProvisionerFactory,
    IProvisioner,
)
from utils import get_plugin_factory


class OrgsyncProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)

    tag = "orgsync"
    opt_help = "Orgsync RESTful API Provisioner"
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        return OrgsyncProvisioner()


class OrgsyncProvisioner(object):
    implements(IProvisioner)
    service_state = None
    reactor = None
    log = None

    @inlineCallbacks
    def load_config(self, config_file, default_log_level, logObserverFactory):
        """                                                             
        Load the configuration for this provisioner and initialize it.  
        """             
        log = Logger(observer=logObserverFactory("ERROR"))
        try:
            # Load config.
            scp = load_config(config_file, defaults=self.get_config_defaults())
            config = section2dict(scp, "PROVISIONER")
            self.config = config
            # Start logger.
            log_level = config.get('log_level', default_log_level)
            log = Logger(observer=logObserverFactory(log_level))
            self.log = log
            log.info("Initializing Orgsync RESTful API provisioner.",
                event_type='init_provisioner')
            # Load API configuration info-- endpoint info, URL, API key.
                     
    @inlineCallbacks                                                   
    def provision(self, amqp_message):             
        """                                                
        Provision an entry based on an AMQP message.  
        """                                              
        log = self.log
        try:
            entity = self.parse_message(amqp_message)
        except Exception as ex:
            log.warn("Error parsing message: {error}", error=ex)
            raise

    def parse_message(self, msg):
        pass

            

