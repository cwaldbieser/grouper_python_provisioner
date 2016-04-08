
from __future__ import print_function
from textwrap import dedent
from twisted.enterprise import adbapi
from twisted.logger import Logger
from twisted.plugin import IPlugin
from zope.interface import implements
from config import load_config, section2dict

class KikiProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)

    tag = "kiki"
    opt_help = "A provisioner delivery service provisioner."
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        return KikiProvisioner()


class KikiProvisioner(Interface):                                          
    implements(IProvisioner)

    service_state = None
                                                                        
    def load_config(self, config_file, default_log_level, logObserverFactory):
        """                                                             
        Load the configuration for this provisioner and initialize it.  
        """             
        # Load config.
        scp = load_config(config_file, defaults=self.get_config_defaults())
        config = section2dict(scp, "PROVISIONER")
        self.config = config
        # Start logger.
        log_level = config.get('log_level', default_log_level)
        log = Logger(observer=logObserverFactory(log_level))
        self.log = log
        log.debug("Initialized logging for Kiki identity attribute resolver.",
            event_type='init_provisioner_logging')
        # Initialize DB connection pool.
        # e.g. self.dbpool = adbapi.ConnectionPool('cx_Oracle', user='admin', password ='password', dsn='127.0.0.1/XE')
        db_section = "KIKI_DATABASE"
        db_options = scp.options(db_section)
        db_params = {}
        driver = "sqlite3"
        for opt in db_options:
            if opt.lower() == "driver":
                driver = scp.get(db_section, opt)
            else:
                db_params[opt] = scp.get(db_section, opt)
        self.dbpool = adbapi.ConnectionPool(driver, **db_params)
                                                                        
    def provision(self, route_key, message):             
        """                                                             
        Provision an entry based on the original route key and the parsed message.  
        """                                                             
        # Inspect the received message.  Where is the target destination?
        pass                                                            

    def get_config_defaults(self):
        """
        Return option defaults.
        """
        pass

