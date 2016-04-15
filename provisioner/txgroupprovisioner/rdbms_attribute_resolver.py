
from twisted.enterprise import adbapi
from twisted.plugin import IPlugin
from zope.interface import implements
from kikimessage import Instructions
from txgroupprovisioner.interface import (
    IAttributeResolverFactory, 
    IAttributeResolver,
)


class RDBMSAttributeResolverFactory(object):
    implements(IPlugin, IAttributeResolverFactory)
    tag = "rdbms_attribute_resolver"

    def generate_attribute_resolver(config_parser):
        # Initialize DB connection pool.
        # e.g. self.dbpool = adbapi.ConnectionPool('cx_Oracle', user='admin', password ='password', dsn='127.0.0.1/XE')
        db_section = "KIKI_DATABASE"
        db_options = config_parser.options(db_section)
        db_params = {}
        driver = "sqlite3"
        for opt in db_options:
            if opt.lower() == "driver":
                driver = scp.get(db_section, opt)
            else:
                db_params[opt] = config_parser.get(db_section, opt)
        dbpool = adbapi.ConnectionPool(driver, **db_params)
        query = config_parser.get("PROVISIONER", "resolver_sql")
        resolver = RDBMSAttributeResolverFactory()
        resolver.dbpool = dbpool
        resolver.reactor = reactor
        resolver.query = query
        return resolver


class RDBMSAttributeResolver(object):
    implements(IPlugin, IAttributeResolver)
    dbpool = None
    reactor = None
    query = None

    @inlineCallbacks
    def resolve_attributes(self, subject):
        """
        Return a dictionary of attributes for `subject`.
        """
        rows = yield dbpool.runQuery(self.query, subject)
        attribs = {}
        for key, items in itertools.groupby(rows, key=lambda x: x[0]):
            values = [value for junk, value in items]
            attribs[key] = values
        returnValue(attribs)
            
