
from __future__ import print_function
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.enterprise import adbapi
from twisted.plugin import IPlugin
from zope.interface import implements
from interface import (
    IAttributeResolverFactory,
    IAttributeResolver,
)

class RDBMSAttributeResolverFactory(object):
    implements(IPlugin, IAttributeResolverFactory)
    tag = "rdbms_attrib_resolver"

    def generate_attribute_resolver(self, config_parser):
        """
        Create an object that implements IAttributeResolver.
        """
        resolver = RDBMSAttributeResolver()
        section = "RDBMS Attribute Resolver"
        options = config_parser.options(section)
        driver = config_parser.get(section, "driver")
        query = config_parser.get(section, "query")
        named_param = None
        if config_parser.has_option(section, "named_param"):
            named_param = config_parser.get(section, "query")
        driver_options = {}
        for opt in options:
            if opt not in ('driver', 'query', 'named_param'):
                driver_options[opt] = config_parser.get(section, opt)
        if driver == 'sqlite3':
            driver_options['check_same_thread'] = False
        dbpool = adbapi.ConnectionPool(driver, **driver_options)
        resolver.dbpool = dbpool
        resolver.query = query
        resolver.named_param = named_param
        return resolver


class RDBMSAttributeResolver(object):
    implements(IAttributeResolver)
    log = None
    query = None
    dbpool = None 
    named_param = None

    @inlineCallbacks
    def resolve_attributes(self, subject):
        """
        Return a Deferred that fires with the attributes for a subject.
        Atributes are a mapping of keys to a list of values.
        """
        if self.named_param is not None:
            args = {self.named_params: subject}
        else:
            args = [subject]
        rows = yield self.dbpool.runQuery(self.query, args)
        attribs = {}
        for attrib, value in rows:
            values = attribs.get(attrib)
            if values is None:
                values = []
                attribs[attrib] = values
            values.append(value)
        returnValue(attribs)        
        
