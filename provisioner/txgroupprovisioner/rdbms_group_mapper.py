
from __future__ import print_function
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.enterprise import adbapi
from twisted.plugin import IPlugin
from zope.interface import implements
from interface import (
    IGroupMapperFactory,
    IGroupMapper,
)

class RDBMSGroupMapperFactory(object):
    implements(IPlugin, IGroupMapperFactory)
    tag = "rdbms_group_mapper"

    def generate_group_mapper(self, config_parser):
        """
        Create an object that implements IGroupMapper.
        """
        mapper = RDBMSGroupMapper() 
        section = "RDBMS Group Mapper"
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
        mapper.dbpool = dbpool
        mapper.query = query
        mapper.named_param = named_param
        return mapper


class RDBMSGroupMapper(object):
    implements(IGroupMapper)
    log = None
    query = None
    dbpool = None 
    named_param = None

    @inlineCallbacks
    def get_groups_for_subject(self, subject):
        """
        Return a Deferred that fires with a list of groups for
        which `subject` is a member.
        """
        log = self.log
        log.debug(
            "Looking up groups for subject '{subject}' ...",
            subject = subject)
        if self.named_param is not None:
            args = {self.named_params: subject}
        else:
            args = [subject]
        log.debug(
            "Arguments that will be passed to query: {args}",
            args=args) 
        query = self.query
        log.debug("Query: {query}", query=query)
        rows = yield self.dbpool.runQuery(query, args)
        log.debug("Number of rows returned: {row_count}", row_count=len(rows))
        groups = []
        for row in rows:
            value = row[0]
            groups.append(value) 
        log.debug("Completed looking up {group_count} groups.", group_count=len(groups))
        returnValue(groups)        
        
