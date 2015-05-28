
from twisted.logger import Logger, LogLevel
from twisted.logger import LegacyLogObserverWrapper, FilteringLogObserver, LogLevelFilterPredicate
from twisted.python.syslog import SyslogObserver
import datetime

def wrap_observer(observer):

    def observeit(event):
        new_event = {}
        new_event.update(event)
        new_event['level'] = event['log_level'].name.upper()
        new_event['log_format'] = '[{{level}}] {0}'.format(event['log_format'])
        return observer(new_event)

    return observeit

def make_syslog_observer(log_level_name, prefix="txLogger"):
    log_level = LogLevel.lookupByName(log_level_name.lower())
    observer = SyslogObserver(prefix)
    observer = LegacyLogObserverWrapper(observer.emit)
    observer = wrap_observer(observer)
    predicate = LogLevelFilterPredicate(defaultLogLevel=log_level)
    observer = FilteringLogObserver(observer, [predicate])
    return observer


