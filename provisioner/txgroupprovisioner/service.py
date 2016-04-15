#! /usr/bin/env python

# Standard library
from __future__ import print_function
import datetime
from functools import partial
from json import load
import os
import os.path
import sys
from textwrap import dedent
# External modules
# - Twisted
from twisted.application import service
from twisted.application.service import Service
from twisted.internet import reactor, task
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.logger import Logger
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
from txamqp.queue import Closed as QueueClosedError
import txamqp.spec
# - application
from config import load_config, section2dict
from interface import IProvisionerFactory
from logging import make_syslog_observer, make_file_observer
from utils import get_plugin_factory


class ServiceState(object):
    last_update = None
    read_from_queue = False
    stopping = False

class GroupProvisionerService(Service):
    log = None
    maxSafeTimeNoUpdate = 60
    consumerTag = "mytag"

    def __init__(
            self, 
            a_reactor=None, 
            config=None, 
            use_syslog=False, 
            syslog_prefix=None,
            logfile=None):
        """
        Initialize the service.
        
        :param a_reactor: Override the reactor to use.
        """
        if a_reactor is None:
            a_reactor = reactor
        self._reactor = a_reactor
        self._port = None
        self.service_state = ServiceState()
        self.use_syslog = use_syslog
        self.syslog_prefix = syslog_prefix
        self.logfile = logfile
        self.config = config
        self.amqpLooper = None
        self.make_log_observer_factory()

    def checkStatus(self):
        """
        Return True if (1) the AMQP service is actively reading messages,
        and (2) the last_update set by the provisioner is 
        """
        log = self.log
        service_state = self.service_state
        if service_state.read_from_queue:
            last_update = service_state.last_update 
            log.debug("last_update={last_update}", 
                last_update=last_update.strftime("%Y-%m-%d %H:%M:%S"))
            delta = datetime.datetime.today() - last_update
            total_secs = total_seconds(delta)
            log.debug("total_seconds = {total_seconds} max_safe_time = {mst}", 
                total_seconds=total_secs, mst=self.maxSafeTimeNoUpdate)
            if total_secs <= self.maxSafeTimeNoUpdate:
                return True
            else:
                return False
        else:
            return False

    def make_log_observer_factory(self):
        if self.use_syslog:
            self.logObserverFactory = partial(
                make_syslog_observer,
                prefix=self.syslog_prefix)
        elif self.logfile is not None:
            self.logObserverFactory = partial(
                make_file_observer,
                self.logfile)
        else:
            raise Exception("No log observer factory!")    
        
    def startService(self):
        """
        Start the service.
        """
        scp = load_config(
            config_file=self.config, defaults=self.make_config_defaults())
        self.scp = scp
        app_info = section2dict(scp, "APPLICATION")
        log_level = app_info.get("log_level", "INFO")
        log = Logger(observer=self.logObserverFactory(log_level))
        self.log = log
        self.amqp_info = section2dict(scp, "AMQP")
        amqp_log_level = self.amqp_info.get("log_level", log_level) 
        self.amqp_log = Logger(observer=self.logObserverFactory(amqp_log_level))
        service_state = self.service_state 
        service_state.last_update = datetime.datetime.today()
        self.start_amqp_client()
        provisioner_tag = app_info['provisioner']
        log.info("Provisioner tag => '{provisioner}'", provisioner=provisioner_tag)
        provisioner_factory = get_plugin_factory(provisioner_tag, IProvisionerFactory)
        if provisioner_factory is None:
            log.error("No provisioner factory was found!")
            sys.exit(1)
        provisioner = provisioner_factory.generateProvisioner()
        provisioner.service_state = service_state
        provisioner.load_config(
            self.config, 
            log_level,
            self.logObserverFactory)
        self.provisioner = provisioner
        web_log_level = log_level
        if scp.has_section("WEB"):
            web_info = section2dict(scp, "WEB")
            web_log_level = web_info.get("log_level", log_level)
        self.web_log = Logger(observer=self.logObserverFactory(web_log_level))

    def parse_bindings(self, fname):
        """
        Queue map should be a JSON list of (queue_name, route_key) mappings.
        """
        with open(fname, "r") as f:
            o = load(f)
            return o

    def start_amqp_client(self):
        amqp_info = self.amqp_info
        log = self.amqp_log
        endpoint_str = amqp_info['endpoint']
        exchange = amqp_info['exchange']
        vhost = amqp_info['vhost']
        spec_path = amqp_info['spec']
        queue_name = amqp_info['queue']
        route_file = amqp_info['route_map']
        user = amqp_info['user']
        passwd = amqp_info['passwd']
        creds = (user, passwd)
        bindings = self.parse_bindings(route_file)
        queue_names = set([q for q, rk in bindings])
        queue_names.add(queue_name)
        log.debug(
            "endpoint='{endpoint}', exchange='{exchange}', vhost='{vhost}', user='{user}, spec={spec}'",
            endpoint=endpoint_str, exchange=exchange, vhost=vhost, user=user, spec=spec_path)
        for q in sorted(queue_names):
            log.debug("Declared: queue='{queue}'", queue=q)
        for q, rk in bindings:
            log.debug("Binding: queue='{queue}', route_key='{route_key}'", queue=q, route_key=rk)
        delegate = TwistedDelegate()
        spec = txamqp.spec.load(spec_path)
        ep = clientFromString(self._reactor, endpoint_str)
        d = connectProtocol(
            ep, 
            AMQClient( 
                delegate=delegate, 
                vhost=vhost, 
                spec=spec))
        d.addCallback(self.on_amqp_connect, exchange, queue_name, queue_names, bindings, creds)

        def onError(err):
            if reactor.running:
                log.failure(err)
                reactor.stop()

        d.addErrback(onError)

    @inlineCallbacks
    def on_amqp_connect(self, conn, exchange, queue_name, queue_names, bindings, creds):
        log = self.amqp_log
        provisioner = self.provisioner
        service_state = self.service_state
        log.info("Connected.")
        self.amqpConn = conn
        user, passwd = creds
        yield conn.authenticate(user, passwd)
        log.info("Authenticated.")
        channel = yield conn.channel(1)
        self.amqpChannel = channel
        yield channel.channel_open()
        log.info("Channel opened.")
        for name in queue_names:
            yield channel.queue_declare(queue=name, durable=True)
        log.info("Queues declared.")
        yield channel.exchange_declare(exchange=exchange, type='topic')
        log.info("Exchange declared.")
        for qname, route_key in bindings:
            yield channel.queue_bind(exchange=exchange, queue=qname, routing_key=route_key)
        log.info("Routings have been mapped.")
        self.createAMQPMessageLoop()
        self.startAMQPMessageLoop()

    def createAMQPMessageLoop(self):
        log = self.amqp_log
        amqpLooper = task.LoopingCall(self.processAMQPMessage)
        self.amqpLooper = amqpLooper

    def startAMQPMessageLoop(self):
        stopping = self.service_state.stopping
        read_from_queue = self.service_state.read_from_queue
        if not read_from_queue and not stopping: 
            log = self.amqp_log
            log.debug("Starting the AMQP message loop ...")
            self.service_state.read_from_queue = True
            d = self.startConsumingFromQueue()

            def startLoop_(result):
                try:
                    self.amqpLooper.start(0)
                except Exception as ex:
                    log.error("An error occured during AMQP message processing: {error}", error=str(ex))
                    raise

            d.addCallback(startLoop_)

    def stopAMQPMessageLoop(self):
        if self.amqpLooper.running: 
            log = self.amqp_log
            self.amqpLooper.stop()
            self.service_state.read_from_queue = False

            def logStopped_(result):
                log.debug("Stopped the AMQP message loop.")

            d = self.amqpChannel.basic_cancel(consumer_tag=self.consumerTag)
            d.addCallback(logStopped_)
            d.addErrback(lambda err: None)

    @inlineCallbacks
    def startConsumingFromQueue(self):
        log = self.amqp_log
        queue_name = self.amqp_info['queue']
        channel = self.amqpChannel
        conn = self.amqpConn
        log.debug("AMQP: starting basic_consume() for queue '{0}' ...".format(queue_name))
        yield channel.basic_consume(queue=queue_name, consumer_tag=self.consumerTag)
        self.amqpQueue = yield conn.queue(self.consumerTag) 
        log.debug("AMQP: basic_consume() started.")

    @inlineCallbacks
    def processAMQPMessage(self):
        channel = self.amqpChannel
        queue = self.amqpQueue
        reactor = self._reactor
        log = self.amqp_log
        provisioner = self.provisioner
        service_state = self.service_state
        log.debug("Attempting to read an AMQP message ...")
        try:
            msg = yield queue.get()
        except QueueClosedError:
            log.warn("Queue closed-- message not processed.")
            self.stopAMQPMessageLoop()
            yield task.deferLater(reactor, 30, self.start_amqp_client)
            returnValue(None) 
        if service_state.stopping or not service_state.read_from_queue:
            returnValue(None) 
        log.debug('Received: "{msg}" from channel # {channel}.', msg=msg.content.body, channel=channel.id)
        delay = 0
        recorded = False
        while not recorded and service_state.read_from_queue and not service_state.stopping:
            try:
                yield task.deferLater(reactor, delay, provisioner.provision, msg)
            except Exception as ex:
                log.error("Could not record message from queue.  Error was: {error}", error=ex)
                delay = min(600, max(delay+20, delay*2))
            else:
                recorded = True
                delay = 0    
                yield channel.basic_ack(delivery_tag=msg.delivery_tag)
                log.debug("Message from queue recorded.")
        
    @inlineCallbacks
    def closeAMQPConnection(self):
        conn = self.amqpConn
        channel = self.amqpChannel
        log.info("Closing AMQP channel ...")
        try:
            yield channel.channel_close()
        except Exception as ex:
            log.error("Error while trying to close AMQP channel: {error}", error=ex)
        else:
            log.info("AMQP Channel closed.")
        log.info("Closing AMQP connection ...")
        try:
            yield conn.connection_close()
        except Exception as ex:
            log.error("Error while trying to close AMQP connection: {error}", error=ex)
        else:
            log.info("AMQP Connection closed.")

    def set_listening_port(self, port):
        self._port = port
        
    def stopService(self):
        """
        Stop the service.
        """
        self.service_state.stopping = True
        async_tasks = []
        if self._port is not None:
            async_tasks.append(self._port.stopListening())
        async_tasks.append(self.closeAMQPConnection())
        return DeferredList(async_tasks, consumeErrors=True)             

    def make_config_defaults(self):
        spec_dir = os.path.join(os.path.split(os.path.split(__file__)[0])[0], "spec")
        spec_path = os.path.join(spec_dir, "amqp0-9-1.stripped.xml")
        return dedent("""\
            [APPLICATION]
            log_level = DEBUG
            provisioner = ldap
            
            [AMQP]
            log_level = INFO
            endpoint = tcp:host=localhost:port=5672
            exchange = grouper_exchange
            vhost = /
            spec = {spec_path}
            user = guest
            passwd = guest
            """.format(spec_path=spec_path))


def total_seconds(td):
    return float((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6)) / 10**6    

def main():
    service = GroupProvisionerService()
    service.startService()
    reactor.run()

if __name__ == "__main__":
    main()
#else:
#    application = service.Application("Twisted Group Provisioner")
#    service = GroupProvisionerService()
#    service.setServiceParent(application)
    
    
    
