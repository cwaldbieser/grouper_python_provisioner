#! /usr/bin/env python

from klein import Klein
from twisted.application.service import Application, Service
from twisted.internet import reactor
from twisted.internet.defer import CancelledError, Deferred, inlineCallbacks, returnValue
from twisted.internet.endpoints import clientFromString, connectProtocol, serverFromString
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from twisted.web.server import Site
import werkzeug
import datetime

#=======================================================================
#=======================================================================

class ConnectionClosedByPeerError(Exception):
    pass

class StatusCheckerProtocol(LineReceiver):
    def __init__(self):
        self.response = Deferred()
        self.success = False

    def askStatus(self):
        self.sendLine("status:")

    def lineReceived(self, line):
        self.success = True
        self.response.callback(line)
        self.transport.loseConnection()

    def connectionLost(self, reason):
        if not self.success:
            self.response.errback(ConnectionClosedByPeerError())

class StatusWebService(object):
    app = Klein()
    peer = "tcp:127.0.0.1:9600"
    max_drift = 900
    clientTimeout = 30

    def __init__(self, reaktor=reactor):
        self.reactor = reaktor

    @app.route('/status', methods=['GET'])
    @inlineCallbacks
    def status(self, request):
        client_ip = request.getClientIP()
        proto = StatusCheckerProtocol()
        peer = self.peer 
        attempt = connectProtocol(clientFromString(self.reactor, peer), proto)
        self.reactor.callLater(self.clientTimeout, attempt.cancel)
        try:
            yield attempt
        except CancelledError as ex:
            log.msg("[ERROR] Timeout while trying to connect to peer.")
            request.setResponseCode(500)
            returnValue('''500 - internal error''')
        self.reactor.callLater(self.clientTimeout, proto.response.cancel)
        proto.askStatus()
        try:
            response = yield proto.response
        except CancelledError as ex:
            log.msg("[ERROR] Timeout while communicating with peer.")
            request.setResponseCode(500)
            returnValue('''500 - internal error''')
        try:
            dt = datetime.datetime.strptime(response, "%Y-%m-%dT%H:%M:%S")
        except ValueError as ex:
            log.msg("[ERROR] Was not able to parse date string received from status endpoint: {0}".format(response))
            request.setResponseCode(500)
            returnValue('''500 - internal error''')
        now = datetime.datetime.today()
        delta = now - dt
        elapsed_seconds = delta.days * (24*3600) + delta.seconds 
        if elapsed_seconds > self.max_drift:
            log.msg("[ERROR] Elased seconds since last successful status == {0}".format(elapsed_seconds))
            request.setResponseCode(500)
            returnValue('''500 - internal error''')
        else:
            request.setResponseCode(200)
            returnValue('''200 - OK''')

    @app.handle_errors(werkzeug.exceptions.NotFound)
    def error_handler_404(self, request, failure):
        log.msg("[ERROR] http_status=404, client_ip={client_ip}, path={path}".format(
            client_ip=request.getClientIP(), path=request.path))
        request.setResponseCode(404)
        return '''404 - Not Found'''

    @app.handle_errors
    def error_handler_500(self, request, failure):
        request.setResponseCode(500)
        log.msg("[ERROR] http_status=500, client_ip={client_ip}: {err}".format(
            client_ip=request.getClientIP(), err=str(failure)))
        return '''500 - Internal Error'''
        
def make_ws():
    """
    Create and return the web service site.
    """
    ws = StatusWebService()
    root = ws.app.resource()
    site = Site(root)
    return site

class StatusService(Service):
    port = None
    endpoint = "tcp:9610"

    def startService(self):
        site = make_ws()
        site.displayTracebacks = False
        ep = serverFromString(reactor, self.endpoint)
        d = ep.listen(site)
        d.addCallback(self.setListeningPort)

    def stopService(self):
        port = self.port
        if port is not None:
            port.stopListening()

    def setListeningPort(self, port):
        self.port = port

application = Application("Status Checking App")
service = StatusService()
service.setServiceParent(application)
