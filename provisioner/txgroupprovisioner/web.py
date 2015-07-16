
from klein import Klein
from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import serverFromString
from twisted.web.server import Site
import werkzeug


class WebResources(object):
    app = Klein()
    _log = None
    reactor = reactor
    groupService = None

    @property
    def log(self):
        if self._log is None:
            self._log = self.groupService.web_log
        return self._log

    @app.route('/status', methods=['GET'])
    def status(self, request):
        client_ip = request.getClientIP()
        if self.groupService.checkStatus():
            request.setResponseCode(200)
            return '''200 - OK'''
        else:
            request.setResponseCode(500)
            return '''500 - Error'''

    @app.handle_errors(werkzeug.exceptions.NotFound)
    def error_handler_404(self, request, failure):
        self.log.error("http_status=i{status}, client_ip={client_ip}, path={path}".format(
            status=404,
            client_ip=request.getClientIP(), 
            path=request.path))
        request.setResponseCode(404)
        return '''404 - Not Found'''

    @app.handle_errors
    def error_handler_500(self, request, failure):
        request.setResponseCode(500)
        self.log.error("status={status} http_status=500, client_ip={client_ip}: {err}".format(
            status=500, 
            client_ip=request.getClientIP(), 
            err=str(failure)))
        return '''500 - Internal Error'''
       
 
def make_ws(groupService):
    """
    Create and return the web service site.
    """
    ws = WebResources()
    ws.groupService = groupService
    root = ws.app.resource()
    site = Site(root)
    return site


class WebService(Service):
    _port = None
    endpointStr = "tcp:9610"
    displayTracebacks = False
    groupService = None

    def startService(self):
        site = make_ws(self.groupService)
        site.displayTracebacks = self.displayTracebacks
        ep = serverFromString(reactor, self.endpointStr)
        d = ep.listen(site)
        d.addCallback(self.setListeningPort)

    def stopService(self):
        port = self._port
        if port is not None:
            port.stopListening()

    def setListeningPort(self, port):
        self._port = port

