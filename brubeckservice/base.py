import logging
import os
import sys
import time
import ujson as json
import imp
import string
from uuid import uuid4
from brubeck.request import Request
from brubeck.request_handling import (
    CORO_LIBRARY,
    Brubeck,
)
from brubeckservice.connections import (
    ServiceConnection,
    _service_client_registration_listener,
    DEFAULT_HEARTBEAT_INTERVAL,
    start_service_client_registration_listener,
)
from brubeckservice.models import (
    ServiceRequest,
    ServiceResponse,
)
from brubeckservice.handlers import (
    ServiceMessageHandler,
)
from brubeckservice.tnetstrings import (
    t_parse,
)
from brubeckservice.service import (
    _register_service,
    _unregister_service,
    _DEFAULT_SERVICE_REQUEST_METHOD,
)
from brubeckservice.resource import (
    assure_resource,
)
from brubeckservice.service import (
    _SERVICE_RESOURCE_TYPE,
    _send_service_request,
    _get_service_info,
    _get_service_conn,
    _get_waiting_service_clients,
    REQUEST_TYPE_SYNC,
    REQUEST_TYPE_ASYNC,
    REQUEST_TYPE_FORWARD,
    HANDLE_RESPONSE,
    DO_NOT_HANDLE_RESPONSE,
)

from brubeckservice.coro import (
    coro_get_event,
    coro_send_event,
    coro_sleep,
    CORO_LIBRARY,
    coro_spawn,
)

#################################
## Request and Response stuff 
#################################


## handler client mixins
class ServiceClientMixin(object):
    """Class adds the functionality to any handler to send messages to a ServiceConnection
    This must be used with a handler or something that has the following attributes:
        self.application
    """

    ################################
    ## The public interface methods
    ## This is all your handlers should use
    ################################

    def register_service(self, service_id, service_addr, service_resp_addr, service_passphrase, sender_id):
        """Public wrapper around _register_service"""
        return _register_service(self.application, service_id, service_addr, service_resp_addr, service_passphrase, service_conn, sender_id)
        
    def unregister_service(self, service_id, service_passphrase, sender_id):
        """Public wrapper around _unregister_service"""
        return _unregister_service(self.application, service_id, service_passphrase, sender_id)
        
    def create_service_request(self, path, handle_response=HANDLE_RESPONSE, 
        method=_DEFAULT_SERVICE_REQUEST_METHOD, arguments={}, msg={}, headers={}):
        """ path - string, used to route to proper handler
            method - used to map to the proper method of the handler
            arguments - dict, used within the method call if needed
            These are not used anymore, but I feel they belong. 

            If not to only hold the original request
                headers - dict, contains the accepted method to call on handler
                msg - dict, the body of the message to process
        """
        if not isinstance(headers, dict):
            headers = json.loads(headers)
        if not isinstance(msg, dict):
            msg = json.loads(msg)

        data = {
            # Not sure if this is the socket_id, but it is used to return the message to the originator
            "origin_sender_id": self.message.sender,
            # This is the connection id used by the originator and is needed for Mongrel2
            "origin_conn_id": self.message.conn_id,
            # This is the socket address for the reply to the client
            "origin_out_addr": self.application.msg_conn.out_addr,
            # used to route the request
            "handle_response": str(handle_response),
            "path": path,
            "method": method,
            "arguments": arguments,
            # a dict, right now only METHOD is required and must be one of:
            # ['get', 'post', 'put', 
            # 'delete','options', 'connect', 
            # 'response', 'request']
            # "headers": headers,
            # a dict, this can be whatever you need it to be.
            "body": msg,
        }
        return ServiceRequest(**data)
    
    def send_service_request(self, service_id, service_req):
        """do some work and wait for the results of the response to handler_response the response from the service
        blocking, waits for handled result.
        """
        logging.debug("send_service_request(service_id=%s, service_req=%s)" % (service_id, service_req))
        #sender_id = self.application.msg_conn.sender_id
        service_req._request_type = REQUEST_TYPE_SYNC
        (sender_id, service_req) = _send_service_request(self.application, service_id, service_req)
        conn_id = service_req.conn_id
        raw_response = _wait(self.application, service_id, conn_id, sender_id)
        service_conn = _get_service_conn(self.application, service_id, sender_id)
        
        (response, handler_response) = service_conn.process_message( 
            self.application, raw_response, 
            service_id, service_conn.passphrase,
        )

        return (response, handler_response)

    def send_service_request_nowait(self, service_id, service_req):
        """defer some work, but still handle the response yourself
        non-blocking, returns immediately.
        """
        service_req._request_type = REQUEST_TYPE_ASYNC
        _send_service_request(self.application, service_id, service_req)
        return

    def forward_to_service(self, service_id, service_req):
        """give up any responsability for the request, someone else will respond to the client
        non-blocking, returns immediately.
        """
        service_req._request_type = REQUEST_TYPE_FORWARD
        _send_service_request(self.application, service_id, service_req)


###################################################
## Functions for waiting and notifying of response
###################################################
def _wait(application, service_id, conn_id, sender_id):
    """wait for the application to create an event from the service listener"""
    raw_response = None
    conn_id = str(conn_id)
    e = coro_get_event()
    waiting_events = _get_waiting_service_clients(application, service_id, sender_id)
    waiting_events[conn_id] = (int(time.time()), e)

    logging.debug("event %s on %s waiting " % (conn_id, sender_id))
    raw_response = e.get()
    logging.debug("event for %s raised, raw_response: %s" % (conn_id, raw_response))


    if raw_response is not None:
        return raw_response
    else:
        logging.debug("_wait for %s NO RESULTS" % conn_id)
        return None

def _update_service_heartbeat(application, service_id, sender_id):
    """Update our hearbeat timestamp in our service_listener for the service"""
    service_info = _get_service_info(application, service_id, sender_id)
    if not service_info is None:
        service_info['timestamp'] = int(time.time())
        return True
    else:
        logging.debug("service_info %s not found to update heartbeat (service_id):" % 
            (service_id))
        return False

## init functions
def service_client_init(application, 
    service_registration_passphrase, service_id, service_registration_addr,
    service_client_heartbeat_addr = None, service_client_heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL):
    """Starts everything needed for a brubeck app to be a service_client"""
    logging.debug('******** START service_client_init *********')

    logging.debug('service_registration_passphrase: %s' % service_registration_passphrase)
    logging.debug('service_id: %s' % service_id)
    logging.debug('service_registration_addr: %s' % service_registration_addr)
    logging.debug('service_client_heartbeat_addr: %s' % service_client_heartbeat_addr)
    logging.debug('service_client_heartbeat_interval: %s' % service_client_heartbeat_interval)

    logging.debug('******** END service_client_init *********')
    
    assure_resource(application)
    start_service_client_registration_listener(application,
        service_registration_passphrase, service_id, service_registration_addr, 
        service_client_heartbeat_addr, service_client_heartbeat_interval)

def lazyprop(method):
    """ A nifty wrapper to only load preoperties when accessed
    uses the lazyProperty pattern from:
    http://j2labs.tumblr.com/post/17669120847/lazy-properties-a-nifty-decorator
    inspired by  a stack overflow question:
    http://stackoverflow.com/questions/3012421/python-lazy-property-decorator
    This is to replace initializing common variable from cookies, query string, etc ..
    that would be in the prepare() method.
    """
    attr_name = '_' + method.__name__
    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            attr = method(self)
            setattr(self, attr_name, method(self))
            # filter out our javascript nulls
            if getattr(self, attr_name) == 'undefined':
                setattr(self, attr_name, None)
        return getattr(self, attr_name)
    return _lazyprop

class BrooklynCodeBrubeck(Brubeck):
    """our main application, extending Brubeck
    This is not application specific,
    but general stuff that maybe shoud be in Brubeck someday"""

    def __init__(self, *args, **kwargs):
        """ Most of the parameters are dealt with by Brubeck,
            do a little before call to super
        """
        settings_file = kwargs['settings_file'] if 'settings_file' in kwargs else none
        project_dir = kwargs['project_dir'] if 'project_dir' in kwargs  else none


        if project_dir == None:
            raise Exception('missing project_dir from config')
        else:
            self.project_dir = project_dir

        """load our settings"""
        if settings_file != None:
            self.settings = self.get_settings_from_file(settings_file)
        else:
            self.settings = {}

        # we may have overriden default mongrel2_pairs in settings
        #if 'mongrel2_pair' in self.settings:
        #    kwargs['mongrel2_pair'] = self.settings['mongrel2_pair']

        Brubeck.__init__(self, **kwargs)


    def get_settings(self, setting_name, file_name=None):
        """ This is an attempt at providing a possible
        external location for settings to overide any
        settings in the settings file that was passed
        to the application during initialization.
        """

        try:
            if hasattr(self, 'settings'):
                if hasattr(self.settings, setting_name):
                    # we had our settings loaded already
                    return getattr(self.settings, setting_name)

            if file_name == None:
                # create default file name
                file_name = self.project_dir + '/conf/' + setting_name + '.py'

            # try to load our file
            settings = self.get_settings_from_file(file_name)

            if hasattr(settings, setting_name):
                # load us in self.settings
                # so we don't need to read from file  next time
                self.settings.append({setting_name:settings[settings_name]})
                return settings[settings_name]

            raise Exception("Unable to load settings from file %s: %s " % (file_name, setting_name))

        except:
            # raise our error
            raise


    def get_settings_from_file(self, file_name):

        settings = None
        logging.debug ('loading settings from %s' % file_name)

        try:
            settings = imp.load_source('settings', file_name)
        except:
            raise #Exception("unknown error getting file: " + file_name)

        return settings
