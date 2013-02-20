#!/usr/bin/env python
import logging
import time
from brubeck.connections import Mongrel2Connection
from brubeck.request_handling import (
    JSONMessageHandler,
    WebMessageHandler, 
    Brubeck,
    render,
)
from brubeckservice.connections import (
    coro_sleep,
)
from brubeckservice.base import (
    ServiceClientMixin,
    service_client_init,
    HANDLE_RESPONSE,
    DO_NOT_HANDLE_RESPONSE,
)
from brubeckservice.handlers import (
    ServiceMessageHandler,
)

from brubeck.templating import (
    Jinja2Rendering,
    load_jinja2_env,
)
# some static data for testing
service_registration_passphrase = "my_shared_registration_secret"
service_registration_addr = "ipc://run/service_registration"
service_client_heartbeat_addr = "ipc://run/service_client_heartbeat"
service_id = 'run_slow'
service_path = ""
#service_addr = "ipc://run/slow"
#service_resp_addr = "ipc://run/slow_response"
#service_passphrase = "my_shared_secret"
service_path = '/service/slow'
request_headers = {}
request_method = 'request'
sync_request_arguments  = {"RETURN_DATA": 'I made a round trip, it took a while but I bring results.'}
async_request_arguments = {"RETURN_DATA": 'I made a round trip, it took so long I will respond to no one.'}

class DemoHandler(
        Jinja2Rendering,
        WebMessageHandler
    ):

    def get(self):
        # just return a page with some links
        context = {
            'name': "Async is faster, but ... nothing to report on my trip.",
        }

        return self.render_template('index.html', **context)


class CallServiceAsyncHandler(
        Jinja2Rendering,
        ServiceClientMixin,
        WebMessageHandler
    ):

    def get(self):
        # register our resource
        #self.register_service(service_addr, service_resp_addr, service_passphrase)
        # create a servicerequest
        service_request = self.create_service_request(
            service_path,
            DO_NOT_HANDLE_RESPONSE,
            request_method,
            async_request_arguments
        )

        ## Async
        self.send_service_request_nowait(service_id, service_request)

        # now return to client whatever you want
        self.set_status(200)
        context = {
            'name': "Async is faster, but ... nothing to report on my trip.",
        }
        return self.render_template('success.html', **context)


class CallServiceSyncHandler(
        Jinja2Rendering,
        ServiceClientMixin,
        WebMessageHandler
    ):

    def get(self):
        # register our service, if exist nothing happens
        #self.register_service(service_addr, service_resp_addr, service_passphrase)
        # create a servicerequest
        service_request = self.create_service_request(
            service_path,
            DO_NOT_HANDLE_RESPONSE,
            request_method,
            sync_request_arguments
        )

        ## Sync
        (response, handler_response) = self.send_service_request(service_id, service_request)

        # now return to client what you got back
        self.set_status(200)
        context = {
            'name': response.body["RETURN_DATA"],
        }
        return self.render_template('success.html', **context)


class ServiceResponseHandler(ServiceMessageHandler):
    """handles the response from our service
    """

    def response(self):
        """On successfull response from Brubeck Service"""
        if self.status_code == 200:
            logging.debug("Successfull %s:%s)!" % (self.status_code,self.status_msg))
        else:
            logging.debug("Failed (%s:%s)!" % (self.status_code,self.status_msg))
        # This is not a response to the client, but to the original handler
        # or no one at all if the service is called async
        return self.render()


##
## runtime configuration
##
config = {
    'msg_conn': Mongrel2Connection('tcp://127.0.0.1:9999',
                                     'tcp://127.0.0.1:9998'),
    'handler_tuples': [ ## Set up our routes
        # Handle our service responses
        (r'^/service/slow', ServiceResponseHandler),
        # Handle our request
        (r'^/service/sync', CallServiceSyncHandler),
        (r'^/service/async', CallServiceAsyncHandler),
        (r'^/$', DemoHandler),
    ],
    'cookie_secret': '51cRa%76fa^O9h$4cwl$!@_F%g9%l_)-6OO1!',
    'template_loader': load_jinja2_env('./templates'),
    'log_level': logging.DEBUG,
}

##
## get us started!
##
app = Brubeck(**config)

# Start our registration listener
service_client_init(app, 
    service_registration_passphrase,
    service_id, 
    service_registration_addr, 
    service_client_heartbeat_addr
)
    
## start our server to handle requests
if __name__ == "__main__":
    app.run()
