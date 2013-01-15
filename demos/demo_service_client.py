#!/usr/bin/env python
import logging
import time
from brubeck.connections import Mongrel2Connection
from brubeck.request_handling import (
    JSONMessageHandler,
    WebMessageHandler, 
    Brubeck,
)
from brubeckservice.base import (
    ServiceClientMixin,
    ServiceMessageHandler,
)
from brubeck.templating import (
    Jinja2Rendering,
    load_jinja2_env,
)
# some static data for testing
service_addr = "ipc://run/slow"
service_passphrase = "my_shared_secret"
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
        self.register_service(service_addr, service_passphrase)
        # create a servicerequest
        service_request = self.create_service_request(
            service_path,
            request_method,
            async_request_arguments
        )

        ## Async
        self.send_service_request_nowait(service_addr, service_request)

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
        self.register_service(service_addr, service_passphrase)
        # create a servicerequest
        service_request = self.create_service_request(
            service_path,
            request_method,
            sync_request_arguments
        )

        ## Sync
        (response, handler_response) = self.send_service_request(service_addr, service_request)

        logging.debug("Took a while, but lot's to say now")
        logging.debug("response: %s" % response)
        logging.debug("handler_response: %s" % handler_response)

        # now return to client what you got back
        self.set_status(200)
        context = {
            'name': response.body["RETURN_DATA"],
        }
        logging.debug("service_infos: %s" % self.application._resources)
        return self.render_template('success.html', **context)

class ServiceResponseHandler(ServiceMessageHandler):
    """handles the response from our service
    """

    def response(self):
        """On successfull upload by uploader BrubeckInstance"""
        if self.status_code == 200:
            logging.debug("Successfull %s:%s)!" % (self.status_code,self.status_msg))
        else:
            logging.debug("Failed (%s:%s)!" % (self.status_code,self.status_msg))
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
        (r'^/service/sync', CallServiceSyncHandler),
        (r'^/service/async', CallServiceAsyncHandler),
        (r'^/', DemoHandler),
    ],
    'cookie_secret': '51cRa%76fa^O9h$4cwl$!@_F%g9%l_)-6OO1!',
    'template_loader': load_jinja2_env('./templates'),
    'log_level': logging.DEBUG,
}

##
## get us started!
##
app = Brubeck(**config)
## start our server to handle requests
if __name__ == "__main__":
    app.run()
