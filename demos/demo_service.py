#!/usr/bin/env python
import logging
import time
from brubeck.request_handling import Brubeck
from brubeckservice.base import (
    ServiceConnection,
    ServiceMessageHandler,
    coro_sleep,
    service_registration,
)
from brubeck.templating import (
    Jinja2Rendering,
    load_jinja2_env,
)


class SlowEchoServiceHandler(ServiceMessageHandler):
    """A slow service"""

    def request(self):
        """do something and take too long"""
        logging.debug("Starting request %s:%s" % (self.message.conn_id, int(time.time())))
        coro_sleep(5)
        self.set_status(200, "Took a while, but I am back.")
        self.add_to_payload("RETURN_DATA", self.message.get_argument("RETURN_DATA", "NO DATA"))
        self.headers = {"METHOD": "response"}
        logging.debug("Done, sending back %s:%s" % (self.message.conn_id, int(time.time())))
        return self.render()


##
## runtime configuration
##

# these must match the values defined on on the service_client
service_registration_addr = 'ipc://run/service_registration'
service_registration_passphrase='my_shared_registration_secret'
service_id = 'run_slow' # the id to call service by in client

# these are all defined here
service_addr = 'ipc://run/slow'
service_response_addr = 'ipc://run/slow_response'
service_passphrase = 'my_shared_secret'
heartbeat_addr = 'ipc://run/heartbeat'

config = {
    'msg_conn': ServiceConnection(service_addr, service_response_addr, service_passphrase),
    'handler_tuples': [ ## Set up our routes
        # Handle our service responses
        (r'^/service/slow', SlowEchoServiceHandler),
    ],
    'cookie_secret': '51cRa%76fa^O9h$4cwl$!@_F%g9%l_)-6OO1!',
    'template_loader': load_jinja2_env('./templates'),
    'log_level': logging.DEBUG,
}


##
## get us started!
##
app = Brubeck(**config)

service_registration(app, service_registration_addr, service_registration_passphrase, 
    service_id, service_addr, service_response_addr, service_passphrase, heartbeat_addr)
## start our server to handle requests
if __name__ == "__main__":
    app.run()
