#!/usr/bin/env python
import logging
import time
from brubeck.request_handling import Brubeck
from brubeckservice.base import (
    ServiceConnection,
    ServiceMessageHandler,
    coro_sleep,
)
from brubeck.templating import (
    Jinja2Rendering,
    load_jinja2_env,
)


class SlowEchoServiceHandler(ServiceMessageHandler):
    """A slow service"""

    def request(self):
        """do something and take too long"""
        coro_sleep(5)
        self.set_status(200, "Took a while, but I am back.")
        self.add_to_payload("RETURN_DATA", self.message.get_argument("RETURN_DATA", "NO DATA"))
        self.headers = {"METHOD": "response"}
        return self.render()


##
## runtime configuration
##
config = {
    'msg_conn': ServiceConnection('ipc://run/slow', 'my_shared_secret'),
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
## start our server to handle requests
if __name__ == "__main__":
    app.run()
