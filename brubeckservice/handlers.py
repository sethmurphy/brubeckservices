##
## BrubeckService handlers 
##
import logging
from brubeck.request_handling import MessageHandler
from coro import (
    coro_conn,
    coro_count,
    coro_spawn,
)
# supported methods for a service (MAYBE, NOT ALL IMPLEMENTED)
_SERVICE_METHODS = ['get', 'post', 'put', 'delete', 
                   'options', 'connect', 'response', 'request']

class ServiceMessageHandler(MessageHandler):
    """Class is the simplest implementation of a message handlers. 
    Intended to be used for Service inter communication.
    """
    def __init__(self, application, message, *args, **kwargs):
        self.headers = {}
        super(ServiceMessageHandler, self).__init__(application, message, *args, **kwargs)
        
    def render(self, status_code=None, status_msg=None, headers = None, **kwargs):
        if status_code is not None:
            self.set_status(status_code, status_msg)

        if headers is not None:
            self.headers = headers

        body = self._payload
        
        logging.info('%s %s %s (%s:%s) for (%s:%s)' % (self.status_code, self.message.method,
                                        self.message.path,
                                        self.message.sender,
                                        self.message.conn_id,
                                        self.message.origin_out_addr,
                                        self.message.origin_conn_id,
                                        ))

        return body

    def __call__(self):
        """This is similar to the base call in MessageHandler without the 
        assumption we are an HTTP request.

        It requires a method attribute to indicate which function on the
        handler should be called. If that function is not supported, call the
        handlers unsupported function.

        In the event that an error has already occurred, _finished will be
        set to true before this function call indicating we should render
        the handler and nothing else.

        In all cases, generating a response for the service request is attempted.
        """
        try:
            self.prepare()
            if not self._finished:
                mef = self.message.method.lower()  # M-E-T-H-O-D man!

                # Find function mapped to method on self
                if (mef in _SERVICE_METHODS):
                    fun = getattr(self, mef, self.unsupported)
                else:
                    fun = self.unsupported

                # Call the function we settled on
                try:
                    if not hasattr(self, '_url_args') or self._url_args is None:
                        self._url_args = []

                    if isinstance(self._url_args, dict):
                        ### if the value was optional and not included, filter it
                        ### out so the functions default takes priority
                        kwargs = dict((k, v)
                                      for k, v in self._url_args.items() if v)
                        rendered = fun(**kwargs)
                    else:
                        rendered = fun(*self._url_args)

                    if rendered is None:
                        logging.debug('Handler had no return value: %s' % fun)
                        return ''
                except Exception, e:
                    logging.error(e, exc_info=True)
                    rendered = self.error(e)

                self._finished = True
                return rendered
            else:
                return self.render()
        finally:
            self.on_finish()

def ServicesHandler(MessageHandler):
    def get(self):
        data  = [{
            "name": "services",
            "coro_count": coro_count(self.application),
            "coro_count_service_in": coro_count(self.application, 'service_in'),
            "coro_count_service_out": coro_count(self.application, 'service_out'),
            "coro_count_heartbeat": coro_count(self.application, 'heartbeat'),
            "coro_count_heartbeart_listener": coro_count(self.application, 'heartbeat_listener'),
            "coro_count_registration": coro_count(self.application, 'registration'),
            "coro_count_registration_listener": coro_count(self.application, 'registration_listener'),
        }]
        return self.render_template('services', **data)