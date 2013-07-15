#!/usr/bin/env python

import unittest
import sys
from brubeckservice.connections import ServiceConnection
from handlers.method_handlers import simple_handler_method
from brubeck.request_handling import Brubeck, WebMessageHandler,
JSONMessageHandler
from brubeck.connections import to_bytes, Request, WSGIConnection
from brubeck.request_handling import(
    cookie_encode, cookie_decode,
    cookie_is_encoded, http_response
)
from handlers.object_handlers import(
    SimpleWebHandlerObject, CookieWebHandlerObject,
    SimpleJSONHandlerObject, CookieAddWebHandlerObject,
    PrepareHookWebHandlerObject, InitializeHookWebHandlerObject
)
from fixtures import service_request_handler_fixtures as FIXTURES

###
### Message handling (non)coroutines for testing
###
def route_message(application, message):
    handler = application.route_message(message)
    return request_handler(application, message, handler)

def request_handler(application, message, handler):
    if callable(handler):
        return handler()

class MockServiceRequest(object):
    """ we are enough of a message to test routing rules message """
    def __init__(self,
            path = '/',
            msg = FIXTURES.SERVICE_REQUEST_ROOT,
            sender = '1',
            conn_id = '1',
            origin_sender_id = '1',
            origin_conn_id  = '1',
            origin_out_addr  = '1',
            method = 'request',
            arguments = DictField(),
            headers = {},
            body = DictField(),
        ):
        self.msg = msg
        self.sender = sender
        self.conn_id = conn_id
        self.origin_sender_id = origin_sender_id #StringField(required=True)
        self.origin_conn_id  = conn_id #StringField(required=True)
        self.origin_out_addr  = origin_out_addrStringField(required=True)
        self.path = path #StringField(required=True)
        self.method = method #StringField(required=True)
        self.arguments = arguments #DictField(required=False)
        self.headers = headers
        self.body = body #DictField(required=True)

    def get(self):
       return self.msg

class TestRequestHandling(unittest.TestCase):
    """
    a test class for brubeck's request_handler
    """

    def setUp(self):
        """ will get run for each test """
        config = {
            # Verbose debugging?
            "DEBUG_MODE": True,

            ## Temporary directory
            ## no trailing slash
            "TEMP_DIR": "tmp/test",
            "SERVICE_CONN": 
            ## our service connection parameters
            "SERVICE_INFO": {
                "SERVICE_REGISTRATION_PASSPHRASE": "137b789e-b7eb-475a-8d5a-b16ab544yurp", 
                "SERVICE_ID": "test",
                "SERVICE_REGISTRATION_ADDR": "ipc://run/test/brubeck_svc_registration",
                "SERVICE_PASSPHRASE": "137b789e-b7eb-475a-8d5a-b16ab544yusp",
                "SERVICE_ADDR": "ipc://run/test/brubeck_svc",
                "SERVICE_RESPONSE_ADDR": "ipc://run/test/brubeck_svc_response",
                "SERVICE_HEARTBEAT_ADDR": "ipc://run/test/brubeck_svc_heartbeat",
                'HEARTBEAT': True,
            },
        }
        self.app = ServiceConnection(**config)
        
        config = {
            # Verbose debugging?
            "DEBUG_MODE": True,

            ## Temporary directory
            ## no trailing slash
            "TEMP_DIR": "tmp/uploader",
    
            ## our service connection parameters
            "SERVICE_INFO": {
                "SERVICE_REGISTRATION_PASSPHRASE": "137b789e-b7eb-475a-8d5a-b16ab544yurp", 
                "SERVICE_ID": "uploader",
                "SERVICE_REGISTRATION_ADDR": "ipc://run/uploader/brubeck_svc_registration",
                "SERVICE_PASSPHRASE": "137b789e-b7eb-475a-8d5a-b16ab544yusp",
                "SERVICE_ADDR": "ipc://run/uploader/brubeck_svc",
                "SERVICE_RESPONSE_ADDR": "ipc://run/uploader/brubeck_svc_response",
                "SERVICE_HEARTBEAT_ADDR": "ipc://run/uploader/brubeck_svc_heartbeat",
                'HEARTBEAT': True,
            },
        }
    ##
    ## our actual tests( test _xxxx_xxxx(self) ) 
    ##

    def test_send_service_request(self, service_id, service_req):

    def test_send_service_request_nowait(self, service_id, service_req):

    def test_forward_to_service(self, service_id, service_req):

    def test_add_route_rule_method(self):
    # Make sure we have no routes
        self.assertEqual(hasattr(self.app,'_routes'),False)

        # setup a route
        self.setup_route_with_method()

        # Make sure we have some routes
        self.assertEqual(hasattr(self.app,'_routes'),True)

        # Make sure we have exactly one route
        self.assertEqual(len(self.app._routes),1)

    def test_init_routes_with_methods(self):
        # Make sure we have no routes
        self.assertEqual(hasattr(self.app, '_routes'), False)

        # Create a tuple with routes with method handlers
        routes = [ (r'^/', simple_handler_method), (r'^/brubeck',
simple_handler_method) ]
        # init our routes
        self.app.init_routes( routes )

        # Make sure we have two routes
        self.assertEqual(len(self.app._routes), 2)

    def test_init_routes_with_objects(self):
        # Make sure we have no routes
        self.assertEqual(hasattr(self.app, '_routes'), False)

        # Create a tuple of routes with object handlers
        routes = [(r'^/', SimpleWebHandlerObject), (r'^/brubeck',
SimpleWebHandlerObject)]
        self.app.init_routes( routes )

        # Make sure we have two routes
        self.assertEqual(len(self.app._routes), 2)

    def test_init_routes_with_objects_and_methods(self):
        # Make sure we have no routes
        self.assertEqual(hasattr(self.app, '_routes'), False)

        # Create a tuple of routes with a method handler and an object handler
        routes = [(r'^/', SimpleWebHandlerObject), (r'^/brubeck',
simple_handler_method)]
        self.app.init_routes( routes )

        # Make sure we have two routes
        self.assertEqual(len(self.app._routes), 2)

    def test_add_route_rule_object(self):
        # Make sure we have no routes
        self.assertEqual(hasattr(self.app,'_routes'),False)
        self.setup_route_with_object()

        # Make sure we have some routes
        self.assertEqual(hasattr(self.app,'_routes'),True)

        # Make sure we have exactly one route
        self.assertEqual(len(self.app._routes),1)

    def test_brubeck_handle_request_with_object(self):
        # set up our route
        self.setup_route_with_object()

        # Make sure we get a handler back when we request one
        message = MockMessage(path='/')
        handler = self.app.route_message(message)
        self.assertNotEqual(handler,None)

    def test_brubeck_handle_request_with_method(self):
        # We ran tests on this already, so assume it works
        self.setup_route_with_method()

        # Make sure we get a handler back when we request one
        message = MockMessage(path='/')
        handler = self.app.route_message(message)
        self.assertNotEqual(handler,None)

    def test_cookie_handling(self):
        # set our cookie key and values
        cookie_key = 'my_key'
        cookie_value = 'my_secret'

        # encode our cookie
        encoded_cookie = cookie_encode(cookie_value, cookie_key)

        # Make sure we do not contain our value (i.e. we are really encrypting)
        self.assertEqual(encoded_cookie.find(cookie_value) == -1, True)

        # Make sure we are an encoded cookie using the function
        self.assertEqual(cookie_is_encoded(encoded_cookie), True)

        # Make sure after decoding our cookie we are the same as the unencoded
        # cookie
        decoded_cookie_value = cookie_decode(encoded_cookie, cookie_key)
        self.assertEqual(decoded_cookie_value, cookie_value)
    
    ##
    ## test a bunch of very simple requests making sure we get the expected
results
    ##
    def test_web_request_handling_with_object(self):
        self.setup_route_with_object()
        result = route_message(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT))
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(FIXTURES.HTTP_RESPONSE_OBJECT_ROOT, response)

    def test_web_request_handling_with_method(self):
        self.setup_route_with_method()
        response = route_message(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT))
        self.assertEqual(FIXTURES.HTTP_RESPONSE_METHOD_ROOT, response)

    def test_json_request_handling_with_object(self):
        self.app.add_route_rule(r'^/$',SimpleJSONHandlerObject)
        result = route_message(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT))
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(FIXTURES.HTTP_RESPONSE_JSON_OBJECT_ROOT, response)

    def test_request_with_cookie_handling_with_object(self):
        self.app.add_route_rule(r'^/$',CookieWebHandlerObject)
        result = route_message(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT_WITH_COOKIE))
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(FIXTURES.HTTP_RESPONSE_OBJECT_ROOT_WITH_COOKIE,
response)

    def
test_request_with_cookie_response_with_cookie_handling_with_object(self):
        self.app.add_route_rule(r'^/$',CookieWebHandlerObject)
        result = route_message(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT_WITH_COOKIE))
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(FIXTURES.HTTP_RESPONSE_OBJECT_ROOT_WITH_COOKIE,
response)

    def
test_request_without_cookie_response_with_cookie_handling_with_object(self):
        self.app.add_route_rule(r'^/$',CookieAddWebHandlerObject)
        result = route_message(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT))
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(FIXTURES.HTTP_RESPONSE_OBJECT_ROOT_WITH_COOKIE,
response)

    def test_build_http_response(self):
        response = http_response(FIXTURES.TEST_BODY_OBJECT_HANDLER, 200, 'OK',
dict())
        self.assertEqual(FIXTURES.HTTP_RESPONSE_OBJECT_ROOT, response)

    def test_handler_initialize_hook(self):
        ## create a handler that sets the expected body(and headers) in the
initialize hook
        handler = InitializeHookWebHandlerObject(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT))
        result = handler()
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(response, FIXTURES.HTTP_RESPONSE_OBJECT_ROOT)

    def test_handler_prepare_hook(self):
        # create a handler that sets the expected body in the prepare hook
        handler = PrepareHookWebHandlerObject(self.app,
Request.parse_msg(FIXTURES.HTTP_REQUEST_ROOT))
        result = handler()
        response = http_response(result['body'], result['status_code'],
result['status_msg'], result['headers'])
        self.assertEqual(response, FIXTURES.HTTP_RESPONSE_OBJECT_ROOT)

    ##
    ## some simple helper functions to setup a route """
    ##
    def setup_route_with_object(self, url_pattern='^/$'):
        self.app.add_route_rule(url_pattern,SimpleWebHandlerObject)

    def setup_route_with_method(self, url_pattern='^/$'):
        method = simple_handler_method
        self.app.add_route_rule(url_pattern, method)

##
## This will run our tests
##
if __name__ == '__main__':
    unittest.main()
