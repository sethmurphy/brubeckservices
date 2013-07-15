from brubeckservice import (
    ServiceRequestHandler,
)

from tests.fixtures import service_request_handler_fixtures as FIXTURES



class SimpleServiceRequestHandlerObject(ServiceRequestHandler):
    def get(self):
        self.set_body(FIXTURES.TEST_SERVICE_REQUEST_HANDLER)
        return self.render()


class PrepareHookWebHandlerObject(ServiceRequestHandler):
    def get(self):
        return self.render()

    def prepare(self):
        self.set_body(FIXTURES.TEST_SERVICE_REQUEST_HANDLER)

class InitializeHookWebHandlerObject(ServiceRequestHandler):
    def get(self):
        return self.render()

    def initialize(self):
        self.headers = dict()
        self.set_body(FIXTURES.TEST_SERVICE_REQUEST_HANDLER)

