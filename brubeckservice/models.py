##
## BrubeckService models
##
import time
from dictshield.document import Document
from dictshield.fields import (
    StringField,
    BooleanField,
    URLField,
    EmailField,
    LongField,
    DictField,
    IntField,
)

class ServiceRequest(Document):
    """Class used to construct a Brubeck service request message.
    Both the client and the server use this.
    """
    # set by the send call in the client connection
    sender = StringField(required=True)
    # set by the send call in the client connection
    conn_id = StringField(required=True)
    # set by send call in client connection
    # used to return the message to the originator
    origin_sender_id = StringField(required=True)
    # This is the connection id used by the originator and is needed for Mongrel2
    origin_conn_id  = StringField(required=True)
    # This is the socket address for the reply to the client
    origin_out_addr  = StringField(required=True)
    # flag to see if response tries to call handler (1=yes)
    handle_response = StringField(required=True)
    # used to route the request
    path = StringField(required=True)
    # used to route the request to the proper method of the handler
    method = StringField(required=True)
    # a dict, used to populat an arguments dict for use within the method
    arguments = DictField(required=False)
    # a dict, these get passed to the method called (use body for complex or large objects)
    headers = DictField(required=False)
    # a dict, this can be whatever you need it to be to get the job done.
    body = DictField(required=True)

    def __init__(self, *args, **kwargs):
        self.request_timestamp = int(time.time() * 1000)
        super(ServiceRequest, self).__init__(*args, **kwargs)

    def get_argument(self, key, default=None):
        """get's an argument by name"""
        if key in self.arguments:
            return self.arguments[key]
        return default


class ServiceResponse(ServiceRequest):
    """Class used to construct a Brubeck service response message.
    """
    status_code = IntField(required=True)
    status_message = StringField()
    def __init__(self, *args, **kwargs):
        self.start_timestamp = int(time.time() * 1000)
        self.end_timestamp = 0
        super(ServiceResponse, self).__init__(*args, **kwargs)

class ServiceConnectionInfo(Document):
    """Contains all the infor mation needed to set up a service
    heartbeats and all (Not the request itself)"""
    # passphrase needed for service registration
    service_registration_passphrase = StringField(required=True)   
    # service id or name
    service_id = StringField(required=True)
    # the address to register the service for
    service_registration_addr = StringField(required=True)


    # passphrase needed for service call
    service_passphrase = StringField(required=True)    
    # the service request address
    service_addr = StringField(required=True)
    # the service response address
    service_response_addr = StringField(required=True)
    
    # the service heartbeat address
    service_heartbeat_addr = StringField(required=True)
    # the client heartbeat address
    client_heartbeat_addr = StringField(required=True)
    
    # timeout before an endpoint dead
    service_heartbeat_timeout = IntField(required=True)
    # how often to send a heartbeat
    service_heartbeat_interval = IntField(required=True)
    
    # single connection id for pair
    sender_id = StringField(required=True)

    is_reregistration = IntField(required=True, default=0)
    
