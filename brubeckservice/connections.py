##
## BrubeckService connections
##
import logging
import time
from uuid import uuid4
import ujson as json
from brubeck.request import to_bytes, to_unicode
from brubeck.connections import (
    load_zmq,
    load_zmq_ctx,
    Mongrel2Connection,
    Connection,
)
from brubeck.request_handling import (
    CORO_LIBRARY,
)
from brubeck.request_handling import (
    JSONMessageHandler,
    WebMessageHandler, 
    Brubeck,
    render,
)
from brubeckservice.tnetstrings import (
    t,
    t_parse,
)
from brubeckservice.service import (
    _register_service,
    _unregister_service,
    parse_service_request,
    _DEFAULT_SERVICE_RESPONSE_METHOD,
    create_service_response,
    _get_service_info,
    close_out,
    close_in,
)
from brubeckservice.models import (
    ServiceResponse, 
    ServiceConnectionInfo
)
    
from brubeckservice.coro import (
    coro_get_event,
    coro_send_event,
    coro_sleep,
    CORO_LIBRARY,
    coro_spawn,
)

_DEFAULT_SERVICE_CLIENT_TIMEOUT = 360 # seconds before service is reregistered    
_DEFAULT_SERVICE_TIMEOUT = 360 # seconds before service is unregistered on client

_ALLOWED_MISSED_HEARTBEATS = 1 # missed heartbeats before listener killed
DEFAULT_HEARTBEAT_INTERVAL = 360 # time in seconds between heartbeats

class ServiceConnection(Mongrel2Connection):
    """Class is specific to handling communication with a ServiceClientConnection.
    """
    
    def __init__(self, svc_addr, svc_resp_addr, passphrase, sender_id = None):
        """sender_id = uuid.uuid4() or anything unique
        pull_addr = pull socket used for incoming messages
        pub_addr = publish socket used for outgoing messages

        The class encapsulates socket type by referring to it's pull socket
        as in_sock and it's publish socket as out_sock.
        """
        zmq = load_zmq()
        ctx = load_zmq_ctx()

        self.sender_id = uuid4().hex if sender_id is None else sender_id
        self.in_addr = svc_addr
        self.out_addr = svc_resp_addr
        self.last_response_sender_id = None

        # the request (in_sock) is received from a DEALER socket (round robin)
        self.in_sock = ctx.socket(zmq.PULL)
        self.in_sock.connect(self.in_addr)
        print("CONNECT service requested PULL socket %s:%s" % (self.in_addr, self.sender_id))

        # the response is sent to original clients incoming ROUTER socket
        self.out_sock = ctx.socket(zmq.ROUTER)
        self.out_sock.connect(self.out_addr)
        print("CONNECT service response ROUTER socket %s:%s" % (self.out_addr, self.sender_id))

        self.service_heartbeat = None
        self.reg_addr = None
        self.reg_sock = None
        
        self.zmq = zmq
        self.passphrase = passphrase
        
        self.service_heartbeat_connection = None

    def set_service_heartbeat_connection(self, application, service_heartbeat_connection):
        """set and start our service_heartbeat_connection """
        self.service_heartbeat_connection = service_heartbeat_connection
        self.service_heartbeat_connection.start(application)
        
    def process_message(self, application, message):
        """Function for coroutine that looks at the message, determines which handler will
        be used to process it, and then begins processing.
        The application is responsible for handling misconfigured routes.
        """

        service_request = parse_service_request(message, application.msg_conn.passphrase)
        if service_request is None:
            return
            
        handler = application.route_message(service_request)
        result = handler()

        msg = {}

        if result is not None and result is not "":
            msg = json.dumps(result)
        service_response = create_service_response(service_request, handler, method=_DEFAULT_SERVICE_RESPONSE_METHOD, arguments={}, msg=msg, headers={})
        
        application.msg_conn.send(service_response)

    def send(self, service_response):
        """uuid = unique ID that both the client and server need to match
           conn_id = connection id from this request needed to wake up handler on response
           origin_uuid = unique ID from the original request
           origin_conn_id = the connection id from the original request
           origin_out_addr = the socket address that expects the final result
           msg = the payload (a JSON object)
           request_type = what behavior am I expecting from the request (async, sync, forward)
           handle_response = call a handler on response processing
           path = the path used to route to the proper response handler
        """

        service_response.end_timestamp = int(time.time() * 1000)
        
        # check who we last sent a response to
        if self.last_response_sender_id is None:
            # first time, set it
            self.last_response_sender_id = service_response.sender
        elif not service_response.sender == self.last_response_sender_id:
            # Our single client restarted, reconnect
            self.out_sock.connect(self.out_addr)
            self.last_response_sender_id = service_response.sender

        header = "%s %s %s %s %s %s %s %s %s %s %s %s" % ( service_response.sender,
            t(service_response.conn_id),
            t(service_response.request_timestamp),
            t(service_response.start_timestamp),
            t(service_response.end_timestamp),
            t(self.passphrase),
            t(service_response.origin_sender_id),
            t(service_response.origin_conn_id),
            t(service_response.origin_out_addr),
            #t(service_response._service_request_type),
            t(service_response.handle_response),
            t(service_response.path),
            t(service_response.method),
        )
                
        status_code = to_bytes(str(json.dumps(service_response.status_code)))
        status_msg = to_bytes(json.dumps(service_response.status_msg))
        arguments = to_bytes(json.dumps(service_response.arguments))
        headers = to_bytes(json.dumps(service_response.headers))
        body = to_bytes(json.dumps(service_response.body))
        msg = '%s %s%s%s%s%s' % (header,
            t(status_code),
            t(status_msg),
            t(arguments),
            t(headers),
            t(body),
        )
        
        logging.debug("ServiceConnection send (%s:%s) : \"%s ...\"" % (self.out_addr, service_response.sender, msg[:20]))

        self.out_sock.send(service_response.sender, self.zmq.SNDMORE)
        self.out_sock.send("", self.zmq.SNDMORE)
        self.out_sock.send(msg, self.zmq.NOBLOCK)
        return

    def recv(self):
        """Receives a message from a ServiceClientConnection.
        """
        try:
            # blocking recv call
            logging.debug("recv waiting...")
            zmq_msg = self.in_sock.recv()
            logging.debug("...recv got")
            # if we are multipart, keep getting our message until we are done
            while self.in_sock.getsockopt(self.zmq.RCVMORE):
                logging.debug("...recv getting more")
                zmq_msg += self.in_sock.recv()
            logging.debug("...recv got all")

            return zmq_msg
        except Exception, e:
            logging.debug("ServiceConnection recv() going down.")

    def register_service(self, application, 
            service_registration_passphrase, service_id, service_registration_addr,  
            service_passphrase, service_addr, service_response_addr,  
            service_heartbeat_addr, service_heartbeat_timeout=_DEFAULT_SERVICE_CLIENT_TIMEOUT, 
            sender_id = None, 
            is_reregistration = 0):
        """just a wrapper around the function of the same name"""

        return register_service(application, 
            service_registration_passphrase, service_id, service_registration_addr, 
            service_passphrase, service_addr, service_response_addr, 
            service_heartbeat_addr, service_client_heartbeat_addr, 
            service_heartbeat_timeout, sender_id,
            is_reregistration);

    def reconnect(self, sender_id=None):
        """a full reconnect of our underlying sockets"""
        self.in_sock.close()
        self.out_sock.close()

        zmq = load_zmq()
        ctx = load_zmq_ctx()

        if sender_id is None:
            sender_id = uuid4().hex
        self.sender_id = sender_id
        
        self.last_response_sender_id = None

        # the request (in_sock) is received from a DEALER socket (round robin)
        self.in_sock = ctx.socket(zmq.PULL)
        self.in_sock.connect(self.in_addr)
        print("RECONNECT service requested PULL socket %s:%s" % (self.in_addr, self.sender_id))

        # the response is sent to original clients incoming DEALER socket
        self.out_sock = ctx.socket(zmq.ROUTER)
        self.out_sock.connect(self.out_addr)
        print("RECONNECT service response ROUTER socket %s:%s" % (self.out_addr, self.sender_id))


class ServiceClientConnection(ServiceConnection):
    """Class is specific to communicating with a ServiceConnection.
    """

    def __init__(self, application, svc_addr, svc_resp_addr, passphrase, sender_id=None, client_heartbeat_connection = None):
        """ passphrase = unique ID that both the client and server need to match
                for security purposed

            svc_addr = address of the Brubeck Service we are connecting to
            This socket is used for both inbound and outbound messages
        """
        self.active = True
        self.passphrase = passphrase
        self.sender_id = uuid4().hex if sender_id is None else sender_id
        self.out_addr = svc_addr
        self.in_addr = svc_resp_addr
        
        zmq = load_zmq()
        ctx = load_zmq_ctx()

        self.out_sock = ctx.socket(zmq.PUSH)
        self.out_sock.bind(self.out_addr)
        logging.debug("BIND service request PUSH socket %s:%s)" % (self.out_addr, self.sender_id))

        self.in_sock = ctx.socket(zmq.DEALER)
        self.in_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        self.in_sock.setsockopt(zmq.LINGER, 0)
        
        self.in_sock.bind(self.in_addr)
        logging.debug("BIND service response DEALER socket %s:%s" % (self.in_addr, self.sender_id))

        self.service_client_heartbeat = None
        self.reg_sock = None
        self.reg_addr = None
        self.zmq = zmq    

        self.client_heartbeat_connection = client_heartbeat_connection
        if not self.client_heartbeat_connection is None:
            self.client_heartbeat_connection.start(application)

    def set_client_heartbeat_connection(self, application, client_heartbeat_connection):
        """set and start our client_heartbeat_connection """
        self.client_heartbeat_connection = client_heartbeat_connection
        self.client_heartbeat_connection.start(application)

    def process_message(
        self,
        application,
        message,
        service_addr,
        service_passphrase
    ):
        """This coroutine looks at the message, determines which handler will
        be used to process it, and then begins processing.
        Since this is a reply, not a request,
        we simply call the handler and are done
        returns a tuple containing 1) the response object created 
            from parsing the message and 2) the handlers return value
        """
        if message is None:
            return (None, None)
            
        logging.debug("service_client_process_message service_passphrase: %s" % service_passphrase)
        service_response = parse_service_response(message, service_passphrase)
    
        logging.debug(
            "service_client_process_message service_response: %s" % service_response
        )
        
        logging.debug("service_client_process_message handle_response: %s" % service_response.handle_response)
        logging.debug("service_client_process_message service_response.path: %s" % service_response.path)
        if service_response.handle_response == "1":
            handler = application.route_message(service_response)
            
            handler.set_status(service_response.status_code,  service_response.status_msg)
            result = handler()
            logging.debug(
                "service_client_process_message service_response: %s ..." % service_response[:20])
            logging.debug("service_client_process_message result: %s ..." % result[:20])
            return (service_response, result)
    
        return (service_response, None)

    def send(self, service_req):
        """Send will wait for a response with a listener and is async
        """
        if service_req is None:
            return None
            
        service_req.conn_id = uuid4().hex
        #logging.debug("service_req._data: %s" % service_req._data)
        header = "%s %s %s %s %s %s %s %s %s %s" % (self.sender_id, 
            t(service_req.conn_id), 
            t(service_req.request_timestamp),
            t(self.passphrase),
            t(service_req.origin_sender_id),
            t(service_req.origin_conn_id),
            t(service_req.origin_out_addr),
            t(service_req.handle_response),
            t(service_req.path),
            t(service_req.method),
        )
        arguments = to_bytes(json.dumps(service_req.arguments))
        headers = to_bytes(json.dumps(service_req.headers))
        body = to_bytes(json.dumps(service_req.body))

        msg = '%s %s%s%s' % (header, t(arguments),t(headers), t(body))
        logging.debug(
            "ServiceClientConnection send (%s:%s:%s): %s ..." % (self.out_addr, self.sender_id, service_req.conn_id, msg[0:20])
        )
        self.out_sock.send(to_bytes(msg))

        return service_req

    def reconnect(self, sender_id = None):
        """a full reconnect of our underlying sockets"""
        zmq = load_zmq()
        ctx = load_zmq_ctx()

        self.in_sock.close()
        self.out_sock.close()

        if sender_id is None:
            sender_id = uuid4().hex
        self.sender_id = sender_id

        self.out_sock = ctx.socket(zmq.PUSH)
        self.out_sock.bind(self.out_addr)
        logging.debug("REBIND service request PUSH socket %s:%s" % (self.out_addr, self.sender_id))

        self.in_sock = ctx.socket(zmq.DEALER)
        self.in_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        self.in_sock.bind(self.in_addr)
        logging.debug("REBIND service response DEALER socket %s:%s" % (self.in_addr, self.sender_id))


## message listeners
def register_service(application, 
    service_registration_passphrase, service_id, service_registration_addr,
    service_passphrase, service_addr, service_response_addr,  
    service_heartbeat_addr = None, service_heartbeat_timeout=_DEFAULT_SERVICE_CLIENT_TIMEOUT, 
    sender_id = None, is_reregistration = 0,
    service_heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL,
    ):
    """Register a service with a client. Called once on service startup to 
    register service with the remote Brubeck service client.
    """

    logging.debug('******** START _service_registration *********')
    logging.debug('service_registration_passphrase: %s' % service_registration_passphrase)
    logging.debug('service_id: %s' % service_id)
    logging.debug('service_registration_addr: %s' % service_registration_addr)
    logging.debug('service_passphrase: %s' % service_passphrase)
    logging.debug('service_addr: %s' % service_addr)
    logging.debug('service_response_addr: %s' % service_response_addr)
    logging.debug('service_heartbeat_addr: %s' % service_heartbeat_addr)
    logging.debug('service_heartbeat_timeout: %s' % service_heartbeat_timeout)
    logging.debug('sender_id: %s' % sender_id)
    logging.debug('is_reregistration: %s' % is_reregistration)
    logging.debug('service_heartbeat_interval: %s' % service_heartbeat_interval)
    logging.debug('******** END _service_registration *********')
    
    service_connection_info = {
            'service_registration_passphrase': service_registration_passphrase,
            'service_id': service_id,
            'service_registration_addr': service_registration_addr,
            'service_passphrase': service_passphrase,    
            'service_addr': service_addr,
            'service_response_addr': service_response_addr,
            'service_heartbeat_addr': service_heartbeat_addr,
            'service_heartbeat_timeout': service_heartbeat_timeout,
            'service_heartbeat_interval': service_heartbeat_interval,
            'sender_id': sender_id,
            'is_reregistration': 1 if is_reregistration==1 else 0,
    }

    logging.debug("register_service: (service_registration_addr, service_registration_passphrase,service_heartbeat_timeout,sender_id): (%s,%s, %s, %s)" % 
        (service_registration_addr, service_registration_passphrase,service_heartbeat_timeout,sender_id))
    # Just start our zmq socket here, no reason for abstraction
    zmq = load_zmq()
    ctx = load_zmq_ctx()

    if sender_id is None:
        sender_id = application.msg_conn.sender_id
    # the request (in_sock) is received from a DEALER socket (round robin)
    service_registration_sock = ctx.socket(zmq.REQ)
    service_registration_sock.connect(service_registration_addr)
    
    print("CONNECT service registration REQ socket %s" % (service_registration_addr))
    msg = "%s %s %s %s %s %s %s" % (sender_id, t(service_registration_passphrase), 
        t(service_id), t(service_addr), t(service_response_addr), t(service_passphrase), 
        t(service_heartbeat_addr))
    logging.debug("service_registration sending request");
    service_registration_sock.send(to_bytes(msg))
    logging.debug("service_registration waiting for response");
    raw_registration_response = service_registration_sock.recv()    
    service_registration_sock.close()
    logging.debug("_service_registration recv(): %s ..." % raw_registration_response[:20])

    fields = (sndr_id, svc_registration_passphrase, svc_client_heartbeat_addr) = (
        raw_registration_response.split(' ', 2))
    i=1
    service_client_heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL
    for field in fields[1:]:
        fields[i] = t_parse(field)[0]
        i+=1   
    logging.debug("_service_reg application.msg_conn.reconnect(%s)" % sndr_id)    
    application.msg_conn.reconnect(sndr_id)
    
    # our minimal "security"    
    if fields[1] != service_registration_passphrase:
        # just log the breach
        logging.debug('Unknown service registration passphrase! (%s != %s)' % (str(fields[1]),service_registration_passphrase))
        return False;
    elif fields[1] == "0":
        logging.debug("registration failed")
        return False
    else:
        logging.debug("else we go on:")        
        # start both heartbeat and listener
        service_connection_info['service_client_heartbeat_addr']=fields[2]
        service_connection_info['sender_id']=sender_id
        
        service_info = _get_service_info(application, service_id, sender_id, 0)
        if is_reregistration == 1 or not service_info is None:
            # we will need to re-connect
            # first, our service base in/out sockets
            #svc_conn = application.msg_conn
            #logging.debug("RECONNECT svc_conn (OUT:IN:SENDER) (%s:%s:%s)" % (svc_conn.out_addr, svc_conn.in_addr, sndr_id))
            #svc_conn.reconnect(sndr_id)

            if not service_info is None:
                service_conn = service_info['service_conn']
                #logging.debug("RECONNECT service_conn (OUT:IN:SENDER) (%s:%s:%s)" % (service_conn.out_addr, service_conn.in_addr, sndr_id))
                #service_conn.reconnect(sndr_id)
            else:
                service_connection_info['is_reregistration'] = 0
        else:
            if not service_heartbeat_addr is None:
                service_heartbeat_connection = ServiceHeartbeatConnection(application, **service_connection_info)
                application.msg_conn.set_service_heartbeat_connection(application, service_heartbeat_connection)

    return True

def start_service_client_registration_listener(application, 
    service_registration_passphrase, service_id, service_registration_addr, 
    service_client_heartbeat_addr = None, service_client_heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL):
    """public function to start our registration listeners for services to register with."""
    coro_spawn(_service_client_registration_listener, application, 'registration_listener',
        service_registration_passphrase, service_id, service_registration_addr, 
        service_client_heartbeat_addr, service_client_heartbeat_interval)
    # allow other events to process to make sure the listener is started
    coro_sleep(0)

def _service_client_registration_listener(application, 
    service_registration_passphrase, service_id, service_registration_addr, 
    service_client_heartbeat_addr = None, service_client_heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL):
    """Function runs in a coroutine, one registration listener for each brubeck instance
    When a registration message is received the service is registered and a listener is started.
    """
    logging.debug("_service_client_registration_listener: (service_registration_addr, service_registration_passphrase,service_client_heartbeat_interval): (%s, %s, %s)" % 
        (service_registration_addr, service_registration_passphrase,service_client_heartbeat_interval))
    # Just start our zmq socket here, no reason for abstraction
    zmq = load_zmq()
    ctx = load_zmq_ctx()
    service_registration_sock = ctx.socket(zmq.REP)
    print("Binding service registration listener REP socket %s" % (service_registration_addr))
    service_registration_sock.bind(service_registration_addr)
    print("BOUND service registration listener REP socket %s" % (service_registration_addr))

    while True:
        logging.debug("_service_client_registration_listener waiting");
        raw_registration_request = service_registration_sock.recv()
        logging.debug("_service_client_registration_listener recv(): %s" % raw_registration_request)
        # 21a6d348083f4b7d8d9680be387e01da 29:my_shared_registration_secret 8:run_slow 14:ipc://run/slow 23:ipc://run/slow_response 16:my_shared_secret 27:ipc://run/service_heartbeat
        # just send raw message to connection client
        fields = (sender_id, service_reg_passphrase, service_id, 
            service_addr, service_resp_addr, service_passphrase, heartbeat_addr
        ) = raw_registration_request.split(' ', 6)
        i=1
        for field in fields[1:]:
            fields[i] = t_parse(field)[0]
            i+=1
        # our minimal "security"    
        if fields[1] != service_registration_passphrase:
            # just log the breach
            logging.debug('Unknown service registration passphrase! (%s != %s)' % (str(fields[1]),service_registration_passphrase))
            msg = "%s %s %s" % (t(fields[1]), t(0), t(service_client_heartbeat_addr))
        else:
            service_conn = ServiceClientConnection(application, fields[3], fields[4], fields[5], sender_id)
            
            logging.debug("_service_client_registration_listener sender_id: %s" % sender_id)
            _register_service(application, 
                fields[5], #serv_passphrase
                fields[2], #service_id 
                fields[3], # service_addr
                fields[4], # service_resp_addr
                service_conn)
            
            service_connection_info = {
                    'service_registration_passphrase': fields[5],
                    'service_id': fields[2],
                    'service_registration_addr': service_registration_addr,
                    'service_passphrase': fields[0],    
                    'service_addr': fields[3],
                    'service_response_addr': fields[4],
                    'service_heartbeat_addr': fields[6],
                    'service_client_heartbeat_addr': service_client_heartbeat_addr,
                    'service_heartbeat_timeout': _DEFAULT_SERVICE_TIMEOUT,
                    'service_heartbeat_interval': service_client_heartbeat_interval,
                    'sender_id': sender_id,
                    'is_reregistration': 0,
            }
            if not service_client_heartbeat_addr is None:
                client_heartbeat_connection = ClientHeartbeatConnection(application, 
                    **service_connection_info)
                service_conn.set_client_heartbeat_connection(application, client_heartbeat_connection)
            msg = "%s %s %s" % (sender_id, t(fields[1]), t(service_client_heartbeat_addr))
        coro_sleep(0)
        service_registration_sock.send(to_bytes(msg))

def parse_service_response(msg, passphrase):
    """Function for constructing a Reponse instance out of a
    message read straight off a zmq socket from a ServiceConnection.
    """
    logging.debug("parse_service_response: %s" % msg)

    fields = (sender, conn_id, request_timestamp, start_timestamp, end_timestamp, 
    msg_passphrase, origin_sender_id, origin_conn_id, origin_out_addr, handle_response, 
    path, method, rest) = msg.split(' ', 12)
    # first field is not tnetstring, no need to do anything
    # last is group of tnetstrings, will handle after
    i=1
    for field in fields[1:-1]:
        fields[i] = t_parse(field)[0]
        i+=1   
    # our minimal "security"    
    if fields[5] != passphrase:
        raise Exception('Unknown service identity! (%s != %s)' % (str(fields[5]),str(passphrase)))
    # deal with the "body" or rest that is group of tnetstrings
    (status_code, rest) = t_parse(rest)
    (status_msg, rest) = t_parse(rest)
    (arguments, rest) = t_parse(rest)
    (headers, rest) = t_parse(rest)
    (body, rest) = t_parse(rest)
    logging.debug("arguments: %s" % arguments)
    logging.debug("headers: %s" % headers)
    logging.debug("body: %s" % body)
    arguments = json.loads(arguments) if len(arguments) > 0 else {}
    headers = json.loads(headers) if len(headers) > 0 else {}
    if body[0] == "{":
        body = json.loads(body) if len(body) > 0 else {}
    else:
        body = {
            "RETURN_DATA": body,
        }
    service_response = ServiceResponse(**{
        "sender": fields[0], 
        "conn_id": fields[1], 
        "request_timestamp": fields[2],
        "start_timestamp": fields[3],
        "end_timestamp": fields[4],
        "origin_sender_id": fields[6], 
        "origin_conn_id": fields[7], 
        "origin_out_addr": fields[8], 
        "handle_response": fields[9],
        "path": fields[10],
        "method": fields[11], 
        "status_code": int(status_code), 
        "status_msg": status_msg,
        "arguments": arguments, 
        "headers": headers, 
        "body": body, 
    })
    return service_response


class HeartbeatConnection(Connection):
    """a HeartbeatConnection is a pair of distintly separate sockets.
    PING socket is a heartbeat set at a regular interval.
    PONG socket is a listener for a heartbeat.
    The two PING and PONG in this class should not be PING/PONGing each other.
    The use case is for a service and serivce client o be PING/PONGing each other both ways.
    """

    def __init__(self, application, *args, **kwargs):
        
        zmq = load_zmq()
        ctx = load_zmq_ctx()
        
        if kwargs['sender_id'] is None:
            self.sender_id = application.msg_conn.sender_id
        else:
            self.sender_id = kwargs['sender_id']

        self.application = application
        self.service_registration_passphrase = kwargs['service_registration_passphrase']
        self.service_id = kwargs['service_id']
        self.service_registration_addr = kwargs['service_registration_addr']
        self.service_passphrase=kwargs['service_passphrase']
        self.service_addr=kwargs['service_addr']
        self.service_response_addr=kwargs['service_response_addr']
        self.service_heartbeat_addr=kwargs['service_heartbeat_addr']
        self.service_client_heartbeat_addr=kwargs['service_client_heartbeat_addr']
        self.service_heartbeat_timeout = kwargs['service_heartbeat_timeout']
        self.service_heartbeat_interval = kwargs['service_heartbeat_interval']
        self.sender_id = kwargs['sender_id']
        self.is_reregistration= kwargs.get('is_reregistration', 0)
        self.heartbeat= kwargs.get('heartbeat', False)
        #self.sender_id = uuid4().hex
                    
        self.in_sock = ctx.socket(zmq.SUB) # our listener
        self.out_sock = ctx.socket(zmq.PUB) # our heartbeat

        self.in_sock.setsockopt(zmq.SUBSCRIBE, self.sender_id)

    def get_service_connection_info(self):
        return {
            'service_registration_passphrase': self.service_registration_passphrase,
            'service_id': self.service_id,
            'service_registration_addr': self.service_registration_addr,
            'service_passphrase': self.service_passphrase,
            'service_addr': self.service_addr,
            'service_response_addr': self.service_response_addr,
            'service_heartbeat_addr': self.service_heartbeat_addr,
            'service_client_heartbeat_addr': self.service_client_heartbeat_addr,
            'service_heartbeat_timeout': self.service_heartbeat_timeout,
            'service_heartbeat_interval': self.service_heartbeat_interval,
            'sender_id': self.sender_id,
            'is_reregistration': (1 if self.is_reregistration == 1 else 0),
            'heartbeat': self.heartbeat,
        }

    def start(self, application):
        def start_me_up(application):
            self.beat_forever(application)
            self.listen_forever(application)

        coro_spawn(start_me_up, application)

    def recv(self):
        """Receives a raw mongrel2.handler.Request object that you from the
        zeromq socket and return whatever is found.
        """
        zmq_msg = self.in_sock.recv()
        return zmq_msg
        
    def close(self):
        """Tells mongrel2 to explicitly close the HTTP connection.
        """
        self.alive = False

    def listen_forever(self, application):
        """Defines a function that will be the heartbeat listener.
        """
        pass

    def beat_forever(self, application):
        """our heartbeat"""
        pass
    
    def send(self, sender_id, msg):
        """send our heartbeat itseld"""
        self.out_sock.send('%s %s' % (sender_id, to_bytes(msg)) );


class ServiceHeartbeatConnection(HeartbeatConnection):
    """a HeartbeatConnection is a pair of distintly separate sockets.
    PING socket is a heartbeat set at a regular interval.
    PONG socket is a listener for a heartbeat.
    The two PING and PONG in this class should not be PING/PONGing each other.
    The use case is for a service and serivce client o be PING/PONGing each other both ways.
    """

    def __init__(self, application, *args, **kwargs):            

        super(ServiceHeartbeatConnection, self).__init__(application, 
        *args, **kwargs)

        zmq = load_zmq()
        ctx = load_zmq_ctx()

        self.in_addr = self.service_client_heartbeat_addr
        self.out_addr = self.service_heartbeat_addr
        self.alive = True

        logging.debug("ServiceHeartbeatConnection (%s, %s)" % (self.in_addr ,self.out_addr ))
        
        self.in_sock.connect(self.in_addr)
        self.out_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        self.out_sock.bind(self.out_addr)
        
        

    def listen_forever(self, application):
        """Defines a function that will be the heartbeat listener.
        """
        def fun_listen_forever(application):
            # forever is a long time, let's have a timeout
            zmq = load_zmq()
            ctx = load_zmq_ctx()

            poller = zmq.Poller()
            poller.register(self.in_sock, zmq.POLLIN)

            logging.debug("ServiceHeartbeatConnection listen_forever (%s, %s)." % (self.in_addr, self.sender_id))
            while self.alive:
                logging.debug("ServiceHeartbeatConnection listen_forever ... %s" % self.service_heartbeat_timeout)
                socks = dict(poller.poll(self.service_heartbeat_timeout * 1000))
                raw_heartbeat_response = None
                if socks and socks.get(self.in_sock) == zmq.POLLIN:
                    raw_heartbeat_response = self.in_sock.recv()
                    logging.debug("service PONG (%s,%s)" % (self.in_addr, self.sender_id))
                    coro_sleep(0)

                if raw_heartbeat_response is None:
                    logging.debug("service PONG TIMEOUT in service client heartbeat listener (%s,%s)" % (self.in_addr, self.sender_id))
                    # let us all die
                    self.alive=False
                    kwargs = self.get_service_connection_info()

                    # Just reregister all over again
                    kwargs['is_reregistration'] = 1

                    service_conn = ServiceClientConnection(application, 
                        kwargs['service_addr'], 
                        kwargs['service_response_addr'], 
                        kwargs['service_passphrase'])

                    logging.debug("ServiceHeartbeatConnection listen_forever self.sender_id: %s" % self.sender_id)
                    _register_service(application, 
                       kwargs['service_passphrase'], #serv_passphrase
                       kwargs['service_id'], #service_id 
                       kwargs['service_addr'], # service_addr
                       kwargs['service_response_addr'], # service_resp_addr
                       service_conn)

                    service_heartbeat_connection = ServiceHeartbeatConnection(application, 
                        **kwargs)
                    application.msg_conn.set_service_heartbeat_connection(application, service_heartbeat_connection)

            self.in_sock.close()
        coro_spawn(fun_listen_forever, application, 'client_heartbeat_listener')

    def beat_forever(self, application):
        """our heartbeat"""
        def the_beat_is_on(application):
            logging.debug("SERVICE beat is on")
            while self.alive:
                msg = "%s %s %s %s" % (self.sender_id, t(self.service_passphrase), t(self.service_id), t(self.service_heartbeat_addr))
                self.send(self.sender_id, msg)
                logging.debug("service PING (%s:%s)" % (self.out_addr, self.sender_id))
                coro_sleep(self.service_heartbeat_interval)

            if not self.alive:
                logging.debug("service PING DOWN self.alive check failed(%s:%s)" % (self.out_addr, self.sender_id))
            self.out_sock.close()
            
        logging.debug("SERVICE beat_forever spawning the_beat_is_on")
        coro_spawn(the_beat_is_on, application, 'service_heartbeat')


class ClientHeartbeatConnection(HeartbeatConnection):
    """a HeartbeatConnection is a pair of distintly separate sockets.
    PING socket is a heartbeat set at a regular interval.
    PONG socket is a listener for a heartbeat.
    The two PING and PONG in this class should not be PING/PONGing each other.
    The use case is for a service and serivce client o be PING/PONGing each other both ways.
    """

    def __init__(self, application, *args, **kwargs):            

        super(ClientHeartbeatConnection, self).__init__(application, 
        *args, **kwargs)

        zmq = load_zmq()
        ctx = load_zmq_ctx()

        self.in_addr = self.service_heartbeat_addr
        self.out_addr = self.service_client_heartbeat_addr
        self.alive = True
      

        logging.debug("ClientHeartbeatConnection (%s, %s)" % (self.in_addr ,self.out_addr ))
        
        self.in_sock.connect(self.in_addr)
        self.out_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        self.out_sock.bind(self.out_addr)


    def listen_forever(self, application):
        """Defines a function that will be the heartbeat listener.
        """
        def listen_forever(application):
            # forever is a long time, let's have a timeout
            zmq = load_zmq()
            ctx = load_zmq_ctx()
        
            poller = zmq.Poller()
            poller.register(self.in_sock, zmq.POLLIN)
        
            logging.debug("ClientHeartbeatConnection listen_forever (%s, %s)." % (self.in_addr, self.sender_id))
            while self.alive:
                logging.debug("ClientHeartbeatConnection listen_forever ... %s" % self.service_heartbeat_timeout)
                socks = dict(poller.poll(self.service_heartbeat_timeout * 1000))
                raw_heartbeat_response = None
                if socks and socks.get(self.in_sock) == zmq.POLLIN:
                    raw_heartbeat_response = self.in_sock.recv()
                    logging.debug("client PONG (%s, %s)" % (self.in_addr, self.sender_id))
                    coro_sleep(0)
                if raw_heartbeat_response is None:
                    logging.debug("client PONG TIMEOUT in service heartbeat listener (%s,%s)" % (self.in_addr, self.sender_id))
                    self.alive=False
                    #unreregister our service from the application
                    logging.debug("_unregister_service (%s,%s,%s)" % (self.application.msg_conn.sender_id, self.service_id, self.sender_id)) 
                    _unregister_service(self.application, self.service_id, self.sender_id)
                    
                    # reregister our service
                    #_service_registration(application, 
                    #    self.service_passphrase, self.service_id, self.service_addr,
                    #    self.service_passphrase, self.service_addr, self.service_response_addr,  
                    #    self.service_heartbeat_addr, self.service_client_heartbeat_addr, 
                    #    self.service_heartbeat_timeout, self.sender_id,
                    #    1)
                             
        coro_spawn(listen_forever, application, 'service_heartbeat_listener')

    def beat_forever(self, application):
        """our heartbeat"""
        def the_beat_is_on(application):
            logging.debug("CLIENT beat is on")
            while self.alive:
                msg = "%s %s %s %s" % (self.sender_id, t(self.service_passphrase), t(self.service_id), t(self.service_heartbeat_addr))
                self.send(self.sender_id, msg)
                logging.debug("client PING (%s:%s)" % (self.out_addr, self.sender_id))
                coro_sleep(self.service_heartbeat_interval)
            if not self.alive:
                logging.debug("client PING DOWN self.alive check failed(%s:%s)" % (self.out_addr, self.sender_id))

        logging.debug("CLIENT beat_forever spawning the_beat_is_on")
        coro_spawn(the_beat_is_on, application, 'client_heartbeat')
