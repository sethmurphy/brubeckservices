############################################
## Service registration (Resource) helpers
############################################
import logging
import time
import ujson as json
from brubeckservice import (
    ServiceError,
)
from brubeckservice.resource import (
    assure_resource,
    is_resource_registered,
    register_resource,
    unregister_resource,
    get_resource,
    create_resource_key,
    get_resource_keys,
    Resource,
)
from brubeck.request_handling import (
    CORO_LIBRARY,
)
from brubeckservice.tnetstrings import (
    t_parse,
    t,
)
from brubeckservice.models import (
    ServiceRequest,
    ServiceResponse,
)

from brubeckservice.coro import (
    coro_get_event,
    coro_send_event,
    coro_sleep,
    CORO_LIBRARY,
    coro_conn,
    coro_count,
    coro_spawn,
)
  
_SERVICE_RESOURCE_TYPE = 'SERVICE'
_DEFAULT_SERVICE_REQUEST_METHOD = 'request'
_DEFAULT_SERVICE_RESPONSE_METHOD = 'response'

REQUEST_TYPE_SYNC = 0 # Request is sent and sender waits for response
REQUEST_TYPE_ASYNC = 1 # Request is sent and sender continues on
REQUEST_TYPE_FORWARD = 2 # Request is sent and sender cedes any responsabilities

HANDLE_RESPONSE = 1
DO_NOT_HANDLE_RESPONSE = 0

def _service_is_registered(application, service_id):
    """ Check if a service is registered"""
    key = create_resource_key(service_id, _SERVICE_RESOURCE_TYPE)
    return is_resource_registered(key)

def _register_service(application, 
    service_passphrase, service_id, service_addr, 
    service_resp_addr, service_conn):
    """ Create and store a connection and it's listener and waiting_clients queue.
    """
    assure_resource(application)      
    sender_id = service_conn.sender_id
    key = create_resource_key(service_id, _SERVICE_RESOURCE_TYPE)  
    if not is_resource_registered(key):                
        instances = {}
        instance = {            
            'sender_id': sender_id,
            'service_conn': service_conn, 
            'waiting_clients': {}, 
        }
        instances[sender_id] = instance
        
        # add us to the list
        resource = {
            'resource_type': _SERVICE_RESOURCE_TYPE,
            'current_sender_id': sender_id,
            'service_id': service_id ,
            'instances': instances, 
            'timestamp': int(time.time()) 
        }

        # create and start our listener
        logging.debug("_register_service starting service response listener (service_id, service_addr, sender_id): (%s,%s,%s)" % (service_id, service_addr, sender_id))        
        if register_resource(resource, key):
            # Start our listener to receive service responses
            coro_spawn(
                service_response_listener,
                application,
                'registration_listener',
                service_id, 
                service_addr, 
                service_resp_addr, 
                service_conn, 
                service_passphrase
            )
            #logging.debug("_register_service success: %s" % key)
            # give above process a chance to start
            coro_sleep(0)
        else:
            logging.debug("_register_service failure: %s" % key)

    else:
        # we may just want to add our instance
        resource = get_resource(key)
        instances = resource['instances']
        sender_id = service_conn.sender_id
        
        if instances is None:
            instances = {}

        if not sender_id in instances:
            #logging.debug("adding %s instance: %s " % (service_id, sender_id))
            instance = {            
                'sender_id': sender_id ,
                'service_conn': service_conn, 
                'waiting_clients': {}, 
            }
            instances[sender_id] = instance 

        resource['current_sender_id'] = service_conn.sender_id
            
        logging.debug("register_service service_conn reconnect(%s:%s:%s)" % (
            service_conn.in_addr, service_conn.out_addr, service_conn.sender_id))

        #logging.debug("register_service  service_conn.sender_id: %s" %
        # (service_conn.sender_id))
        #logging.debug("register_service  application.msg_conn.sender_id: %s" %
        # (application.msg_conn.sender_id))

        # Start our listener to receive service responses
        coro_spawn(
            service_response_listener,
            application,
            'registration_listener',
            service_id, 
            service_addr, 
            service_resp_addr, 
            service_conn, 
            service_passphrase
        )

        service_conn.reconnect(service_conn.sender_id)
        logging.debug("register_service ignored: %s already registered" % service_id)
    return True

def _unregister_service(application, service_id, sender_id):
    """ unregister a service.
    """
    if not _service_is_registered(application, service_id):
        logging.debug("unregister_resource ignored: (%s,%s) not registered" % (service_id, sender_id))
        return False
    else:
        
        service_info = _get_service_info(application, service_id, sender_id)

        if not service_info is None:
            sender_id = service_info['sender_id']
            service_conn = service_info['service_conn']
            waiting_clients = service_info['waiting_clients']
        else:
            logging.debug("unregister_resource ERROR: (%s,%s) no instances." % (service_id, sender_id))
        conn = None
        # should pause a bit here waiting for final responses
        # would need to
        #for sock in waiting_clients:
        #    sock.close()
                        
        key = create_resource_key(service_id, _SERVICE_RESOURCE_TYPE)    

        resource = get_resource(key)
        instances = resource['instances']
        #service_conn = resource['service_conn']
        
        waiting_clients = service_info['waiting_clients']
        conn = service_info['service_conn']
        if not conn is None:
            #logging.debug("conn closing incoming")
            close_in(conn)
            #logging.debug("conn waiting for current clients to finish: %s" % len(waiting_clients))
            stime = int(time.time())
            timeout = 5
            while  (int(time.time()) - timeout) < stime and len(waiting_clients) > 0:
                coro_sleep(1)
            #logging.debug("conn closing outgoing")
            close_out(conn)
        logging.debug("removing instance: %s" % sender_id)                    
        del instances[sender_id]


        if len(instances) == 0:
            # really unregister us
            unregister_resource(key, sender_id)
            logging.debug("unregister_resource success: %s" % key)   
            #service_conn.reconnect(sender_id)
            if isinstance(service_info, Resource):
                service_info._on_unregistered()
        else:
            #logging.debug("unregister_resource service_conn: %s" % 
            # ( id(service_conn) ))
            #logging.debug("unregister_resource conn: %s" % (id(conn)))
            
            logging.debug("unregister_resource conn == service_conn: %s" % (conn == service_conn))                    
            if conn == service_conn:
                #logging.debug("unregister_resource reassinging service_conn: %s" % (key))                    
                resource['service_conn']=instances.itervalues().next()
            logging.debug("unregister_service partial success: %s:%s" % (service_id, len(instances)))
            
        return True

## message listener
def service_response_listener(application, service_id, service_addr, service_resp_addr, service_conn, service_passphrase):
    """Function runs in a coroutine, one listener for each server per handler.
    Once running, it stays running until the brubeck instance is killed."""
    try:
        logging.debug("service_response_listener: (service_id, service_addr, service_resp_addr,service_passphrase): (%s:%s:%s:%s)" % (
            service_id, service_addr, service_resp_addr, service_passphrase))
        loop = True
        key = create_resource_key(service_id, _SERVICE_RESOURCE_TYPE)  
        while is_resource_registered(key) and loop == True:
            logging.debug("service_response_listener waiting")
            raw_response = service_conn.recv()
            logging.debug("service_response_listener recv(): %s" % raw_response)
            # just send raw message to connection client
            if raw_response is None:
                loop = False
            else:
                sender, conn_id = raw_response.split(' ', 1)
                conn_id = t_parse(conn_id)[0]
                sender_id = sender
                _notify_waiting_service_client(application, service_id, conn_id, raw_response, sender_id)
                # Call our handler
                (response, handler_response) = service_conn.process_message(
                    application,
                    raw_response,
                    service_addr,
                    service_passphrase,
                )
        service_conn.close()

        logging.debug("service_response_listener ending: (service_id, service_addr, service_resp_addr,service_passphrase): (%s:%s:%s:%s)" % (
            service_id, service_addr, service_resp_addr, service_passphrase))
        
    except Exception, e:
        logging.error(e, exc_info=True)

# functions for parsing messages
def parse_service_request(msg, passphrase):
    """Function for constructing a Request instance out of a
    message read straight off a zmq socket from a ServiceClientConnection.
    """
    if msg is None:
        return None
        
    #logging.debug("parse_service_request: %s" % msg)
    fields = (sender, conn_id, request_timestamp, msg_passphrase, 
    origin_sender_id, origin_conn_id, origin_out_addr, handle_response, 
    path, method, rest) = msg.strip().split(' ', 10)
    # first field is not tnetstring, no need to do anything
    # last is group of tnetstrings, will handle after
    i=1
    for field in fields[1:-1]:
        fields[i] = t_parse(field)[0]
        i+=1
    # our minimal "security" 
    if fields[3] != passphrase:
        raise ServiceError('Unknown service identity! (%s != %s)' % (str(fields[3]),str(passphrase)))
    # handle the "body" of the message or last parsed field
    arguments, rest = t_parse(rest)
    headers, rest = t_parse(rest)
    body = t_parse(rest)[0]
    arguments = json.loads(arguments) if len(arguments) > 0 else {}
    headers = json.loads(headers) if len(headers) > 0 else {}
    body = json.loads(body) if len(body) > 0 else {}
    # create our sevice request object
    r = ServiceRequest(**{
            "sender": fields[0],
            "conn_id": fields[1],
            "request_timestamp": fields[2],
            "origin_sender_id": fields[4],
            "origin_conn_id": fields[5],
            "origin_out_addr": fields[6],
            "handle_response": fields[7],
            "path": fields[8],
            "method": fields[9],
            "arguments": arguments,
            "headers": headers,
            "body": body,
    })
    return r

#############################################
## Functions for sending the service request
#############################################
def _send_service_request(application, service_id, service_req):
    """send our message, used internally only.
    returns a tuple with the first element the sender_id and the second the response"""
    service_info = _get_service_info(application, service_id)
    sender_id = None
    if not service_info is None:
        #for key in service_info.keys():
        #    logging.debug("service_info key: %s" % key)
        if 'current_sender_id' in service_info:
            sender_id = service_info['current_sender_id']
        elif 'sender_id' in service_info:
            sender_id = service_info['sender_id']
    #logging.debug("sending service request")
    service_conn = _get_service_conn(application, service_id, sender_id)
    #logging.debug("service_conn: %s"  % service_conn)
    return (sender_id, service_conn.send(service_req))


def _get_service_info(application, service_id, sender_id=None, timeout=10):
    """every internal function needs to use this to get a service_info.
    will wait for a service_info if there is none registered."""
    logging.debug("_get_service_info application.msg_conn.sender_id, sender_id: %s,%s" % 
        (application.msg_conn.sender_id, sender_id))
    service_info = None
    key = create_resource_key(service_id, _SERVICE_RESOURCE_TYPE)
    if _service_is_registered(application, service_id):
        service_info = get_resource(key)
        if sender_id is None:
            return service_info
    else:
        timestart = int(time.time())
        time_timeout = (timestart + timeout)
        now = int(time.time())
        service_info = None
        #logging.debug('timestart + timeout: %s + %s = %s' % (timestart, timeout, time_timeout))
        #logging.debug('now: %s' % (now))
        while service_info is None and now < time_timeout:
            service_info = get_resource(key)
            if(service_info is None):
                coro_sleep(1)
                logging.debug('waiting for service_info (%s): %s' % (key, now))
                now = int(time.time())
        if timeout >0:
            if service_info is None:
                logging.debug("_get_service_info timed out (%s)" % timeout)
            else:
                logging.debug("_get_service_info success waited %s" % (now-timestart))
        elif service_info is None:
            logging.debug("_get_service_info service_info not found")

    if not service_info is None:
        instances = service_info['instances']

        if sender_id is None or not sender_id in instances:
            sender_id = service_info['current_sender_id']
            logging.debug('using current sender_id: %s' % sender_id)
                
        if sender_id in instances:
            instance = instances[sender_id]
            return instance

    return service_info
        #raise ServiceError("%s service not registered" % service_id)

def _get_service_conn(application, service_id, sender_id=None, timeout=10):
    """get the ServiceClientConnection for a service.
    uses _get_service_info, so it will wait for a resource if there is none initialy.
    """
    service_info = _get_service_info(application, service_id, sender_id)
    if service_info is None:
        # logging.debug("_get_service_conn service_info not found")
        return None
    else:
        # logging.debug("_get_service_conn service_conn: %s" % service_info['service_conn'])
        return service_info['service_conn'] 

def get_service_keys():
    """ get our list of resources.
    """ 
    _resource_keys = get_resource_keys()
    _service_keys = []
    for key in _resource_keys:
        key_parts = key.split(":")
        if key_parts[0]==_SERVICE_RESOURCE_TYPE:
            _service_keys.append(key)
    return _service_keys


# functions for creating messages
def create_service_response(service_request, handler, method=_DEFAULT_SERVICE_REQUEST_METHOD, arguments={}, msg={}, headers={}):
    """Function for creating a ServiceResponse object to send."""
    if not isinstance(headers, dict):
        headers = json.loads(headers)
    if not isinstance(msg, dict):
        msg = json.loads(msg)

    service_response = ServiceResponse(**{
        "sender": service_request.sender,
        "conn_id": service_request.conn_id,
        "origin_sender_id": service_request.origin_sender_id,
        "origin_conn_id": service_request.origin_conn_id,
        "origin_out_addr": service_request.origin_out_addr,
        "handle_response": service_request.handle_response,
        "path": service_request.path,
        "method": method,
        "arguments": arguments,
        "headers": headers,
        "body": msg,
        "status_code": handler.status_code,
        "status_msg": handler.status_msg,
    })

    return service_response
def _notify_waiting_service_client(application, service_id, conn_id, raw_results, sender_id):
    """Notify waiting events if they exist."""
    waiting_clients = _get_waiting_service_clients(application, service_id, sender_id)
    logging.debug("waiting_clients: %s" % (waiting_clients))
    conn_id = str(conn_id)
    if not waiting_clients is None and conn_id in waiting_clients:
        logging.debug("conn_id %s found to notify(%s)" % (conn_id,waiting_clients[conn_id]))
        coro_send_event(waiting_clients[conn_id][1], raw_results)
        del waiting_clients[conn_id]
        #logging.debug("conn_id %s sent to: %s" % (conn_id, raw_results))
        coro_sleep(0)
        return True
    else:
        logging.debug("conn_id %s not found to notify." % conn_id)
        return False

def _get_waiting_service_clients(application, service_id, sender_id):
    """get the waiting service clients."""
    service_info = _get_service_info(application, service_id, sender_id)
    if service_info is None:
        return None
    else:     
        return service_info['waiting_clients']

## Connection functions

def close(conn):
    """close all connections"""
    conn.out_sock.close()
    conn.in_sock.close()

def close_out(conn):
    """close our outgoing connection"""
    conn.out_sock.close()

def close_in(conn):
    """close our incoming connection"""
    conn.in_sock.close()