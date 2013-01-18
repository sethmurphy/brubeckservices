// Demo go brubeck service
//
// Author: Seth Murphy   github.com/sethmurphy
// Requires: http://github.com/alecthomas/gozmq
//
package main

import (
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	//tnetstring "github.com/edsrzf/tnetstring-go"
	"time"
	//"encoding/json"
    "strings"
    "strconv"
    "errors"
)

type brubeckServiceRequest struct {
    // Class used to construct a Brubeck service request message.
    // Both the client and the server use this.
    // this is set by the send call in the client connection
    sender string
    // this is set by the send call in the client connection
    conn_id string
    // Not sure if this is the socket_id, but it is used to return the message to the originator
    origin_sender_id string
    // This is the connection id used by the originator and is needed for Mongrel2
    origin_conn_id string
    // This is the socket address for the reply to the client
    origin_out_addr string
    // used to route the request
    path string
    // used to route the request to the proper method of the handler
    method string
    // a dict, used to populat an arguments dict for use within the method
    arguments map[string] string
    // a dict, right now only METHOD is required and must be one of: ['get', 'post', 'put', 'delete','options', 'connect', 'response', 'request']
    headers map[string] string
    // a dict, this can be whatever you need it to be to get the job done.
    body string
}

func createBrubeckServiceRequest(service_request map[string] string) *brubeckServiceRequest {
    body := service_request["body"]
    return &brubeckServiceRequest{
        "", service_request["conn_id"], 
        service_request["origin_sender_id"],
        service_request["origin_conn_id"],
        service_request["origin_out_addr"],
        service_request["path"],
        service_request["method"],
        nil, nil, body,
    }

}

type brubeckServiceResponse struct {
    // Class used to construct a Brubeck service request message.
    // Both the client and the server use this.
    // this is set by the send call in the client connection
    sender string
    // this is set by the send call in the client connection
    conn_id string
    // Not sure if this is the socket_id, but it is used to return the message to the originator
    origin_sender_id string
    // This is the connection id used by the originator and is needed for Mongrel2
    origin_conn_id string
    // This is the socket address for the reply to the client
    origin_out_addr string
    // used to route the request
    path string
    // used to route the request to the proper method of the handler
    method string
    // a dict, used to populate an arguments dict for use within the method
    arguments map[string] string
    // a dict, right now only METHOD is required and must be one of: ['get', 'post', 'put', 'delete','options', 'connect', 'response', 'request']
    headers map[string] string
    // a dict, this can be whatever you need it to be to get the job done.
    body string

    // the status code of the response
    status_code int
    // a message associated with the status code
    status_message string

}

func createBrubeckServiceResponse(service_request *brubeckServiceRequest, 
        status_code int, status_message string, 
        method string, arguments map[string] string, body string, headers map[string] string) *brubeckServiceResponse {
    return &brubeckServiceResponse{
        "", service_request.conn_id, 
        service_request.origin_sender_id, service_request.origin_conn_id, service_request.origin_out_addr,
        service_request.path, method, arguments,
        headers, body, status_code, status_message,
    }
}

// parse a raw tnetstring field in the format:
// [int]:[message]
// where [int] is the length of [string]
// if the value to teh left of the first ':' is not an int
// or the value to the right of the first ':' does not match the size
// an error of NOT nil will be returned
func parse_msg_string(text string) (string, error) {
    field_parts := strings.SplitN(text, ":", 2)
    field_len, err := strconv.Atoi(field_parts[0])
    if err != nil {
        // handle error
        fmt.Println(field_parts[0], err)
        return "", err
    }
    field_val := field_parts[1]
    if len(field_val) != field_len {
        fmt.Println(field_parts[1], "Wrong field length, should be " , field_len, ". is ", len(field_val), "'", field_val,"'")
        return "", err
    }
    println("field_len", field_len)
    println("field_val", field_val)
    return field_val, nil
}

// Get the name of the field in a raw request message by index
func service_request_field_name(index int) (string, error) {
    field_name := ""
    switch index {
    default: return field_name, errors.New("Invalid service_request_field_name index, must be > 0 and < 10")
    case 0: field_name = "conn_id" 
    case 1: field_name = "start_timestamp" 
    case 2: field_name = "end_timestamp"
    case 3: field_name = "msg_passphrase"
    case 4: field_name = "origin_sender_id"
    case 5: field_name = "origin_conn_id"
    case 6: field_name = "origin_out_addr"
    case 7: field_name = "path"
    case 8: field_name = "method"
    case 9: field_name = "body"
    }
    return field_name, nil
 }

// Parse a raw service request's message:
// The following fields are returned in a map
//  conn_id, start_timestamp, end_timestamp, msg_passphrase,
//  origin_sender_id, origin_conn_id, origin_out_addr, 
//  path, method, body
// This parses the envelope only and not the message body 
func parse_service_request(text string) (map[string] string, error) {
	msg_parts := strings.SplitN(strings.Trim(text, " "),  " ", 10)
    var request_field_values = make(map[string]string, 10)
    for i := 0; i < len(msg_parts); i ++ {
        var field_val, err = parse_msg_string(msg_parts[i])
        if err == nil {
            var field_name, err = service_request_field_name(i)
            if err == nil {
                request_field_values[field_name] = field_val                
            } else {
                return request_field_values, err
            }
        } else {
            return request_field_values, err
        } 
    }
    return request_field_values, nil
}

// Our demo handler
func demo_handler(request *brubeckServiceRequest) (int, string, string) {
    return 200, "success", "hello"
}

// This is where the work gets delegated
func handle_request(service_request *brubeckServiceRequest, socket zmq.Socket) {
    // run in a goroutine
    go func(){
        // handle request
        //   1. get method to call in routing table
        my_handler := demo_handler // fake it for now

        //   2. call method which should return a value which is the body of the response.
        var status_code, status_message, response_body =  my_handler(service_request)

        var method string 
        var arguments map[string] string
        var headers map[string] string 

        // send the request back 
        //   1. create a response object
        service_response := createBrubeckServiceResponse(service_request, 
        status_code, status_message, 
        method, arguments, response_body, headers)

        //   2. call method to send response
        send_response(service_response, socket)
        
        // We are done, nothing to report
        return
        
    }()
    // We will be done in a while, nothing to report
    return
}

// Create and send our ZMQ response
func send_response(response *brubeckServiceResponse, socket zmq.Socket) {
    // build our ZMQ message string from the response
    var msg string
    
    // send our message
    socket.Send([]byte(msg), 0)
    
    // We are done, nothing to report
    return
}

func main() {
    test_msg := " 36:4d80af85-5e31-4322-9b27-8e9fbb5ae69f 13:1358433619512 13:1358433619512 16:my_shared_secret 36:34f9ceee-cd52-4b7f-b197-88bf2f0ec378 1:1 20:tcp://127.0.0.1:9998 13:/service/slow 7:request 75:{\"RETURN_DATA\":\"I made a round trip, it took a while but I bring results.\"}2:{}2:{}"
    context, _ := zmq.NewContext()
	insocket, _ := context.NewSocket(zmq.DEALER)
	outsocket, _ := context.NewSocket(zmq.DEALER)
	defer context.Close()
	defer insocket.Close()
	insocket.Bind("ipc://run/slow")
	outsocket.Bind("ipc://run/slow/response")

    var request_fields, err = parse_service_request(test_msg)
    if err == nil {
        handle_request(createBrubeckServiceRequest(request_fields), outsocket)
    }
    println("Go Brubeck Service v0.1.0 online ]-----------------------------------")
	// Wait for messages
	for {
		msg, _ := insocket.Recv(0)
		println("Received ", string(msg))
		println("Received ", string(test_msg))

        //"""This is what we did in the python demo"""
        //self.set_status(200, "Took a while, but I am back.")
        //self.add_to_payload("RETURN_DATA", self.message.get_argument("RETURN_DATA", "NO DATA"))
        //self.headers = {"METHOD": "response"}

        //"""do something and take too long"""
        time.Sleep(time.Second * 5)
        

		// send reply back to client
		reply := fmt.Sprintf("Took a while, but I am back.")
		insocket.Send([]byte(reply), 0)
	}
}