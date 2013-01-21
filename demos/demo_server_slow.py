#!/usr/bin/env python
import logging
import time
from brubeck.connections import Mongrel2Connection
from brubeck.request_handling import (
    Brubeck,
    render,
)
import gevent

##
## runtime configuration
##
config = {
    'msg_conn': Mongrel2Connection('tcp://127.0.0.1:9999',
                                     'tcp://127.0.0.1:9998'),
    'log_level': logging.DEBUG,
}

##
## get us started!
##
app = Brubeck(**config)

@app.add_route('^/slow$', method='GET')
def SlowFunction(application, message):
     logging.debug("Slow Function starting %s." % message.conn_id)
     gevent.sleep(5)        
     logging.debug("Slow Function done.")
     return render("Slow function...", 200, 'OK', {})

@app.add_route('^/slow/1', method='GET')
def SlowFunction1(application, message):
     logging.debug("Slow Function 1 starting %s." % message.conn_id)
     gevent.sleep(5)        
     logging.debug("Slow Function 1 done.")
     return render("Slow function 1...", 200, 'OK', {})

@app.add_route('^/slow/2', method='GET')
def SlowFunction2(application, message):
     logging.debug("Slow Function 2 starting %s." % message.conn_id)
     gevent.sleep(5)        
     logging.debug("Slow Function 2 done.")
     return render("Slow function 2...", 200, 'OK', {})

@app.add_route('^/slow/3', method='GET')
def SlowFunction3(application, message):
     logging.debug("Slow Function 3 starting %s." % message.conn_id)
     gevent.sleep(5)        
     logging.debug("Slow Function 3 done.")
     return render("Slow function 3...", 200, 'OK', {})

@app.add_route('^/slow/4', method='GET')
def SlowFunction4(application, message):
     logging.debug("Slow Function 4 starting %s." % message.conn_id)
     gevent.sleep(5)        
     logging.debug("Slow Function 4 done.")
     return render("Slow function 4...", 200, 'OK', {})
    
## start our server to handle requests
if __name__ == "__main__":
    app.run()
