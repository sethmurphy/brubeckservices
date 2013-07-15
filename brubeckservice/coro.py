import logging
from brubeck.request_handling import (
    CORO_LIBRARY,
)

################################################################
### Attempt to setup gevent wrappers for sleep, events and more
### Always prefere gevent if installed, then try eventlet
################################################################
coro_pool = {}
if CORO_LIBRARY == 'gevent':
    from gevent import sleep
    from gevent.event import AsyncResult
    from gevent import pool

    def coro_sleep(secs):
        sleep(secs)

    def coro_get_event():
        return AsyncResult()

    def coro_send_event(e, value):
        e.set(value)

    def coro_create_pool():
        return pool.Pool()

    def coro_pool_wait_available(pool):
        if pool is None:
            return 0
        return pool.waiting()

    def _coro_spawn(p, function, app, *a, **kw):
        return p.spawn(function, app, *a, **kw)

    CORO_LIBRARY = 'gevent'

elif CORO_LIBRARY == 'eventlet':
    from eventlet import event
    from eventlet import sleep

    def coro_sleep(secs):
        sleep(secs)

    def coro_get_event():
        return AsyncResult()

    def coro_send_event(e, value):
        e.set(value)

    def coro_create_pool():
        return eventlet.GreenPool()

    def coro_pool_wait_available(pool):
        if pool is None:
            return 0
        return pool.wait_available()

    def _coro_spawn(p, function, app, *a, **kw):
        return p.spawn_n(function, app, *a, **kw)

    CORO_LIBRARY = 'eventlet'

# common coro functions using above wrapper functions
def coro_conn(app, tag = None):
    """get a coro pool"""
    if tag is None or tag == 'coro_pool':
        logging.debug("coro_conn returning app pool.")
        return app.pool
    elif tag in coro_pool:
        logging.debug("coro_conn returning existing pool for %s" % tag)
        return coro_pool[tag]
    else:
        coro_pool[tag] = coro_create_pool()
        logging.debug("coro_conn returning new pool for %s" % tag)
        return coro_pool[tag]

def coro_spawn(function, app, tag = None, *a, **kw):
    """spawn a coro"""
    logging.debug("getting pool for %s" % tag)
    pool = coro_conn(app, tag)
    logging.debug("pool for %s: %s" % (tag, pool))
    return _coro_spawn(pool, function, app, *a, **kw)

def coro_count(app, tag):
    """get the count of a coro pool"""
    p = None
    if tag is None or tag == 'coro_pool':
        return app.pool.wait()
    elif tag in coro_pool:
        return coro_pool_wait_available(coro_pool[tag])
    else:
        return 0
