from brubeck.request_handling import (
    CORO_LIBRARY,
)
#########################################################
### Attempt to setup gevent wrappers for sleep and events
### Always prefere gevent if installed, then try eventlet
#########################################################
if CORO_LIBRARY == 'gevent':
    from gevent import sleep
    from gevent.event import AsyncResult
    from gevent import pool

    coro_pool_in = pool.Pool()
    coro_pool_out = pool.Pool()
    coro_pool_heartbeat = pool.Pool()
    coro_pool_heartbeat_listener = pool.Pool()
    coro_pool_client_heartbeat = pool.Pool()
    coro_pool_client_heartbeat_listener = pool.Pool()
    coro_pool_registration = pool.Pool()
    coro_pool_registration_listener = pool.Pool()

    def coro_conn(app, tag):
        if tag == 'coro_pool':
            return app.pool
        elif tag == 'in':
            return coro_pool_in
        elif tag == 'out':
            return coro_pool_out
        elif tag == 'heartbeat':
            return coro_pool_heartbeat
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener
        elif tag == 'client_heartbeat':
            return coro_pool_client_heartbeat
        elif tag == 'client_heartbeat_listener':
            return coro_pool_client_heartbeat_listener
        elif tag == 'registration':
            return coro_pool_registration
        elif tag == 'registration_listener':
            return coro_pool_registration_listener
        else:
            return app.pool

    def coro_count(app, tag):
        if tag == 'coro_pool':
            return app.pool.wait_available()
        elif tag == 'in':
            return coro_pool_in.wait_available()
        elif tag == 'out':
            return coro_pool_out.wait_available()
        elif tag == 'heartbeat':
            return coro_pool_heartbeat.wait_available()
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener.wait_available()
        elif tag == 'client_heartbeat':
            return coro_pool_client_heartbeat.wait_available()
        elif tag == 'client_heartbeat_listener':
            return coro_pool_client_heartbeat_listener.wait_available()
        elif tag == 'registration':
            return coro_pool_registration.wait_available()
        elif tag == 'heartbeat_listener':
            return coro_pool_registration_listener.wait_available()
        else:
            return app.pool.wait()

    def coro_spawn(function, app, tag, *a, **kw):
        if tag == 'coro_pool':
            return app.pool.spawn(function, app, *a, **kw)
        elif tag == 'in':
            return coro_pool_in.spawn(function, app, *a, **kw)
        elif tag == 'out':
            return coro_pool_out.spawn(function, app, *a, **kw)
        elif tag == 'heartbeat':
            return coro_pool_heartbeat.spawn(function, app, *a, **kw)
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener.spawn(function, app, *a, **kw)
        elif tag == 'client_heartbeat':
            return coro_pool_client_heartbeat.spawn(function, app, *a, **kw)
        elif tag == 'client_heartbeat_listener':
            return coro_pool_client_heartbeat_listener.spawn(function, app, *a, **kw)
        elif tag == 'registration':
            return coro_pool_registration.spawn(function, app, *a, **kw)
        elif tag == 'registration_listener':
            return coro_pool_registration_listener.spawn(function, app, *a, **kw)
        else:
            return app.pool.spawn(function, app, *a, **kw)

    
    def coro_sleep(secs):
        sleep(secs)

    def coro_get_event():
        return AsyncResult()

    def coro_send_event(e, value):
        e.set(value)

    CORO_LIBRARY = 'gevent'

elif CORO_LIBRARY == 'eventlet':
    from eventlet import event
    from eventlet import sleep

    coro_pool_in = eventlet.GreenPool()
    coro_pool_out = eventlet.GreenPool()
    coro_pool_heartbeat= eventlet.GreenPool()
    coro_pool_heartbeat_listener = eventlet.GreenPool()
    coro_pool_client_heartbeat= eventlet.GreenPool()
    coro_pool_client_heartbeat_listener = eventlet.GreenPool()
    coro_pool_registration = eventlet.GreenPool()
    coro_pool_registration_listener = eventlet.GreenPool()
    
    def coro_conn(app, tag):
        if tag == 'coro_pool':
            return app.pool
        elif tag == 'in':
            return coro_pool_in
        elif tag == 'out':
            return coro_pool_out
        elif tag == 'heartbeat':
            return coro_pool_heartbeat
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener
        elif tag == 'client_heartbeat':
            return coro_pool_client_heartbeat
        elif tag == 'client_heartbeat_listener':
            return coro_pool_client_heartbeat_listener
        elif tag == 'registration':
            return coro_pool_registration
        elif tag == 'registration_listener':
            return coro_pool_registration_listener
        else:
            return app.pool

    def coro_count(app, tag):
        if tag == 'coro_pool':
            return app.pool.waiting()
        elif tag == 'in':
            return coro_pool_in.waiting()
        elif tag == 'out':
            return coro_pool_out.waiting()
        elif tag == 'heartbeat':
            return coro_pool_heartbeat.waiting()
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener.waiting()
        elif tag == 'client_heartbeat':
            return coro_pool_client_heartbeat.waiting()
        elif tag == 'client_heartbeat_listener':
            return coro_pool_client_heartbeat_listener.waiting()
        elif tag == 'registration':
            return coro_pool_registration.waiting()
        elif tag == 'registration_listener':
            return coro_pool_registration_listener.waiting()
        else:
            return app.pool.waiting()

    def coro_spawn(function, app, tag, *a, **kw):
        if tag == 'coro_pool':
            return app.pool.spawn_n(function, app, *a, **kw)
        elif tag == 'in':
            return coro_pool_in.spawn_n(function, app, *a, **kw)
        elif tag == 'out':
            return coro_pool_out.spawn_n(function, app, *a, **kw)
        elif tag == 'heartbeat':
            return coro_pool_heartbeat.spawn_n(function, app, *a, **kw)
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener.spawn_n(function, app, *a, **kw)
        elif tag == 'heartbeat':
            return coro_pool_heartbeat.spawn_n(function, app, *a, **kw)
        elif tag == 'heartbeat_listener':
            return coro_pool_heartbeat_listener.spawn_n(function, app, *a, **kw)
        elif tag == 'registration':
            return coro_pool_registration.spawn_n(function, app, *a, **kw)
        elif tag == 'registration_listener':
            return coro_pool_registration_listener.spawn_n(function, app, *a, **kw)
        else:
            return app.pool.spawn_n(function, app, *a, **kw)
        service_coro_pool.spawn_n(function, app, *a, **kw)

    def coro_sleep(secs):
        sleep(secs)

    def coro_get_event():
        return AsyncResult()

    def coro_send_event(e, value):
        e.set(value)

    CORO_LIBRARY = 'eventlet'

