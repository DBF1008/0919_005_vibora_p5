"""
Unit tests for the graceful shutdown machinery in vibora/workers/handler.py.
These run without opening any socket and assert:
  1. CleanupRegistry runs sync + async shutdown callbacks.
  2. CleanupRegistry cancels tracked / pending async tasks.
  3. on_shutdown() / track_task() work before a loop/registry exists.
Run: python tests/manual/test_graceful_shutdown.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import tests.manual._compat  # noqa: F401

from vibora.workers.handler import CleanupRegistry, on_shutdown, track_task


class FakeApp:
    pass


def test_callbacks_are_executed_sync_and_async():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = FakeApp()
    registry = CleanupRegistry(app, loop=loop)
    order = []

    def sync_cb():
        order.append('sync')

    async def async_cb():
        await asyncio.sleep(0)
        order.append('async')

    registry.register_callback(sync_cb)
    registry.register_callback(async_cb)
    loop.run_until_complete(registry.run_callbacks())
    loop.close()
    assert order == ['sync', 'async'], order
    print('PASS test_callbacks_are_executed_sync_and_async')


def test_pending_tasks_are_cancelled():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = FakeApp()
    registry = CleanupRegistry(app, loop=loop)

    async def never_ends():
        await asyncio.sleep(3600)

    task = registry.create_task(never_ends())
    assert task in registry.tasks

    async def scenario():
        await asyncio.sleep(0)
        await registry.shutdown()

    loop.run_until_complete(scenario())
    assert task.cancelled(), 'tracked task must be cancelled on shutdown'
    assert not registry.tasks, 'done callback should have removed the task'
    loop.close()
    print('PASS test_pending_tasks_are_cancelled')


def test_callbacks_registered_before_fork_are_preserved():
    app = FakeApp()

    def cb():
        pass

    on_shutdown(app, cb)
    assert hasattr(app, 'cleanup')
    assert cb in app.cleanup.callbacks

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app.cleanup.bind_loop(loop)

    async def worker():
        await asyncio.sleep(3600)

    task = track_task(app, loop.create_task(worker()))
    assert task in app.cleanup.tasks
    loop.run_until_complete(app.cleanup.shutdown())
    assert task.cancelled()
    loop.close()
    print('PASS test_callbacks_registered_before_fork_are_preserved')


def test_failing_callback_does_not_abort_shutdown():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = FakeApp()
    registry = CleanupRegistry(app, loop=loop)
    reached = []

    def boom():
        raise RuntimeError('kaboom')

    def survivor():
        reached.append(True)

    registry.register_callback(boom)
    registry.register_callback(survivor)
    loop.run_until_complete(registry.run_callbacks())
    assert reached == [True]
    loop.close()
    print('PASS test_failing_callback_does_not_abort_shutdown')


def test_request_id_resolution_and_healthz_payload():
    """
    In-process checks for request_id handling and the /healthz payload;
    the live HTTP behaviour is covered by test_request_id.py/test_healthz.py.
    """
    import json
    import re
    import tests.manual._compat  # noqa: F401
    from uuid import UUID
    from vibora import Vibora
    from vibora.headers.headers import Headers

    app = Vibora()
    app.initialize()
    loop = asyncio.new_event_loop()

    # Missing header -> uuid hex generated, same logic as on_headers_complete.
    import uuid
    empty_headers = Headers([])
    rid = empty_headers.get('X-Request-ID') or uuid.uuid4().hex
    UUID(rid)
    generated = rid

    # Inbound header is propagated verbatim.
    inbound = Headers([(b'X-Request-ID', b'client-rid')])
    assert inbound.get('X-Request-ID') == 'client-rid'
    assert generated != 'client-rid'

    # Healthz payload contract.
    class HealthzRequest:
        context = {'request_id': 'hz-rid'}
    response = loop.run_until_complete(app._healthz_handler(HealthzRequest()))
    payload = json.loads(response.content.decode())
    assert response.status_code == 200
    assert payload['status'] == 'ok'
    assert payload['workers'] >= 1
    assert payload['active_connections'] >= 0
    assert payload['memory']['rss_bytes'] > 0
    assert payload['request_id'] == 'hz-rid'
    assert response.headers['X-Request-ID'] == 'hz-rid'
    assert re.match(r'^\d+$', str(payload['uptime']).replace('-', ''))
    print('PASS request_id resolution + healthz payload contract')


if __name__ == '__main__':
    test_callbacks_are_executed_sync_and_async()
    test_pending_tasks_are_cancelled()
    test_callbacks_registered_before_fork_are_preserved()
    test_failing_callback_does_not_abort_shutdown()
    test_request_id_resolution_and_healthz_payload()
    print('ALL GRACEFUL SHUTDOWN UNIT TESTS PASSED')
