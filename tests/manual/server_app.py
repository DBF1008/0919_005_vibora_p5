"""
Shared demo app used by the manual test.sh scenarios.
Run directly: python tests/manual/server_app.py <port>
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import tests.manual._compat  # noqa: F401  (test-only Python 3.10 shim)

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s', stream=sys.stderr)

from vibora import Vibora
from vibora.responses import JsonResponse, Response
from vibora.request.request import Request as NativeRequest
from vibora.workers.handler import on_shutdown, track_task


def build_app(workers: int = 1) -> Vibora:
    app = Vibora()

    # Marker files let the shell tests observe the graceful-shutdown side effects.
    state_dir = os.environ.get('VIBORA_TEST_STATE', '/tmp/vibora_manual_state')
    os.makedirs(state_dir, exist_ok=True)

    @app.route('/')
    async def index():
        return JsonResponse({'hello': 'world'})

    @app.route('/echo')
    async def echo(request: NativeRequest):
        return JsonResponse({'request_id': request.context.get('request_id')})

    @app.route('/slow')
    async def slow():
        await asyncio.sleep(5)
        return Response(b'done')

    @app.route('/background')
    async def background():
        async def periodic():
            while True:
                await asyncio.sleep(0.2)

        track_task(app, app.loop.create_task(periodic()))
        return JsonResponse({'spawned': True})

    # Simulated database connection released during graceful shutdown.
    class FakeDB:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True
            with open(os.path.join(state_dir, 'db_closed'), 'w') as handle:
                handle.write('closed')

    db = FakeDB()

    async def release_db():
        await db.close()

    on_shutdown(app, release_db)

    def sync_callback():
        with open(os.path.join(state_dir, 'sync_callback'), 'w') as handle:
            handle.write('ok')

    on_shutdown(app, sync_callback)

    app.workers_count = workers
    return app


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    build_app(workers=workers).run(host='127.0.0.1', port=port, workers=workers, debug=False,
                                   startup_message=False, necromancer=False)
