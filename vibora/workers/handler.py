import asyncio
import logging
import os
import signal
import time
from inspect import isawaitable
from socket import IPPROTO_TCP, TCP_NODELAY, SO_REUSEADDR, SOL_SOCKET, SO_REUSEPORT, socket
from multiprocessing import Process
from functools import partial
from .reaper import Reaper
from ..hooks import Events
from ..utils import asynclib


logger = logging.getLogger('vibora.workers')


class CleanupRegistry:
    """
    Tracks pending async tasks and user-registered resource callbacks
    (database pools, external clients, etc.) so everything can be released
    in an orderly fashion during the graceful shutdown of a worker.
    """

    def __init__(self, app, loop=None):
        self.app = app
        self.loop = loop
        self.tasks = set()
        self.callbacks = []

    def bind_loop(self, loop):
        self.loop = loop

    def register_callback(self, callback):
        """
        Registers a sync or async callable to run during shutdown.
        Use it to close database connections, flush caches and similar.
        """
        self.callbacks.append(callback)
        return callback

    def track_task(self, task):
        """
        Attaches the task to the registry so it can be awaited/cancelled
        while the worker is shutting down.
        """
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    def create_task(self, coro):
        return self.track_task(self.loop.create_task(coro))

    async def cancel_pending_tasks(self):
        """
        Cancels every task still alive (other than the shutdown task itself)
        and waits for their cancellation to be processed.
        """
        try:
            all_tasks = asyncio.all_tasks(loop=self.loop)
        except TypeError:  # pragma: no cover - Python < 3.7 fallback
            all_tasks = asyncio.Task.all_tasks(loop=self.loop)
        current = asyncio.current_task(loop=self.loop) if hasattr(asyncio, 'current_task') else asyncio.Task.current_task(self.loop)
        pending = [task for task in all_tasks if task is not current and not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def run_callbacks(self):
        """
        Executes every registered cleanup callback (in registration order).
        Exceptions are logged but never abort the shutdown sequence.
        """
        for callback in self.callbacks:
            try:
                result = callback()
                if isawaitable(result):
                    await result
            except Exception as error:
                logger.exception('Shutdown callback %r failed: %s', callback, error)

    async def shutdown(self):
        """
        Cancels pending tasks first (stopping the workload) and only then
        runs the resource release callbacks (database pools, etc.).
        """
        await self.cancel_pending_tasks()
        await self.run_callbacks()


def on_shutdown(app, callback):
    """
    Public helper for registering resource release callbacks.

        from vibora.workers.handler import on_shutdown
        on_shutdown(app, db_pool.close)

    Safe to call before the worker forks: callbacks are carried in the app
    object and re-used by every worker process.
    """
    registry = getattr(app, 'cleanup', None)
    if registry is None:
        registry = CleanupRegistry(app)
        app.cleanup = registry
    return registry.register_callback(callback)


def track_task(app, task):
    """
    Registers an asyncio task for graceful cancellation during shutdown.
    """
    registry = getattr(app, 'cleanup', None)
    if registry is None:
        registry = CleanupRegistry(app)
        app.cleanup = registry
    return registry.track_task(task)


class RequestHandler(Process):

    def __init__(self, app, bind: str, port: int, sock=None):
        super().__init__()
        self.app = app
        self.bind = bind
        self.port = port
        self.daemon = True
        self.socket = sock

    def run(self):

        # Re-using address and ports. Kernel is our load balancer.
        if not self.socket:
            self.socket = socket()
            self.socket.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)
            self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            self.socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
            self.socket.bind((self.bind, self.port))

        # Creating a new event loop using a faster loop.
        loop = asynclib.new_event_loop()
        loop.app = self.app
        self.app.loop = loop
        self.app.components.add(loop)
        asyncio.set_event_loop(loop)

        # Process-level status exposed through the /healthz endpoint.
        self.app.started_at = getattr(self.app, 'started_at', None) or time.time()
        self.app.shutting_down = False

        # Registry used by the graceful shutdown flow to cancel pending
        # tasks and to release user resources (database connections, etc.).
        cleanup = getattr(self.app, 'cleanup', None)
        if cleanup is None:
            cleanup = CleanupRegistry(self.app)
            self.app.cleanup = cleanup
        cleanup.bind_loop(loop)

        # Starting the connection reaper.
        self.app.reaper = Reaper(app=self.app)
        self.app.reaper.start()

        # Registering routes, blueprints, handlers, callbacks, everything is delayed until now.
        self.app.initialize()

        # Calling before server start hooks (sync/async)
        loop.run_until_complete(self.app.call_hooks(Events.BEFORE_SERVER_START, components=self.app.components))

        # Creating the server.
        handler = partial(self.app.handler, app=self.app, loop=loop, worker=self)
        ss = loop.create_server(handler, sock=self.socket, reuse_port=True, backlog=1000)

        # Calling after server hooks (sync/async)
        server = loop.run_until_complete(ss)
        loop.run_until_complete(self.app.call_hooks(Events.AFTER_SERVER_START, components=self.app.components))

        async def stop_server(timeout=30):

            # Stop the reaper.
            self.app.reaper.has_to_work = False

            # Signal the health check endpoint (and any interested user)
            # that this worker is leaving the pool.
            self.app.shutting_down = True

            # Stop accepting new connections from the listening socket.
            server.close()
            await server.wait_closed()

            # Calling the before server stop hook.
            await self.app.call_hooks(Events.BEFORE_SERVER_STOP, components=self.app.components)

            # Ask all connections to finish as soon as possible.
            for connection in self.app.connections.copy():
                connection.stop()

            # Waiting all connections finish their tasks, after the given timeout
            # the connection will be closed abruptly.
            deadline = timeout
            while deadline:
                all_closed = True
                for connection in self.app.connections:
                    if not connection.is_closed():
                        all_closed = False
                        break
                if all_closed:
                    break
                deadline -= 1
                await asyncio.sleep(1)

            # Some connections may refuse to drain in time: close them abruptly
            # so the shutdown never hangs forever.
            for connection in self.app.connections.copy():
                if not connection.is_closed():
                    connection.close()

            # Cancel pending async tasks and run the registered cleanup
            # callbacks (database pools, caches, etc.) before leaving.
            await cleanup.shutdown()

            logger.info('Worker (pid=%s) finished graceful shutdown.', os.getpid())
            loop.stop()

        def handle_kill_signal():
            # Stop receiving new connections at kernel level too (in case
            # another process inherited the socket) and start the drain.
            try:
                self.socket.close()
            except OSError:
                pass
            cleanup.create_task(stop_server(10))

        try:
            loop.add_signal_handler(signal.SIGTERM, handle_kill_signal)
            try:
                loop.add_signal_handler(signal.SIGINT, handle_kill_signal)
            except (NotImplementedError, AttributeError):  # pragma: no cover - Windows
                pass
            loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            handle_kill_signal()
            loop.run_forever()
        finally:
            loop.close()

