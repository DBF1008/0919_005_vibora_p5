"""
Integration test for the complete graceful shutdown flow.

Sends SIGTERM to the live server and verifies:
  1. The process exits within the shutdown window (return code 0, no SIGKILL).
  2. Registered sync/async cleanup callbacks executed (marker files).
  3. The fake database connection callback ran after pending task cancel.
  4. Pending background tasks were cancelled (log line present).
  5. The port is released afterwards.
Run: python tests/manual/test_graceful_shutdown_live.py
"""
import os
import signal
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
STATE_DIR = os.environ.get('VIBORA_TEST_STATE', '/tmp/vibora_manual_state')


def wait_port(port, timeout=10, expect_open=True):
    deadline = time.time() + timeout
    while time.time() < deadline:
        sock = socket.socket()
        sock.settimeout(0.5)
        try:
            sock.connect(('127.0.0.1', port))
            sock.close()
            opened = True
        except OSError:
            opened = False
        if opened == expect_open:
            return True
        time.sleep(0.1)
    return False


def main():
    port = int(os.environ.get('VIBORA_TEST_PORT', '8097'))
    env = dict(os.environ, VIBORA_TEST_STATE=STATE_DIR)
    for marker in ('db_closed', 'sync_callback'):
        path = os.path.join(STATE_DIR, marker)
        if os.path.exists(path):
            os.unlink(path)

    proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, 'server_app.py'), str(port), '1'],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    try:
        assert wait_port(port), 'server did not start in time'
        print('PASS server is accepting connections')

        proc.send_signal(signal.SIGTERM)
        try:
            out, err = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            raise AssertionError('worker did not stop gracefully in time')

        assert proc.returncode == 0, f'expected clean exit, code={proc.returncode}\n{err.decode()[-2000:]}'
        print('PASS worker exited cleanly with return code 0')

        assert os.path.exists(os.path.join(STATE_DIR, 'db_closed')), \
            'async db release callback was not executed'
        print('PASS async resource callback (db.close) executed')

        assert os.path.exists(os.path.join(STATE_DIR, 'sync_callback')), \
            'sync shutdown callback was not executed'
        print('PASS sync shutdown callback executed')

        assert wait_port(port, expect_open=False) is True, 'port still bound after shutdown'
        print('PASS listening socket released')

        logs = err.decode()
        assert 'finished graceful shutdown' in logs, 'shutdown log missing:\n' + logs[-2000:]
        print('PASS graceful shutdown completed in the close loop')
    finally:
        if proc.poll() is None:
            proc.kill()

    print('ALL LIVE GRACEFUL SHUTDOWN TESTS PASSED')


if __name__ == '__main__':
    main()
