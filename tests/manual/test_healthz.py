"""
Integration test for the built-in /healthz endpoint (vibora/server.py).
Starts a real forked server and verifies over real HTTP:
  - status 200 + JSON fields (status, pid, workers, active_connections,
    memory.rss_bytes, uptime, request_id)
  - active_connections reflects live keep-alive connections
  - header X-Request-ID is present on the health response
Run: python tests/manual/test_healthz.py
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))


def wait_port(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        sock = socket.socket()
        sock.settimeout(0.5)
        try:
            sock.connect(('127.0.0.1', port))
            sock.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def fetch(port, path='/healthz', headers=None):
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', headers=headers or {})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, dict(resp.headers), resp.read()


def main():
    port = int(os.environ.get('VIBORA_TEST_PORT', '8099'))
    proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, 'server_app.py'), str(port), '2'],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    try:
        assert wait_port(port), 'server did not start in time'

        status, headers, body = fetch(port)
        assert status == 200, f'expected 200 got {status}'
        payload = json.loads(body.decode())
        assert payload['status'] == 'ok', payload
        assert payload['workers'] == 2, payload
        assert isinstance(payload['active_connections'], int), payload
        assert payload['memory']['rss_bytes'] > 0, payload
        assert isinstance(payload['pid'], int) and payload['pid'] > 0
        assert isinstance(payload['uptime'], int)
        assert payload['request_id'], 'healthz payload must carry the request id'
        assert headers.get('X-Request-ID') == payload['request_id'], headers
        print('PASS healthz payload + response header:', payload)

        # Hold a raw keep-alive connection open and confirm the counter grows.
        alive = socket.create_connection(('127.0.0.1', port), timeout=5)
        alive.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n')
        assert b'200' in alive.recv(4096)
        deadline = time.time() + 3
        active = 0
        while time.time() < deadline:
            _, _, body = fetch(port)
            active = json.loads(body.decode())['active_connections']
            if active >= 1:
                break
            time.sleep(0.2)
        assert active >= 1, f'active_connections should be >= 1 with open client, got {active}'
        print('PASS active_connections reflects live connections:', active)
        alive.close()
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            out, err = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            print(err.decode()[-2000:])
            raise
        assert proc.returncode == 0, f'worker exited badly: {proc.returncode}\n{err.decode()[-2000:]}'

    print('ALL HEALTHZ TESTS PASSED')


if __name__ == '__main__':
    main()
