"""
Integration test for request_id full-chain tracing in
vibora/protocol/cprotocol.pyx.

Verifies:
  1. Missing X-Request-ID -> server generates one, injects it into the
     response header and the request object (echoed by /echo).
  2. Inbound X-Request-ID is transparently propagated, not regenerated.
  3. Generated ids are unique per request and UUID shaped.
  4. Logs emitted while handling the request carry the request id.
Run: python tests/manual/test_request_id.py
"""
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

UUID_RE = re.compile(r'^[0-9a-f]{32}$')


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


def fetch(port, path, headers=None):
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', headers=headers or {})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, dict(resp.headers), resp.read()


def main():
    port = int(os.environ.get('VIBORA_TEST_PORT', '8098'))
    proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, 'server_app.py'), str(port), '1'],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    try:
        assert wait_port(port), 'server did not start in time'

        # 1) Auto generated id when missing.
        status, headers, body = fetch(port, '/echo')
        assert status == 200
        generated = headers.get('X-Request-ID')
        assert generated, 'X-Request-ID response header must be auto generated'
        assert UUID_RE.match(generated), f'expected hex uuid, got {generated}'
        payload = json.loads(body.decode())
        assert payload['request_id'] == generated, payload
        print('PASS auto-generated request id:', generated)

        # 2) Uniqueness across requests.
        ids = set()
        for _ in range(5):
            _, headers, _ = fetch(port, '/echo')
            ids.add(headers['X-Request-ID'])
        assert len(ids) == 5, f'generated ids must differ, got {ids}'
        print('PASS ids are unique per request')

        # 3) Client supplied id is propagated end-to-end.
        status, headers, body = fetch(port, '/echo', {'X-Request-ID': 'trace-abc-123'})
        assert headers.get('X-Request-ID') == 'trace-abc-123', headers
        assert json.loads(body.decode())['request_id'] == 'trace-abc-123'
        print('PASS inbound X-Request-ID is propagated')

        # 4) Ordinary routes also get the header.
        status, headers, _ = fetch(port, '/')
        assert headers.get('X-Request-ID'), headers
        print('PASS header present on every response:', headers['X-Request-ID'])
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            out, err = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            raise

    logs = err.decode()
    assert 'request started id=trace-abc-123' in logs, 'request id missing from worker logs:\n' + logs[-2000:]
    print('PASS request id is injected into worker logs')
    print('ALL REQUEST ID TESTS PASSED')


if __name__ == '__main__':
    main()
