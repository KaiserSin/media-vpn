#!/usr/bin/env python3
"""Check an existing private Xray profile without changing Happ or system routes."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True, type=Path)
    parser.add_argument('--xray', required=True, type=Path)
    parser.add_argument('--outbound', help='Force one named outbound; default keeps profile routing')
    parser.add_argument('--timeout', type=int, default=20)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error('--timeout must be positive')
    profile = json.loads(args.profile.read_text())
    if isinstance(profile, list):
        profile = profile[0]
    if args.outbound:
        if not any(o.get('tag') == args.outbound for o in profile.get('outbounds', [])):
            parser.error('The requested outbound tag does not exist')
        profile['routing'] = {'domainStrategy': 'AsIs', 'rules': [
            {'type': 'field', 'network': 'tcp,udp', 'outboundTag': args.outbound}]}
        profile.pop('observatory', None)
        profile.pop('burstObservatory', None)
    with socket.socket() as reserved:
        reserved.bind(('127.0.0.1', 0))
        port = reserved.getsockname()[1]
    profile['inbounds'] = [{'tag': 'diagnostic-socks', 'listen': '127.0.0.1',
                            'port': port, 'protocol': 'socks', 'settings': {'auth': 'noauth'}}]
    profile['log'] = {'loglevel': 'warning', 'access': 'none'}
    result = {'route': args.outbound or 'profile-default', 'passed': False}
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix='media-check-') as directory:
            config = Path(directory) / 'private.json'
            fd = os.open(config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as stream:
                json.dump(profile, stream)
            with (Path(directory) / 'private.log').open('w') as log:
                process = subprocess.Popen([str(args.xray.resolve()), 'run', '-c', str(config)],
                                           stdout=log, stderr=log)
                ready = False
                for _ in range(50):
                    if process.poll() is not None:
                        raise RuntimeError('Xray exited before its local listener became ready')
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=.1):
                            ready = True
                            break
                    except OSError:
                        time.sleep(.1)
                if not ready:
                    raise RuntimeError('Local diagnostic listener did not become ready')
                common = ['curl', '--fail', '--silent', '--show-error', '--noproxy', '',
                          '--connect-timeout', str(min(args.timeout, 10)),
                          '--max-time', str(args.timeout), '--proxy', f'socks5h://127.0.0.1:{port}']
                trace = subprocess.run(common + ['https://www.cloudflare.com/cdn-cgi/trace'],
                                       capture_output=True, text=True, timeout=args.timeout + 3)
                if trace.returncode:
                    result['curl_status'] = trace.returncode
                    raise RuntimeError('HTTPS trace failed through this route')
                fields = dict(line.split('=', 1) for line in trace.stdout.splitlines() if '=' in line)
                result['exit_country'] = fields.get('loc')
                result['tls'] = fields.get('tls')
                download = subprocess.run(common + ['https://speed.cloudflare.com/__down?bytes=1048576',
                                         '-o', os.devnull, '-w', '%{http_code} %{size_download} %{time_total}'],
                                          capture_output=True, text=True, timeout=args.timeout + 3)
                if download.returncode:
                    result['curl_status'] = download.returncode
                    raise RuntimeError('HTTPS download failed through this route')
                status, size, elapsed = download.stdout.split()
                result.update(http_status=int(status), download_bytes=int(size), seconds=float(elapsed))
                result['passed'] = int(status) == 200 and int(size) == 1048576
                # Stop the child before its private files are removed, including on Windows.
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                process = None
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
        result['error'] = str(error) if isinstance(error, RuntimeError) else type(error).__name__
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
