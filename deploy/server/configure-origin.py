# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import grp
import json
import os
import re
import secrets
import subprocess
import uuid

def run(*args):
    subprocess.run(args, check=True)

def write(path, content, mode=0o644):
    p = Path(path)
    p.write_text(content)
    p.chmod(mode)

ORIGIN_IP = os.environ['MEDIA_ORIGIN_IP']
binary = '/opt/happ-vpn/v26.7.28/xray'
confdir = Path('/etc/happ-vpn')
confdir.mkdir(mode=0o750, exist_ok=True)
if subprocess.run(['id', '-u', 'happ-vpn'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
    run('useradd', '--system', '--user-group', '--no-create-home', '--home-dir', '/nonexistent', '--shell', '/usr/sbin/nologin', 'happ-vpn')
gid = grp.getgrnam('happ-vpn').gr_gid
os.chown(confdir, 0, gid)
statepath = confdir / 'setup-state.json'
if not statepath.exists():
    pairs = subprocess.check_output([binary, 'vlessenc'], text=True)
    server_key = re.findall(r'"decryption": "([^"]+)"', pairs)[0]
    client_key = re.findall(r'"encryption": "([^"]+)"', pairs)[0].replace('.0rtt.', '.1rtt.')
    state = {'uuid': str(uuid.uuid4()), 'path': '/' + secrets.token_hex(16) + '/', 'encryption': client_key,
             'subscription_token': secrets.token_urlsafe(32), 'version': '26.7.28'}
else:
    state = json.loads(statepath.read_text())
    server_key = json.loads((confdir / 'server.json').read_text())['inbounds'][0]['settings']['decryption']

common_extra = {'uplinkHTTPMethod': 'GET', 'uplinkDataPlacement': 'header', 'uplinkDataKey': 'X-Data',
                'uplinkChunkSize': 2048, 'scMaxEachPostBytes': 2048, 'xPaddingBytes': '100-200'}
config = {
    'log': {'loglevel': 'warning'},
    'inbounds': [{'tag': 'happ-xhttp', 'listen': '127.0.0.1', 'port': 18082, 'protocol': 'vless',
                  'settings': {'clients': [{'id': state['uuid']}], 'decryption': server_key},
                  'streamSettings': {'network': 'xhttp', 'security': 'none',
                                     'xhttpSettings': {'path': state['path'], 'mode': 'packet-up',
                                                       'extra': dict(common_extra, scMaxBufferedPosts=64, serverMaxHeaderBytes=16384)}}}],
    'outbounds': [{'tag': 'direct', 'protocol': 'freedom', 'settings': {'domainStrategy': 'UseIP'}},
                  {'tag': 'blocked', 'protocol': 'blackhole'}],
    'routing': {'domainStrategy': 'IPIfNonMatch', 'rules': [
        {'type': 'field', 'ip': ['0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16',
                               '172.16.0.0/12', '192.168.0.0/16', '224.0.0.0/4', '240.0.0.0/4', '::/128',
                               '::1/128', 'fc00::/7', 'fe80::/10', 'ff00::/8'], 'outboundTag': 'blocked'},
        {'type': 'field', 'port': '25', 'network': 'tcp', 'outboundTag': 'blocked'}]}}
write(confdir / 'server.json', json.dumps(config, indent=2) + '\n', 0o640)
os.chown(confdir / 'server.json', 0, gid)
run(binary, 'run', '-test', '-config', str(confdir / 'server.json'))
write(statepath, json.dumps(state, indent=2) + '\n', 0o600)

write('/etc/systemd/system/happ-xray.service', '''[Unit]
Description=Happ encrypted XHTTP VPN origin
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=happ-vpn
Group=happ-vpn
ExecStart=/opt/happ-vpn/v26.7.28/xray run -config /etc/happ-vpn/server.json
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
CapabilityBoundingSet=
UMask=0077

[Install]
WantedBy=multi-user.target
''')

nginx = '''server {
    listen 443 ssl http2;
    server_name __MEDIA_ORIGIN_IP__;
    ssl_certificate /etc/happ-certbot/live/happ-origin/fullchain.pem;
    ssl_certificate_key /etc/happ-certbot/live/happ-origin/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_session_cache shared:HappTLS:1m;
    ssl_session_timeout 1d;
    ssl_session_tickets off;
    server_tokens off;
    access_log off;
    large_client_header_buffers 4 16k;

    location = /healthz {
        default_type text/plain;
        add_header Cache-Control "no-store" always;
        return 200 "happ-origin-ready\\n";
    }

    location ^~ __XHTTP_PATH__ {
        proxy_pass http://127.0.0.1:18082;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_cache off;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
        add_header X-Accel-Buffering "no" always;
        add_header Cache-Control "no-store" always;
    }

    location / { return 404; }
}
'''.replace('__XHTTP_PATH__', state['path']).replace('__MEDIA_ORIGIN_IP__', ORIGIN_IP)
site = Path('/etc/nginx/sites-available/happ-origin')
previous_site = site.read_text() if site.exists() else None
write(site, nginx, 0o600)
enabled = Path('/etc/nginx/sites-enabled/happ-origin')
already_enabled = enabled.exists()
if not already_enabled:
    enabled.symlink_to(site)
try:
    run('nginx', '-t')
except Exception:
    if not already_enabled:
        enabled.unlink()
    if previous_site is not None:
        write(site, previous_site, 0o600)
    raise

write('/etc/systemd/system/happ-cert-renew.service', '''[Unit]
Description=Renew short-lived HTTPS certificate for Happ origin
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/opt/happ-vpn/certbot/bin/certbot renew --quiet --cert-name happ-origin --config-dir /etc/happ-certbot --work-dir /var/lib/happ-certbot --logs-dir /var/log/happ-certbot --deploy-hook "/usr/sbin/nginx -t && /usr/bin/systemctl reload nginx"
''')
write('/etc/systemd/system/happ-cert-renew.timer', '''[Unit]
Description=Check Happ origin certificate every six hours

[Timer]
OnCalendar=*-*-* 00,06,12,18:00:00
RandomizedDelaySec=30m
Persistent=true

[Install]
WantedBy=timers.target
''')
run('systemctl', 'daemon-reload')
run('systemctl', 'enable', '--now', 'happ-xray.service', 'happ-cert-renew.timer')
run('systemctl', 'restart', 'happ-xray.service')
run('systemctl', 'reload', 'nginx')
run('systemctl', 'start', 'happ-cert-renew.service')
run('systemctl', 'is-active', 'happ-xray', 'nginx', 'happ-cert-renew.timer')
print('Encrypted XHTTP origin and automatic certificate renewal configured. Secrets retained on VDS.')
