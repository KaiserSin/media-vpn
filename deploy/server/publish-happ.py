# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import base64
import copy
import grp
import json
import os
import re
import subprocess

os.umask(0o077)
ORIGIN_IP = os.environ['MEDIA_ORIGIN_IP']
cdn_host = os.environ['MEDIA_CDN_HOST']
state = json.loads(Path('/etc/happ-vpn/setup-state.json').read_text())
profile = {
    'remarks': 'Media',
    'log': {'loglevel': 'warning'},
    'dns': {'hosts': {'cloudflare-dns.com': '1.1.1.1'},
            'servers': ['https://cloudflare-dns.com/dns-query'], 'queryStrategy': 'UseIPv4'},
    'inbounds': [
        {'tag': 'socks', 'listen': '127.0.0.1', 'port': 10808, 'protocol': 'socks', 'settings': {'auth': 'noauth', 'udp': True}},
        {'tag': 'http', 'listen': '127.0.0.1', 'port': 10809, 'protocol': 'http', 'settings': {}}],
    'outbounds': [{
        'tag': 'proxy', 'protocol': 'vless',
        'settings': {'vnext': [{'address': cdn_host, 'port': 443,
                                'users': [{'id': state['uuid'], 'encryption': state['encryption']}]}]},
        'streamSettings': {
            'network': 'xhttp', 'security': 'tls',
            'tlsSettings': {'serverName': cdn_host, 'fingerprint': 'chrome', 'alpn': ['h2', 'http/1.1']},
            'xhttpSettings': {'host': cdn_host, 'path': state['path'], 'mode': 'packet-up',
                              'extra': {'uplinkHTTPMethod': 'GET', 'uplinkDataPlacement': 'header', 'uplinkDataKey': 'X-Data',
                                        'sessionIDPlacement': 'query', 'sessionIDKey': 'x_session', 'seqPlacement': 'query', 'seqKey': 'x_seq',
                                        'uplinkChunkSize': 2048, 'scMaxEachPostBytes': 2048, 'scMinPostsIntervalMs': 5,
                                        'xPaddingBytes': '100-200', 'xPaddingObfsMode': True, 'xPaddingPlacement': 'header', 'xPaddingHeader': 'X-Request-ID', 'xPaddingMethod': 'tokenish'}}},
        'mux': {'enabled': False}}]
}
if state.get('profile_mode') == 'automatic':
    cdn = profile['outbounds'][0]
    cdn['tag'] = 'cdn'
    primary = copy.deepcopy(cdn)
    primary['tag'] = 'primary'
    primary['settings']['vnext'][0]['address'] = ORIGIN_IP
    primary['streamSettings'] = {
        'network': 'ws', 'security': 'tls',
        'tlsSettings': {'serverName': ORIGIN_IP, 'fingerprint': 'chrome', 'alpn': ['http/1.1']},
        'wsSettings': {'path': state['primary_path']}}
    profile['outbounds'] = [primary, cdn]
    profile['routing'] = {
        'domainStrategy': 'AsIs',
        'balancers': [{'tag': 'media-auto', 'selector': ['primary'],
                       'strategy': {'type': 'roundRobin'}, 'fallbackTag': 'cdn'}],
        'rules': [{'type': 'field', 'network': 'tcp,udp', 'balancerTag': 'media-auto'}]}
    profile['observatory'] = {
        'subjectSelector': ['primary'], 'probeURL': 'https://www.gstatic.com/generate_204',
        'probeInterval': '10s', 'enableConcurrency': True}
    profile['log']['access'] = 'none'
private_profile = Path('/etc/happ-vpn/client-cdn.json')
private_profile.write_text(json.dumps(profile, indent=2) + '\n')
private_profile.chmod(0o600)
subprocess.run(['/opt/happ-vpn/v26.7.28/xray', 'run', '-test', '-config', str(private_profile)], check=True)
wwwgid = grp.getgrnam('www-data').gr_gid
subdir = Path('/var/lib/happ-subscription')
subdir.mkdir(mode=0o750, exist_ok=True)
subdir.chmod(0o750)
os.chown(subdir, 0, wwwgid)
subscription = subdir / 'happ.json'
subscription.write_text(json.dumps([profile], ensure_ascii=False, indent=2) + '\n')
subscription.chmod(0o640)
os.chown(subscription, 0, wwwgid)

url_path = '/s/' + state['subscription_token'] + '/profile.json'
legacy_path = '/s/' + state['subscription_token'] + '/happ.json'
location_template = '''    location = __SUB_PATH__ {
        alias /var/lib/happ-subscription/happ.json;
        default_type application/json;
        add_header Cache-Control "no-store, private" always;
        add_header X-Robots-Tag "noindex, nofollow" always;
        add_header profile-title "Media" always;
        add_header profile-update-interval "24" always;
    }
'''
location = ('    # BEGIN MEDIA SUBSCRIPTION\n' +
            location_template.replace('__SUB_PATH__', url_path) +
            location_template.replace('__SUB_PATH__', legacy_path) +
            '    # END MEDIA SUBSCRIPTION\n\n')
site = Path('/etc/nginx/sites-available/happ-origin')
old = site.read_text()
if '# BEGIN HAPP SUBSCRIPTION' in old or '# BEGIN MEDIA SUBSCRIPTION' in old:
    new = re.sub(r'    # BEGIN (?:HAPP|MEDIA) SUBSCRIPTION.*?    # END (?:HAPP|MEDIA) SUBSCRIPTION\n\n', lambda _: location, old, flags=re.S)
else:
    new = old.replace('    location / { return 404; }', location + '    location / { return 404; }')
new = new.replace('happ-origin-ready', 'ok')
if new != old:
    site.write_text(new)
    try:
        subprocess.run(['nginx', '-t'], check=True)
        subprocess.run(['systemctl', 'reload', 'nginx'], check=True)
    except Exception:
        site.write_text(old)
        raise

state['cdn_host'] = cdn_host
state['subscription_url'] = 'https://' + ORIGIN_IP + url_path
state['legacy_subscription_url'] = 'https://' + ORIGIN_IP + legacy_path
state['display_name'] = 'Media'
Path('/etc/happ-vpn/setup-state.json').write_text(json.dumps(state, indent=2) + '\n')
print('CDN client profile staged; subscription endpoint ready. Credentials not printed.')
