# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import hashlib
import os
import shutil
import subprocess
import urllib.request
import zipfile

def run(*args):
    subprocess.run(args, check=True)

base = Path('/opt/happ-vpn')
base.mkdir(mode=0o755, exist_ok=True)
backups = base / 'backups'
backups.mkdir(mode=0o700, exist_ok=True)
site = Path(os.environ['MEDIA_EXISTING_NGINX_SITE']).resolve(strict=True)
original = site.read_text()
backup = backups / 'digital-design.before-vpn'
if not backup.exists():
    shutil.copy2(site, backup)
challenge = Path('/var/www/happ-acme/.well-known/acme-challenge')
challenge.mkdir(parents=True, exist_ok=True)
if '/var/www/happ-acme' not in original:
    anchor = '    location / {'
    assert original.count(anchor) == 1
    updated = original.replace(anchor, '''    location ^~ /.well-known/acme-challenge/ {
        root /var/www/happ-acme;
        default_type text/plain;
        try_files $uri =404;
    }

''' + anchor)
    site.write_text(updated)
    try:
        run('nginx', '-t')
        run('systemctl', 'reload', 'nginx')
    except Exception:
        site.write_text(original)
        raise
(challenge / 'happ-setup-check').write_text('happ-origin-acme-ready\n')

venv = base / 'certbot'
if not (venv / 'bin/python').exists():
    run('python3', '-m', 'venv', str(venv))
run(str(venv / 'bin/pip'), 'install', '--disable-pip-version-check', '--quiet', 'certbot==5.8.0')
run(str(venv / 'bin/certbot'), '--version')

version = 'v26.7.28'
binary_dir = base / version
binary_dir.mkdir(exist_ok=True)
archive = base / 'Xray-linux-64.zip'
if not archive.exists():
    urllib.request.urlretrieve(f'https://github.com/XTLS/Xray-core/releases/download/{version}/Xray-linux-64.zip', archive)
assert hashlib.sha256(archive.read_bytes()).hexdigest() == '8195d909f1109b8f3d99eefe401a3c451d7bf4af71f24d3815420f77e5dd2a40', 'Xray checksum mismatch'
with zipfile.ZipFile(archive) as z:
    for name in ['xray', 'LICENSE']:
        if name in z.namelist():
            (binary_dir / name).write_bytes(z.read(name))
(binary_dir / 'xray').chmod(0o755)
run(str(binary_dir / 'xray'), 'version')
print('Origin prerequisites ready; existing HTTP site preserved.')
