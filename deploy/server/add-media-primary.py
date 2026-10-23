# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import copy,json,os,subprocess
os.umask(0o077)
statep=Path('/etc/happ-vpn/setup-state.json');confp=Path('/etc/happ-vpn/server.json');sitep=Path('/etc/nginx/sites-available/happ-origin')
s=json.loads(statep.read_text());c=json.loads(confp.read_text());oldconf=confp.read_text();oldsite=sitep.read_text()
backup=Path('/opt/happ-vpn/backups/before-primary');backup.mkdir(exist_ok=True)
for name,p in [('state.json',statep),('server.json',confp),('origin.conf',sitep)]:
 b=backup/name
 if not b.exists():b.write_bytes(p.read_bytes())
path=s.get('original_transport_path',s['path'].rsplit('/',1)[0]+'/').rstrip('/')+'/live'
source=next(i for i in c['inbounds'] if i['streamSettings']['network']=='xhttp')
primary={'tag':'media-primary','listen':'127.0.0.1','port':18083,'protocol':'vless','settings':copy.deepcopy(source['settings']),'streamSettings':{'network':'ws','security':'none','wsSettings':{'path':path}}}
c['inbounds']=[i for i in c['inbounds'] if i.get('tag')!='media-primary']+[primary]
confp.write_text(json.dumps(c,indent=2)+'\n')
block='''    # BEGIN MEDIA PRIMARY
    location = __PATH__ {
        proxy_pass http://127.0.0.1:18083;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Forwarded-For "";
        proxy_buffering off;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }
    # END MEDIA PRIMARY

'''.replace('__PATH__',path)
if '# BEGIN MEDIA PRIMARY' not in oldsite:sitep.write_text(oldsite.replace('    location / { return 404; }',block+'    location / { return 404; }'))
try:
 subprocess.run(['/opt/happ-vpn/v26.7.28/xray','run','-test','-config',str(confp)],check=True)
 subprocess.run(['nginx','-t'],check=True)
 subprocess.run(['systemctl','restart','happ-xray'],check=True)
 subprocess.run(['systemctl','reload','nginx'],check=True)
except Exception:
 confp.write_text(oldconf);sitep.write_text(oldsite);subprocess.run(['systemctl','restart','happ-xray']);subprocess.run(['systemctl','reload','nginx']);raise
s['primary_path']=path;s['primary_port']=443;s['primary_transport']='ws'
statep.write_text(json.dumps(s,indent=2)+'\n')
print('Primary endpoint added on existing HTTPS port; CDN endpoint preserved.')
