# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import json,subprocess,os
os.umask(0o077)
statep=Path('/etc/happ-vpn/setup-state.json')
confp=Path('/etc/happ-vpn/server.json')
state=json.loads(statep.read_text());conf=json.loads(confp.read_text())
backup=Path('/opt/happ-vpn/backups/media-before-query')
backup.mkdir(exist_ok=True)
for name,p in [('state.json',statep),('server.json',confp)]:
 q=backup/name
 if not q.exists():q.write_bytes(p.read_bytes())
old=confp.read_text()
state.setdefault('original_transport_path',state['path'])
state['path']=state['original_transport_path'].rstrip('/')+'/stream.txt'
x=conf['inbounds'][0]['streamSettings']['xhttpSettings']
x['path']=state['path']
x.setdefault('extra',{}).update({'sessionIDPlacement':'query','sessionIDKey':'x_session','seqPlacement':'query','seqKey':'x_seq'})
confp.write_text(json.dumps(conf,indent=2)+'\n')
try:
 subprocess.run(['/opt/happ-vpn/v26.7.28/xray','run','-test','-config',str(confp)],check=True)
 subprocess.run(['systemctl','restart','happ-xray'],check=True)
 subprocess.run(['systemctl','is-active','--quiet','happ-xray'],check=True)
except Exception:
 confp.write_text(old);subprocess.run(['systemctl','restart','happ-xray']);raise
statep.write_text(json.dumps(state,indent=2)+'\n')
print('Media transport path and query placement updated; validated service active.')
