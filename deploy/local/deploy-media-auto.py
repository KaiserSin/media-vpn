# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import json,subprocess,os
WORK = Path(os.environ['MEDIA_WORK_DIR'])
SSH_HOST = os.environ['MEDIA_SSH_HOST']
check=json.loads((WORK / 'media-failover-result.json').read_text())
assert check.get('passed') is True, 'Automatic failover must pass before publication'
profile=json.loads((WORK / 'xray-client/media-auto-candidate.json').read_text())
assert profile['remarks']=='Media'
assert [o['tag'] for o in profile['outbounds']]==['primary','cdn']
assert profile['routing']['balancers'][0]['selector']==['primary']
assert profile['routing']['balancers'][0]['strategy']['type']=='roundRobin'
remote='''from pathlib import Path
import grp,json,os,subprocess
os.umask(0o077)
profile=json.loads(__PAYLOAD__)
candidate=Path('/etc/happ-vpn/client-auto-candidate.json');candidate.write_text(json.dumps(profile,indent=2)+'\\n')
subprocess.run(['/opt/happ-vpn/v26.7.28/xray','run','-test','-c',str(candidate)],check=True)
statep=Path('/etc/happ-vpn/setup-state.json');state=json.loads(statep.read_text())
private=Path('/etc/happ-vpn/client-cdn.json');sub=Path('/var/lib/happ-subscription/happ.json')
backup=Path('/opt/happ-vpn/backups/before-auto-profile');backup.mkdir(exist_ok=True)
for name,p in [('client.json',private),('subscription.json',sub),('state.json',statep)]:
 b=backup/name
 if not b.exists():b.write_bytes(p.read_bytes())
temp=private.with_suffix('.tmp');temp.write_text(json.dumps(profile,indent=2)+'\\n');temp.chmod(0o600);temp.replace(private)
temp=sub.with_suffix('.tmp');temp.write_text(json.dumps([profile],indent=2)+'\\n');temp.chmod(0o640);os.chown(temp,0,grp.getgrnam('www-data').gr_gid);temp.replace(sub)
state['profile_mode']='automatic';state['primary_tag']='primary';state['fallback_tag']='cdn';state['probe_interval_seconds']=10
statep.write_text(json.dumps(state,indent=2)+'\\n')
print('Published one Media profile with automatic primary/CDN switching; URL unchanged.')
'''.replace('__PAYLOAD__',repr(json.dumps(profile)))
subprocess.run(['ssh','-o','BatchMode=yes','-o','UpdateHostKeys=no',SSH_HOST,'python3 -'],input=remote,text=True,check=True)
