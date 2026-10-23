# Archived deployment script. Read deploy/README.md before explicit execution.
from pathlib import Path
import copy,json,os,subprocess,urllib.request
os.umask(0o077)
WORK = Path(os.environ['MEDIA_WORK_DIR'])
SSH_HOST = os.environ['MEDIA_SSH_HOST']
ORIGIN_IP = os.environ['MEDIA_ORIGIN_IP']
XRAY = os.environ['MEDIA_XRAY']
state=json.loads(subprocess.check_output(['ssh','-o','BatchMode=yes','-o','UpdateHostKeys=no',SSH_HOST,'cat /etc/happ-vpn/setup-state.json'],text=True))
with urllib.request.urlopen(state['subscription_url'],timeout=20) as r:profile=json.load(r)[0]
cdn=copy.deepcopy(next(o for o in profile['outbounds'] if o.get('streamSettings',{}).get('network')=='xhttp'))
cdn['tag']='cdn'
primary=copy.deepcopy(cdn);primary['tag']='primary';primary['settings']['vnext'][0]['address']=ORIGIN_IP
primary['streamSettings']={'network':'ws','security':'tls','tlsSettings':{'serverName':ORIGIN_IP,'fingerprint':'chrome','alpn':['http/1.1']},'wsSettings':{'path':state['primary_path']}}
profile['outbounds']=[primary,cdn]
profile['routing']={'domainStrategy':'AsIs','balancers':[{'tag':'media-auto','selector':['primary'],'strategy':{'type':'roundRobin'},'fallbackTag':'cdn'}],'rules':[{'type':'field','network':'tcp,udp','balancerTag':'media-auto'}]}
profile['observatory']={'subjectSelector':['primary'],'probeURL':'https://www.gstatic.com/generate_204','probeInterval':'10s','enableConcurrency':True}
profile['log']={'loglevel':'warning','access':'none'}
p=(WORK / 'xray-client/media-auto-candidate.json');p.write_text(json.dumps(profile,indent=2)+'\n')
subprocess.run([XRAY,'run','-test','-c',str(p)],check=True)
print('Single Media candidate staged; primary-first automatic fallback, not yet published.')
