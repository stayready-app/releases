#!/usr/bin/env python3
from __future__ import annotations
import json, signal, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = Path('/mnt/data/srdt-stage19-baseline/srdt-stage19')
PSK = '11' * 32
OUT = ROOT / 'docs' / 'stage20-verification'
OUT.mkdir(parents=True, exist_ok=True)

def case(name: str, server_tree: Path, client_tree: Path, server_extra: list[str], expect_success: bool) -> dict:
    metrics = OUT / f'compat-{name}-server.json'
    server_cmd = [str(server_tree/'bin/srdt-server-linux-amd64'), '--multi', '--listen', '127.0.0.1:0', '--psk', PSK, '--metrics-file', str(metrics), *server_extra]
    p = subprocess.Popen(server_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    lines=[]
    try:
        for _ in range(2):
            line=p.stdout.readline().strip(); lines.append(line)
        vals=dict(line.split('=',1) for line in lines if '=' in line)
        cmd=[str(client_tree/'bin/srdt-client-linux-amd64'), '--server', vals['LISTEN_ADDR'], '--server-public', vals['SERVER_PUBLIC'], '--psk', PSK, '--batch','12','--bytes','4096','--timeout', '10s' if expect_success else '1200ms']
        c=subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
        p.send_signal(signal.SIGINT)
        tail,_=p.communicate(timeout=5)
        stats=json.loads(metrics.read_text()) if metrics.exists() else {}
        ok=((c.returncode==0)==expect_success and p.returncode==0 and stats.get('buffer_bytes',0)==0 and (stats.get('delivered_messages',0)==12 if expect_success else True))
        log='\n'.join(lines)+'\nCLIENT:\n'+c.stdout+'\nSERVER:\n'+tail
        (OUT/f'compat-{name}.log').write_text(log)
        return {'name':name,'passed':ok,'expected_success':expect_success,'client_exit':c.returncode,'delivered_messages':stats.get('delivered_messages'),'resource_drops':stats.get('resource_drops'),'server_exit':p.returncode}
    finally:
        if p.poll() is None:
            p.kill(); p.communicate()

results=[
    case('stage20-client-stage19-server', BASE, ROOT, [], True),
    case('stage19-client-stage20-server-explicit', ROOT, BASE, ['--legacy-packet-acks'], True),
    case('stage19-client-stage20-server-no-flag', ROOT, BASE, [], False),
]
path=OUT/'compat-summary.json'; path.write_text(json.dumps(results, indent=2)+'\n')
print(json.dumps(results, indent=2))
if not all(x['passed'] for x in results): sys.exit(1)
