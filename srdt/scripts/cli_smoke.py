#!/usr/bin/env python3
"""CLI smoke/explicit Stage 18 compatibility tests. Linux, no dependencies."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import signal
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PSK = '11'*32  # Public test fixture.

def run_case(name: str, server_tree: Path, client_tree: Path, *, legacy: bool=False,
             filtered: bool=False, expect_success: bool=True, old_credits: bool=False, old_packets: bool=False) -> dict:
    stats_file = ROOT/'docs'/f'stage19-cli-{name}-server.json'
    args = [str(server_tree/'bin/srdt-server-linux-amd64'),'--multi','--listen','127.0.0.1:0',
            '--psk',PSK,'--metrics-file',str(stats_file)]
    if legacy: args += ['--allow-legacy-data']
    if filtered: args += ['--drop-min','800','--drop-max','1100']
    p = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    try:
        head = [p.stdout.readline().strip(),p.stdout.readline().strip()]
        values = dict(line.split('=',1) for line in head if '=' in line)
        command = [str(client_tree/'bin/srdt-client-linux-amd64'),'--server',values['LISTEN_ADDR'],
                   '--server-public',values['SERVER_PUBLIC'],'--psk',PSK,
                   '--batch','12','--bytes','4096','--timeout','10s' if expect_success else '500ms']
        if old_credits: command += ['--legacy-credits']
        if old_packets: command += ['--legacy-packets']
        if filtered: command += ['--drop-min','800','--drop-max','1100']
        c = subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=15)
        p.send_signal(signal.SIGINT)
        tail,_ = p.communicate(timeout=5)
        stats = json.loads(stats_file.read_text())
        passed = (p.returncode == 0 and (c.returncode == 0) == expect_success
                  and stats['delivered_messages'] == (12 if expect_success else 0)
                  and stats['buffer_bytes'] == 0)
        (ROOT/'docs'/f'stage19-cli-{name}.log').write_text('\n'.join(head)+'\n'+c.stdout+tail)
        result = dict(name=name,passed=passed,expected_success=expect_success,
                      client_exit=c.returncode,server=stats)
        print(json.dumps(result),flush=True)
        return result
    finally:
        if p.poll() is None: p.kill();p.communicate()

def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument('--baseline',type=Path)
    a = ap.parse_args()
    results = [run_case('modern',ROOT,ROOT),run_case('filtered',ROOT,ROOT,filtered=True)]
    if a.baseline:
        base = a.baseline.resolve()
        results += [run_case('stage18-client-new-server',ROOT,base),
                    run_case('stage19-client-stage18-server',base,ROOT,old_packets=True),
                    run_case('no-silent-packet-downgrade',base,ROOT,expect_success=False),
                    run_case('legacy-credits-explicit',ROOT,ROOT,old_credits=True)]
    (ROOT/'docs/stage19-cli-smoke.json').write_text(json.dumps(results,indent=2)+'\n')
    if not all(r['passed'] for r in results): raise SystemExit(1)

if __name__ == '__main__': main()
