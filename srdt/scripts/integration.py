#!/usr/bin/env python3
"""Process-level Linux/loopback tests. Samples SERVER resources, not the loader.
Build bin/srdt-{server,load}-linux-amd64 first. No Python dependencies.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
TAG = 'stage20'
PSK = '11' * 32  # Public laboratory fixture, never a production credential.

def run_case(name: str, clients: int, messages: int, size: int,
             *, idle: bool = False, filtered: bool = False, timeout: int = 120, buffer_budget: int | None = None, max_message: int | None = None) -> dict:
    metrics = ROOT / 'docs' / f'{TAG}-{name}-server.json'
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='128MiB')
    args = [str(ROOT/'bin/srdt-server-linux-amd64'), '--multi', '--listen', '127.0.0.1:0',
            '--psk', PSK, '--max-per-ip', '1024', '--hello-rate', '10000',
            '--hello-burst', '2048', '--metrics-file', str(metrics),
            '--idle-timeout', '180s', '--progress-timeout', '90s']
    if filtered: args += ['--drop-min', '800', '--drop-max', '1100']
    if buffer_budget is not None: args += ['--buffer-budget', str(buffer_budget)]
    if max_message is not None: args += ['--max-message', str(max_message)]
    peaks = {'rss_kib': 0, 'threads': 0, 'cpu_seconds': 0.0}
    stop = threading.Event()
    proc = subprocess.Popen(args, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    header = []
    try:
        public = address = None
        for _ in range(2):
            line = proc.stdout.readline().strip()
            header.append(line)
            if line.startswith('SERVER_PUBLIC='): public = line.split('=', 1)[1]
            if line.startswith('LISTEN_ADDR='): address = line.split('=', 1)[1]
        if not public or not address: raise RuntimeError(f'server startup failed: {header}')
        def sample() -> None:
            while not stop.wait(0.02):
                try:
                    for line in Path(f'/proc/{proc.pid}/status').read_text().splitlines():
                        if line.startswith('VmRSS:'):
                            peaks['rss_kib'] = max(peaks['rss_kib'], int(line.split()[1]))
                        if line.startswith('Threads:'):
                            peaks['threads'] = max(peaks['threads'], int(line.split()[1]))
                    fields = Path(f'/proc/{proc.pid}/stat').read_text().split(') ', 1)[1].split()
                    peaks['cpu_seconds'] = (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')
                except (FileNotFoundError, ProcessLookupError): break
        monitor = threading.Thread(target=sample, daemon=True)
        monitor.start()
        workload = [str(ROOT/'bin/srdt-load-linux-amd64'), '--server', address,
                    '--server-public', public, '--psk', PSK,
                    '--mode', 'idle' if idle else 'active', '--clients', str(clients),
                    '--messages', str(messages), '--bytes', str(size), '--timeout', f'{timeout}s']
        if idle: workload += ['--hold', '1s']
        if filtered: workload += ['--drop-min', '800', '--drop-max', '1100']
        started = time.monotonic()
        result = subprocess.run(workload, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout+10)
        elapsed = time.monotonic() - started
        (ROOT/'docs'/f'{TAG}-{name}-load.log').write_text(result.stdout)
        proc.send_signal(signal.SIGINT)
        tail, _ = proc.communicate(timeout=5)
        (ROOT/'docs'/f'{TAG}-{name}-server.log').write_text('\n'.join(header)+'\n'+tail)
        stop.set(); monitor.join()
        server = json.loads(metrics.read_text())
        load = json.loads(result.stdout[result.stdout.find('{'):])
        (ROOT/'docs'/f'{TAG}-{name}-load.json').write_text(json.dumps(load,indent=2)+'\n')
        expected = clients if idle else clients*messages
        passed = (result.returncode == 0 and proc.returncode == 0
                  and load['succeeded'] == clients and server['delivered_messages'] == expected
                  and server['packet_state_bytes'] == 0
                  and server['peak_packet_state_bytes'] <= (16<<20)
                  and server['peak_session_tracked_packets'] <= 128
                  and server['buffer_bytes'] == 0 and server['active_sessions'] == 0
                  and server['peak_buffer_bytes'] <= (buffer_budget if buffer_budget is not None else 64 << 20)
                  and server['resource_drops'] == 0 and server['credit_violations'] == 0
                  and server['legacy_sessions'] == 0 and server['waiting_sessions'] == 0
                  and (not filtered or server['filtered_datagrams'] > 0))
        report = dict(name=name, passed=passed, server_GOMAXPROCS=2,
                      server_GOMEMLIMIT='128MiB (soft Go-runtime limit, not RSS cap)',
                      elapsed_seconds=elapsed, server_sampled=peaks, server=server, load=load)
        print(json.dumps(report, ensure_ascii=False), flush=True)
        return report
    finally:
        stop.set()
        if proc.poll() is None: proc.kill(); proc.communicate()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--bulk', action='store_true', help='also verify 100 MiB aggregate each direction')
    parser.add_argument('--only', choices=['baseline','filter','idle','bulk','long-batch','pressure','active500','active1000'])
    args = parser.parse_args()
    cases = []
    if args.only in (None,'baseline'): cases.append(('baseline',32,12,4096,{}))
    if args.only in (None,'filter'): cases.append(('filter',32,12,4096,dict(filtered=True)))
    if args.only in (None,'idle'): cases.append(('idle',1000,1,1,dict(idle=True)))
    if args.bulk or args.only == 'bulk': cases.append(('bulk',200,32,16384,dict(timeout=120)))
    if args.bulk or args.only == 'long-batch': cases.append(('long-batch',1,6400,16384,dict(timeout=120)))
    if args.bulk or args.only == 'pressure': cases.append(('pressure',128,16,4096,dict(timeout=120,buffer_budget=8192,max_message=4096)))
    if args.only == 'active500': cases.append(('active500',500,8,16384,dict(timeout=120)))
    if args.only == 'active1000': cases.append(('active1000',1000,4,16384,dict(timeout=120)))
    reports = [run_case(name,n,m,size,**options) for name,n,m,size,options in cases]
    path = ROOT/'docs'/(TAG+'-process-tests'+('-'+args.only if args.only else '')+'.json')
    path.write_text(json.dumps(reports,indent=2)+'\n')
    if not all(r['passed'] for r in reports): raise SystemExit(1)

if __name__ == '__main__': main()
