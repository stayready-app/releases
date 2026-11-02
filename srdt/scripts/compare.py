#!/usr/bin/env python3
"""Paired Linux loopback/WAN-delay comparison against an extracted Stage 19.
Usage: python3 scripts/compare.py --baseline /path/to/srdt-stage19 --runs 3
Both trees need built server/load binaries. Never run beside another load test.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import shutil
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--resume', action='store_true', help='retain completed recorded attempts, including failures')
    args = parser.parse_args()
    if not 1 <= args.runs <= 20:
        parser.error('--runs must be 1..20')
    baseline = args.baseline.resolve()
    if baseline == ROOT or not (baseline / 'scripts/integration.py').exists():
        parser.error('baseline must be a separate extracted Stage 19 source tree')
    out = ROOT/'docs'/'stage20-comparison'
    out.mkdir(exist_ok=True)
    versions = [('stage19', baseline, 'stage19'), ('stage20', ROOT, 'stage20')]
    results: dict[str, list[dict]] = {name: [] for name, _, _ in versions}
    for number in range(1, args.runs+1):
        for name, tree, tag in versions:
            log = out/f'{name}-bulk-{number}.log'
            saved = out/f'{name}-bulk-{number}.json'
            if args.resume and saved.exists():
                report = json.loads(saved.read_text())
            else:
                live = tree/'docs'/f'{tag}-process-tests-bulk.json'
                live.unlink(missing_ok=True)  # Never read stale metrics after a failed launch.
                with log.open('w') as handle:
                    result = subprocess.run(['python3', 'scripts/integration.py', '--only', 'bulk'], cwd=tree,
                                            stdout=handle, stderr=subprocess.STDOUT, timeout=180, check=False)
                if not live.exists():
                    raise RuntimeError(f'{name} run {number} produced no report; see {log}')
                report = json.loads(live.read_text())[0]
                if result.returncode != 0:
                    report['passed'] = False
                saved.write_text(json.dumps(report, indent=2)+'\n')
                for suffix in ('server.json', 'load.json', 'server.log', 'load.log'):
                    shutil.copyfile(tree/'docs'/f'{tag}-bulk-{suffix}',out/f'{name}-bulk-{number}-{suffix}')
            results[name].append(report)
            print(name, number, 'PASS' if report['passed'] else 'FAIL',
                  report['elapsed_seconds'], report['server_sampled'],flush=True)
    # Identical synthetic-delay test code in both trees. Preserve an existing
    # baseline file if supplied by the caller; don't overwrite arbitrary code.
    test = 'netreliable/wan_comparison_test.go'
    target = baseline/test
    source = (ROOT/test).read_bytes()
    if target.exists() and target.read_bytes() != source:
        raise RuntimeError('baseline WAN comparison file differs; refusing overwrite')
    target.write_bytes(source)
    for name, tree, _ in versions:
        with (out/f'{name}-wan.txt').open('w') as handle:
            subprocess.run(['go','test','./netreliable','-run','^TestStage16WANComparison$','-count','3','-v'],
                           cwd=tree,stdout=handle,stderr=subprocess.STDOUT,timeout=120,check=True)
    summary = {}
    for name, values in results.items():
        completed = [v for v in values if v['passed']]
        summary[name] = {
            'runs': len(values),
            'completed_runs': len(completed),
            'failed_runs': len(values) - len(completed),
            'elapsed_seconds_all_attempts': [v['elapsed_seconds'] for v in values],
            'server_cpu_seconds_all_attempts': [v['server_sampled']['cpu_seconds'] for v in values],
            'sampled_server_rss_kib_all_attempts': [v['server_sampled']['rss_kib'] for v in values],
            'elapsed_seconds_completed_median': statistics.median(v['elapsed_seconds'] for v in completed) if completed else None,
            'server_cpu_seconds_completed_median': statistics.median(v['server_sampled']['cpu_seconds'] for v in completed) if completed else None,
            'sampled_server_rss_kib_completed_median': statistics.median(v['server_sampled']['rss_kib'] for v in completed) if completed else None,
            'resource_drops': [v['server']['resource_drops'] for v in values],
            'data_verified_all_runs': all(v['passed'] for v in values),
        }
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)
    # A failed historical baseline is evidence, not a reason to discard the
    # remaining paired runs. Failure of the candidate remains a hard failure.
    if not summary['stage20']['data_verified_all_runs']:
        raise SystemExit(1)

if __name__ == '__main__':
    main()
