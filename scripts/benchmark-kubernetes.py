#!/usr/bin/env python3
"""Run bounded local Kubernetes workloads; retain CSV samples and environment metadata."""
import csv
from datetime import datetime, timezone
import hashlib
import json
import fcntl
import math
from pathlib import Path
import platform
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
KUBE = ['kubectl', '--context', 'docker-desktop', '--request-timeout=15s', '-n', 'quantafs-local']


def run(args, timeout=60):
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{args}: {result.stderr or result.stdout}')
    return result


def main():
    run(['sh', 'deploy/kubernetes/verify-local-context.sh'])
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    output = ROOT / 'benchmarks' / stamp
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        'timestamp_utc': stamp, 'platform': platform.platform(),
        'git_commit': run(['git', 'rev-parse', 'HEAD']).stdout.strip(),
        'dirty_files': run(['git', 'status', '--short']).stdout.splitlines(),
        'jar_sha256': hashlib.sha256((ROOT / 'target/quantafs.jar').read_bytes()).hexdigest(),
        'source_sha256': hashlib.sha256((ROOT / 'scripts/QuantaBenchmark.java').read_bytes()).hexdigest(),
        'docker_resources': json.loads(run(['docker', 'info', '--format', '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}}}']).stdout),
        'nodes': json.loads(run(KUBE + ['get', 'nodes', '-o', 'json']).stdout),
        'pods': json.loads(run(KUBE + ['get', 'pods', '-o', 'json']).stdout),
        'workload': 'closed-loop 50% PUT / 50% barrier GET; persistent channels; unique client per worker; no CLI journal fsync',
        'value_bytes': 512, 'warmup_per_worker': 100, 'measured_operations_per_trial': 1000,
        'trials_per_concurrency': 3, 'concurrency': [1, 4],
    }
    (output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    summaries = []
    with tempfile.TemporaryDirectory(prefix='quantafs-bench-') as temp:
        run(['javac', '-cp', 'target/quantafs.jar', '-d', temp, 'scripts/QuantaBenchmark.java'])
        pod = json.loads(run(KUBE + ['get', 'pods', '-l', 'app.kubernetes.io/component=client', '-o', 'json']).stdout)['items'][0]['metadata']['name']
        remote = '/tmp/benchmark-' + stamp
        run(KUBE + ['exec', pod, '--', 'mkdir', '-p', remote])
        try:
            for file in Path(temp).glob('*.class'):
                run(KUBE + ['cp', str(file), pod + ':' + remote + '/' + file.name])
            for workers in [1, 4]:
                for trial in range(1, 4):
                    result = run(KUBE + ['exec', pod, '--', 'java', '-Xms16m', '-Xmx64m',
                        '-cp', remote + ':/opt/quantafs/quantafs.jar', 'QuantaBenchmark',
                        ','.join(f'quantafs-{i}.quantafs-peer:7000' for i in range(3)),
                        str(workers), str(1000 // workers), '100', '512'], timeout=320)
                    name = f'c{workers}-trial{trial}'
                    (output / (name + '.csv')).write_text(result.stdout)
                    (output / (name + '.stderr.txt')).write_text(result.stderr)
                    rows = list(csv.DictReader(result.stdout.splitlines()))
                    elapsed = int(next(line.split('=')[1] for line in result.stderr.splitlines() if line.startswith('elapsed_ns='))) / 1e9
                    summary = {'concurrency': workers, 'trial': trial, 'operations': len(rows),
                        'errors': sum(row['success'] != 'true' for row in rows),
                        'elapsed_seconds': elapsed, 'throughput_ops_s': len(rows) / elapsed}
                    for operation in ['all', 'put', 'get']:
                        samples = sorted(int(row['latency_ns']) / 1e6 for row in rows if operation == 'all' or row['operation'] == operation)
                        summary[operation] = {f'p{p}_ms': samples[math.ceil(p / 100 * len(samples)) - 1] for p in [50, 95, 99]}
                    summaries.append(summary)
                    (output / 'summary.json').write_text(json.dumps(summaries, indent=2) + '\n')
                    print(f'{name}: {summary["throughput_ops_s"]:.1f} ops/s; p95={summary["all"]["p95_ms"]:.2f} ms; errors={summary["errors"]}', flush=True)
        finally:
            # Only our timestamped compiled classes in /tmp; never touches database/session volumes.
            run(KUBE + ['exec', pod, '--', 'rm', '-rf', remote])
    print('Results: ' + str(output), flush=True)


if __name__ == '__main__':
    with open(Path(tempfile.gettempdir()) / 'quantafs-kubernetes-exercise.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('Another QuantaFs Kubernetes demo/benchmark is running; wait for it to finish.')
        main()
