#!/usr/bin/env python3
"""Verify local Pod replacement, persistent storage, and majority-loss refusal.

Requires an already running three-node deployment. Never deletes PVCs. Restores
three replicas even if the majority-loss exercise fails.
"""
import json
import fcntl
from pathlib import Path
import subprocess
import time
import tempfile

ROOT = Path(__file__).resolve().parents[1]
KUBE = ['kubectl', '--context', 'docker-desktop', '--request-timeout=15s',
        '-n', 'quantafs-local']
JAVA = ['java', '-Xms16m', '-Xmx64m', '-jar', '/opt/quantafs/quantafs.jar']
SEEDS = ','.join(f'quantafs-{i}.quantafs-peer:7000' for i in range(3))


def run(args, timeout=60, check=True):
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'{args}: {result.stderr or result.stdout}')
    return result


def kube(*args, **options):
    return run(KUBE + list(args), **options)


def obj(kind, name):
    return json.loads(kube('get', kind, name, '-o', 'json').stdout)


def status(pod):
    output = kube('exec', pod, '--', *JAVA, 'cluster-status', '--target',
                  '127.0.0.1:7000').stdout
    return dict(item.split('=', 1) for item in output.split() if '=' in item)


def client(*args, check=True):
    return kube('exec', 'deployment/quantafs-client', '--', *JAVA,
                'cluster-client', '--network-mode', 'kubernetes', '--seeds', SEEDS,
                '--session', '/var/lib/quantafs-client/recovery-demo.session',
                *args, timeout=45, check=check)


def ready():
    until(lambda: kube('get', 'pod/quantafs-0', 'pod/quantafs-1', 'pod/quantafs-2',
                       check=False).returncode == 0, 'three Pod objects created')
    kube('wait', '--for=condition=Ready', 'pod/quantafs-0', 'pod/quantafs-1',
         'pod/quantafs-2', '--timeout=180s', timeout=190)


def until(test, description, timeout=90):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if test():
            return
        time.sleep(2)
    raise RuntimeError(f'timed out: {description}')


def main():
    run(['sh', 'deploy/kubernetes/verify-local-context.sh'])
    if obj('statefulset', 'quantafs')['spec']['replicas'] != 3:
        raise RuntimeError('start exactly three replicas before running this demo')
    ready()
    client('put', 'kubernetes-key', 'local persistent value')
    assert client('get', 'kubernetes-key').stdout.strip() == 'local persistent value'
    states = [status(f'quantafs-{i}') for i in range(3)]
    leaders = [s['id'] for s in states if s['role'] == 'LEADER']
    if len(leaders) != 1:
        raise RuntimeError(f'expected one leader, got {states}')
    leader = leaders[0]
    old_uid = obj('pod', leader)['metadata']['uid']
    pvc = obj('pvc', 'data-' + leader)
    claim_uid, volume = pvc['metadata']['uid'], pvc['spec']['volumeName']
    print(f'Replacing leader {leader}; Pod UID={old_uid}; volume={volume}', flush=True)
    kube('delete', 'pod', leader, '--wait=true', '--timeout=60s', timeout=70)
    client('put', 'after-pod-replacement', 'still committed')
    ready()
    assert obj('pod', leader)['metadata']['uid'] != old_uid
    replacement_claim = obj('pvc', 'data-' + leader)
    assert replacement_claim['metadata']['uid'] == claim_uid
    assert replacement_claim['spec']['volumeName'] == volume
    assert client('get', 'after-pod-replacement').stdout.strip() == 'still committed'
    print('PASS: replacement Pod reused the same PVC and data remained readable', flush=True)
    try:
        kube('scale', 'statefulset/quantafs', '--replicas=1')
        until(lambda: len(json.loads(kube('get', 'pods', '-l',
              'app.kubernetes.io/component=server', '-o', 'json').stdout)['items']) == 1,
              'two server Pods stopped')
        result = client('get', 'kubernetes-key', check=False)
        if result.returncode == 0:
            raise RuntimeError('minority unexpectedly completed a read barrier')
        if not any(reason in result.stderr for reason in ('UNAVAILABLE', 'DEADLINE_EXCEEDED')):
            raise RuntimeError(f'expected consensus refusal, got {result.stderr}')
        assert status('quantafs-0')['halted'] == 'false'
        print('PASS: minority refused the read without halting', flush=True)
    finally:
        kube('scale', 'statefulset/quantafs', '--replicas=3')
        ready()
    assert client('get', 'kubernetes-key').stdout.strip() == 'local persistent value'
    def caught_up():
        states = [status(f'quantafs-{i}') for i in range(3)]
        return (all(s['halted'] == 'false' for s in states)
                and len({s['commit'] for s in states}) == 1
                and all(s['applied'] == s['commit'] for s in states))
    until(caught_up, 'all three nodes applied the committed log')
    print('PASS: majority restored, all nodes caught up, original data readable; cluster remains running', flush=True)


if __name__ == '__main__':
    with open(Path(tempfile.gettempdir()) / 'quantafs-kubernetes-exercise.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('Another QuantaFs Kubernetes demo/benchmark is running; wait for it to finish.')
        main()
