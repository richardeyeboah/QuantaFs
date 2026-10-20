#!/usr/bin/env python3
"""Real local Java processes, gRPC, and independent disks. Only owns its temporary directory."""
import argparse
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--nodes', type=int, choices=(3, 5), default=3)
parser.add_argument('--keep', action='store_true', help='keep this run\'s temporary node databases and logs')
args = parser.parse_args()
project = pathlib.Path(__file__).resolve().parents[1]
jar = project / 'target/quantafs.jar'
java = shutil.which('java')
if not java or not jar.is_file():
    raise SystemExit('Install JDK 21 and run ./mvnw verify first.')
work = pathlib.Path(tempfile.mkdtemp(prefix='quantafs-real-cluster-'))
reservations = [socket.socket() for _ in range(args.nodes)]
for sock in reservations:
    sock.bind(('127.0.0.1', 0))
ports = [sock.getsockname()[1] for sock in reservations]
for sock in reservations:
    sock.close()
peers = ','.join(f'n{i+1}=127.0.0.1:{p}' for i, p in enumerate(ports))
processes = {}
logs = []
succeeded = False

def start(i):
    log = open(work / f'n{i+1}.log', 'a')
    logs.append(log)
    processes[i] = subprocess.Popen([java, '-jar', str(jar), 'cluster-server', '--id', f'n{i+1}',
        '--peers', peers, '--data-dir', str(work / f'n{i+1}')], stdout=log, stderr=subprocess.STDOUT)

def status(i):
    result = subprocess.run([java, '-jar', str(jar), 'cluster-status', '--target', f'127.0.0.1:{ports[i]}'],
        capture_output=True, text=True, timeout=5)
    if result.returncode:
        return None
    return dict(field.split('=', 1) for field in result.stdout.strip().split())

def leader(exclude=-1):
    end = time.monotonic() + 40
    while time.monotonic() < end:
        for i, process in processes.items():
            if i == exclude or process.poll() is not None:
                continue
            state = status(i)
            if state and state['role'] == 'LEADER' and state['halted'] == 'false' and int(state['commit']) > 0:
                return i
        time.sleep(.1)
    raise RuntimeError('No leader established. Inspect the node logs.')

def client(i, *command):
    result = subprocess.run([java, '-jar', str(jar), 'client', '--target', f'127.0.0.1:{ports[i]}',
        '--timeout-seconds', '8', *command], capture_output=True, text=True, timeout=12)
    print(f'  n{i+1}: {" ".join(command)} -> {(result.stdout + result.stderr).strip()}', flush=True)
    if result.returncode:
        raise RuntimeError('Client request failed; no automatic mutation retries are performed.')
    return result.stdout.strip()

try:
    print(f'Starting {args.nodes} independent Java processes. Storage/logs: {work}', flush=True)
    for i in range(args.nodes):
        start(i)
    first = leader()
    print(f'1. n{first+1} elected leader.', flush=True)
    client(first, 'put', 'order:1042', 'paid')
    assert client(first, 'get', 'order:1042') == 'paid'
    print(f'2. Forcibly killing leader n{first+1}; no shutdown cleanup runs.', flush=True)
    processes[first].kill()
    processes[first].wait(timeout=10)
    replacement = leader(first)
    print(f'3. n{replacement+1} is the replacement leader.', flush=True)
    assert client(replacement, 'get', 'order:1042') == 'paid'
    client(replacement, 'put', 'city', 'Accra')
    print(f'4. Restarting n{first+1} using its original disk directory.', flush=True)
    start(first)
    target = int(status(replacement)['applied'])
    end = time.monotonic() + 30
    while time.monotonic() < end:
        state = status(first)
        if state and int(state['applied']) >= target:
            break
        time.sleep(.1)
    else:
        raise RuntimeError('Restarted node did not catch up.')
    for i in range(args.nodes):
        print(f'  n{i+1}: {status(i)}', flush=True)
    print('PASS: confirmed order survived leader termination; restarted node caught up over real gRPC.', flush=True)
    succeeded = True
finally:
    for process in processes.values():
        if process.poll() is None:
            process.terminate()
    for process in processes.values():
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    for log in logs:
        log.close()
    if args.keep or not succeeded:
        print(f'Kept node databases and logs: {work}', flush=True)
    else:
        shutil.rmtree(work)
