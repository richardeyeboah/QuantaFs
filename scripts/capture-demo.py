#!/usr/bin/env python3
"""Capture actual Kubernetes commands/results for the edited demo; no simulated outcomes."""
import contextlib
import fcntl
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'docs/media/demo-evidence'
spec = importlib.util.spec_from_file_location('recovery_demo', ROOT / 'scripts/kubernetes-demo.py')
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class Tee:
    def __init__(self, file): self.file = file
    def write(self, text):
        sys.__stdout__.write(text)
        self.file.write(text)
        self.file.flush()
    def flush(self):
        sys.__stdout__.flush()
        self.file.flush()


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    path = OUTPUT / 'trace.json'
    trace = {'captured_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
             'description': 'Real local Kubernetes recovery run; edited replay shortens waits.',
             'events': [], 'passed': False}
    start = time.monotonic()
    original = demo.kube
    def recorded(*args, **options):
        began = time.monotonic() - start
        result = original(*args, **options)
        trace['events'].append({'started_seconds': round(began, 3),
            'finished_seconds': round(time.monotonic() - start, 3),
            'args': list(args), 'stdout': result.stdout, 'stderr': result.stderr,
            'returncode': result.returncode})
        path.write_text(json.dumps(trace, indent=2) + '\n')
        return result
    demo.kube = recorded
    with (OUTPUT / 'transcript.txt').open('w') as transcript:
        try:
            with contextlib.redirect_stdout(Tee(transcript)):
                demo.main()
            trace['passed'] = True
        finally:
            trace['duration_seconds'] = round(time.monotonic() - start, 3)
            path.write_text(json.dumps(trace, indent=2) + '\n')


if __name__ == '__main__':
    with open(Path(tempfile.gettempdir()) / 'quantafs-kubernetes-exercise.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main()
