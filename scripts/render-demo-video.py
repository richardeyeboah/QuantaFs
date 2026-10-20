#!/usr/bin/env python3
"""Edit captured Kubernetes evidence into a captioned silent MP4 and README GIF.

Requires Pillow and imageio-ffmpeg. No outcomes are simulated: capture-demo.py must
finish successfully first. Waits are shortened; this is an annotated replay.
"""
import json
from functools import lru_cache
import math
from pathlib import Path
import subprocess
import textwrap

import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/media'
TRACE = json.loads((OUT / 'demo-evidence/trace.json').read_text())
if not TRACE['passed']:
    raise SystemExit('Capture must pass before rendering')
EVENTS = TRACE['events']
W, H, FPS = 1280, 720, 20
BG, PANEL, LINE = '#090f19', '#111c2c', '#26374c'
WHITE, MUTED, GREEN, BLUE, RED = '#f0f5fc', '#9aadc3', '#67e6b5', '#8cb9ff', '#ff8a91'
FONTROOT = Path('/System/Library/Fonts/Supplemental')

@lru_cache(maxsize=64)
def font(size, bold=False, mono=False):
    filename = 'Andale Mono.ttf' if mono else ('Arial Bold.ttf' if bold else 'Arial.ttf')
    return ImageFont.truetype(str(FONTROOT / filename), size)


def select(predicate):
    return [e for e in EVENTS if predicate(e['args'])]


statuses = select(lambda a: 'cluster-status' in a)
initial = [dict(v.split('=', 1) for v in e['stdout'].split()) for e in statuses[:3]]
final = [dict(v.split('=', 1) for v in e['stdout'].split()) for e in statuses[-3:]]
leader = next(s['id'] for s in initial if s['role'] == 'LEADER')
objects = select(lambda a: a[:2] == ['get', 'pod'] and a[2] == leader)
old, new = [json.loads(e['stdout'])['metadata']['uid'] for e in objects]
pvcs = select(lambda a: a[:2] == ['get', 'pvc'] and a[2] == 'data-' + leader)
volume = json.loads(pvcs[0]['stdout'])['spec']['volumeName']
assert json.loads(pvcs[0]['stdout'])['metadata']['uid'] == json.loads(pvcs[1]['stdout'])['metadata']['uid']
assert old != new
failure = next(e for e in EVENTS if 'cluster-client' in e['args'] and e['returncode'] != 0)
error_line = next(line for line in failure['stderr'].splitlines() if line.startswith('error:'))
assert 'DEADLINE_EXCEEDED' in error_line or 'UNAVAILABLE' in error_line
assert len({s['commit'] for s in final}) == 1 and all(s['commit'] == s['applied'] for s in final)
readback = select(lambda a: 'cluster-client' in a and a[-2:] == ['get', 'kubernetes-key'])[-1]['stdout'].strip()

SCENES = [
    {'seconds': 7, 'kind': 'intro'},
    {'seconds': 9, 'title': 'Write. Replicate. Acknowledge.',
     'subtitle': 'A successful reply follows majority commitment and local application.',
     'command': 'cluster-client ... put kubernetes-key "local persistent value"',
     'lines': [('OK', GREEN), ('get kubernetes-key', MUTED), ('local persistent value', WHITE)],
     'caption': 'Java + gRPC + Raft + RocksDB. Three voters, each with its own disk state.'},
    {'seconds': 8, 'title': 'Now remove the elected leader.',
     'subtitle': f'The live test deletes {leader}. Its persistent claim is preserved.',
     'command': f'kubectl -n quantafs-local delete pod {leader}',
     'lines': [(f'pod "{leader}" deleted from quantafs-local namespace', RED)],
     'caption': 'Kubernetes replaces the process. Raft coordinates which data is committed.', 'mode': 'deleted'},
    {'seconds': 8, 'title': 'The client keeps working.',
     'subtitle': 'Leader discovery and bounded retries route the next mutation.',
     'command': 'cluster-client ... put after-pod-replacement "still committed"',
     'lines': [('OK', GREEN), ('get after-pod-replacement', MUTED), ('still committed', WHITE)],
     'caption': 'These replies come from the captured run; no fixed failover-time claim is made.', 'mode': 'recovered'},
    {'seconds': 10, 'title': 'A new Pod. The same storage.',
     'subtitle': 'A different process identity reopens the original persistent volume.',
     'command': f'compare Pod + PVC metadata for {leader}',
     'lines': [('old Pod UID: ' + old[:27] + '...', MUTED),
               ('new Pod UID: ' + new[:27] + '...', WHITE),
               ('same volume: ' + volume, GREEN)],
     'caption': 'The test checks the full Pod UID, PVC UID, and bound volume for identity/reuse.', 'mode': 'recovered'},
    {'seconds': 10, 'title': 'No majority? Refuse the read.',
     'subtitle': 'Two voters stop. One voter cannot commit a fresh read barrier.',
     'command': 'cluster-client ... get kubernetes-key',
     'lines': [('DEADLINE_EXCEEDED', RED), ('Read refused. Remaining voter: halted=false', WHITE)],
     'caption': 'Expected safety behavior: unavailable is better than returning an unverified local value.', 'mode': 'minority'},
    {'seconds': 10, 'title': 'Restore the voters. Read the data.',
     'subtitle': 'All three nodes apply the same committed history after recovery.',
     'command': 'cluster-client ... get kubernetes-key',
     'lines': [(readback, GREEN), (f'commit={final[0]["commit"]}  applied={final[0]["applied"]}  on all three nodes', WHITE),
               ('PASS: majority restored; original data readable', GREEN)],
     'caption': 'The same data survives Pod replacement and the majority-loss exercise.', 'mode': 'final'},
    {'seconds': 8, 'kind': 'outro'},
]
TOTAL = sum(s['seconds'] for s in SCENES)


def text(draw, xy, value, size=24, color=WHITE, bold=False, mono=False):
    draw.text(xy, value, font=font(size, bold, mono), fill=color)


def wrap(draw, xy, value, width=100, size=21, color=MUTED):
    for i, line in enumerate(textwrap.wrap(value, width)):
        text(draw, (xy[0], xy[1] + i * (size + 7)), line, size, color)


def nodes(draw, mode, t):
    states = final if mode == 'final' else initial
    xs = [64, 468, 872]
    for i in range(2):
        draw.line((xs[i]+336, 306, xs[i+1], 306), fill=LINE, width=2)
        if mode not in ('minority', 'deleted'):
            x = xs[i]+336 + int((t % 1) * 68)
            draw.ellipse((x-4, 302, x+4, 310), fill=GREEN)
    for i, s in enumerate(states):
        absent = (mode == 'deleted' and s['id'] == leader) or (mode == 'minority' and i > 0)
        color = RED if absent else (GREEN if s['role'] == 'LEADER' and mode not in ('recovered','minority') else BLUE)
        role = 'STOPPED' if absent else ('ONLINE' if mode == 'recovered' else ('CANDIDATE' if mode == 'minority' else s['role']))
        x = xs[i]
        draw.rounded_rectangle((x, 252, x+336, 362), radius=16, fill=PANEL, outline=color, width=2)
        draw.ellipse((x+20, 273, x+30, 283), fill=color)
        text(draw, (x+44, 267), s['id'], 25, WHITE, True)
        text(draw, (x+20, 312), role, 16, color, mono=True)
        text(draw, (x+194, 312), 'PVC retained', 15, MUTED)


def frame(index, local, elapsed):
    s = SCENES[index]
    im = Image.new('RGB', (W,H), BG)
    d = ImageDraw.Draw(im)
    # Restrained background grid and brand.
    for x in range(0,W,80): d.line((x,0,x,H), fill='#0d1623')
    d.rounded_rectangle((40,30,77,67),radius=9,fill=GREEN)
    text(d,(49,34),'Q',25,BG,True)
    text(d,(92,34),'QuantaFs',27,WHITE,True)
    text(d,(902,40),'RICHARD EFFAH YEBOAH',16,MUTED)
    d.line((40,88,1240,88),fill=LINE,width=1)
    if s.get('kind') == 'intro':
        text(d,(64,130),'FAULT-TOLERANT KEY-VALUE STORAGE',18,GREEN,True)
        for i,line in enumerate(['Save it.', 'Lose a server.', 'Keep the data.']):
            text(d,(60,190+i*82),line,70,WHITE if i<2 else GREEN,True)
        text(d,(66,500),'A real Kubernetes recovery run, edited into 70 seconds.',24,MUTED)
        text(d,(66,548),'JAVA  /  RAFT  /  GRPC  /  ROCKSDB  /  KUBERNETES',17,BLUE,mono=True)
        d.rounded_rectangle((930,165,1200,435),radius=25,fill=PANEL,outline=LINE,width=2)
        for i,label in enumerate(['3 voters','1 leader','Separate volumes']):
            text(d,(960,205+i*70),label,24,WHITE,True)
    elif s.get('kind') == 'outro':
        text(d,(64,132),'Recovery you can inspect.',51,WHITE,True)
        text(d,(66,207),'Captured behavior. Reproducible commands. Explicit limits.',24,MUTED)
        labels=['Leader Pod replaced; original PVC reused', 'Majority-loss read refused; quorum restored', 'Committed data verified after recovery']
        for i,label in enumerate(labels):
            y=285+i*68
            d.rounded_rectangle((64,y,1216,y+50),radius=10,fill=PANEL)
            text(d,(83,y+12),'PASS',20,GREEN,True)
            text(d,(165,y+10),label,24,WHITE)
        text(d,(66,530),'github.com/richardeyeboah/QuantaFs',29,BLUE,True)
        text(d,(66,580),'Local three-voter demo. One host. TLS/authentication remain future work.',20,MUTED)
    else:
        text(d,(64,117),f'{index:02d} / RECOVERY IN PRACTICE',16,GREEN,True)
        text(d,(61,151),s['title'],44,WHITE,True)
        text(d,(64,211),s['subtitle'],21,MUTED)
        nodes(d,s.get('mode','initial'),local)
        d.rounded_rectangle((64,392,1216,588),radius=15,fill='#070c13',outline=LINE,width=1)
        for j,c in enumerate(['#ff817b','#f2c86a','#65d6aa']):d.ellipse((82+j*17,408,89+j*17,415),fill=c)
        text(d,(152,401),'ANNOTATED CAPTURE / COMMANDS ABBREVIATED',14,MUTED,mono=True)
        cmd='$ '+s['command']
        reveal=int(min(1,max(0,(local-.35)/1.25))*len(cmd))
        text(d,(84,436),cmd[:reveal],18,BLUE,mono=True)
        if local>1.9:
            for j,(line,c) in enumerate(s['lines']):
                if local>1.9+j*.35:text(d,(85,473+j*31),line,20,c,mono=True)
        wrap(d,(65,610),s['caption'],105,20,MUTED)
    d.line((40,680,1240,680),fill=LINE,width=3)
    d.line((40,680,40+int(1200*elapsed/TOTAL),680),fill=GREEN,width=3)
    text(d,(42,694),'REAL LOCAL RUN  /  EDITED REPLAY  /  WAITS SHORTENED',12,MUTED,mono=True)
    text(d,(1160,694),f'{index+1:02d} / 08',12,MUTED,mono=True)
    # Short fades between chapters, not distracting motion over the evidence.
    fade=min(1,1 if index == 0 else local/.3,(s['seconds']-local)/.3)
    if fade<1: im=Image.blend(Image.new('RGB',(W,H),BG),im,max(0,fade))
    return im


if __name__ == '__main__':
    OUT.mkdir(parents=True,exist_ok=True)
    ffmpeg=imageio_ffmpeg.get_ffmpeg_exe()
    target=OUT/'quantafs-demo.mp4'
    command=[ffmpeg,'-y','-loglevel','error','-f','rawvideo','-vcodec','rawvideo','-pix_fmt','rgb24',
        '-s',f'{W}x{H}','-r',str(FPS),'-i','-','-an','-c:v','libx264','-preset','fast','-crf','21',
        '-pix_fmt','yuv420p','-movflags','+faststart',str(target)]
    process=subprocess.Popen(command,stdin=subprocess.PIPE)
    elapsed=0
    for index,scene in enumerate(SCENES):
        preview=frame(index,min(4,scene['seconds']-1),elapsed+4)
        preview.save(OUT/f'chapter-{index+1:02d}.png')
        for f in range(scene['seconds']*FPS):
            t=f/FPS
            process.stdin.write(frame(index,t,elapsed+t).tobytes())
        elapsed+=scene['seconds']
        print(f'Rendered chapter {index+1}/8',flush=True)
    process.stdin.close()
    if process.wait()!=0:raise SystemExit('Video encoding failed')
    subprocess.run([ffmpeg,'-y','-loglevel','error','-i',str(target),'-vf',
        'fps=4,scale=640:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=96[p];[b][p]paletteuse=dither=bayer',
        '-loop','0',str(OUT/'quantafs-demo-preview.gif')],check=True)
    (OUT/'chapters.json').write_text(json.dumps({'duration_seconds':TOTAL,'fps':FPS,
        'editing':'Captioned replay of captured command results; real waits shortened; no audio.',
        'capture':'demo-evidence/trace.json','chapters':[{'title':s.get('title',s.get('kind')),'seconds':s['seconds']} for s in SCENES]},indent=2)+'\n')
    print(f'Video complete: {TOTAL}s, {target}',flush=True)
