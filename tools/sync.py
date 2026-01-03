#!/usr/bin/env python3
import os, sys, time, json, subprocess, argparse
from pathlib import Path
from datetime import datetime

class Config:
    def __init__(self):
        self.local_input_dir = Path.home() / 'pdf-processor-local' / 'input'
        self.local_output_dir = Path.home() / 'pdf-processor-local' / 'output'
        self.repo_dir = Path(__file__).parent.parent
        self.check_interval = 30
        self.max_wait_time = 7200

CONFIG = Config()

def run_git(args, cwd=None):
    cwd = cwd or CONFIG.repo_dir
    result = subprocess.run(['git'] + args, cwd=cwd, capture_output=True, text=True, encoding='utf-8')
    return result.returncode == 0, result.stdout, result.stderr

def ensure_dirs():
    CONFIG.local_input_dir.mkdir(parents=True, exist_ok=True)
    CONFIG.local_output_dir.mkdir(parents=True, exist_ok=True)
    print('Input folder: ', CONFIG.local_input_dir)
    print('Output folder:', CONFIG.local_output_dir)

def get_local_pdfs():
    return list(CONFIG.local_input_dir.glob('*.pdf'))

def get_repo_outputs():
    return list((CONFIG.repo_dir / 'output').glob('*.md'))

def upload_pdfs():
    ensure_dirs()
    pdfs = get_local_pdfs()
    if not pdfs:
        print('No PDF files in', CONFIG.local_input_dir)
        return False
    print('Found', len(pdfs), 'PDF(s):')
    for pdf in pdfs:
        size = pdf.stat().st_size / (1024*1024)
        print('  -', pdf.name, '(' + str(round(size, 1)) + ' MB)')
    print('Pulling latest...')
    run_git(['pull', 'origin', 'main'])
    repo_input = CONFIG.repo_dir / 'input'
    repo_input.mkdir(exist_ok=True)
    for pdf in pdfs:
        (repo_input / pdf.name).write_bytes(pdf.read_bytes())
        print('Copied', pdf.name)
    run_git(['add', 'input/*.pdf'])
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = 'Upload ' + str(len(pdfs)) + ' PDF(s) - ' + ts
    success, out, err = run_git(['commit', '-m', msg])
    if not success and 'nothing to commit' in (out + err):
        print('Already up to date')
        return True
    success, _, err = run_git(['push', 'origin', 'main'])
    if success:
        print('Pushed!')
    else:
        print('Push failed:', err)
    return success

def get_queue_status():
    f = CONFIG.repo_dir / 'logs' / 'queue_status.json'
    if not f.exists():
        return None
    try:
        return json.loads(f.read_text(encoding='utf-8'))
    except:
        return None

def check_status():
    print('Fetching status...')
    run_git(['pull', 'origin', 'main'])
    status = get_queue_status()
    if not status:
        print('No status found')
        return
    jobs = status.get('jobs', [])
    stats = status.get('stats', {})
    print('Updated:', status.get('updated_at'))
    for s, c in stats.get('by_status', {}).items():
        print(' ', s + ':', c)
    cost = stats.get('total_cost_usd', 0)
    print('Cost: $' + str(round(cost, 4)))
    for j in jobs:
        print('  [' + j['status'] + ']', j['filename'])

def download_outputs():
    ensure_dirs()
    print('Pulling results...')
    run_git(['pull', 'origin', 'main'])
    outputs = [o for o in get_repo_outputs() if o.name != '.gitkeep']
    if not outputs:
        print('No outputs yet')
        return 0
    n = 0
    for o in outputs:
        dest = CONFIG.local_output_dir / o.name
        if dest.exists() and dest.read_bytes() == o.read_bytes():
            continue
        dest.write_bytes(o.read_bytes())
        print('Downloaded', o.name)
        n += 1
    if n:
        print('Downloaded', n, 'file(s)')
    else:
        print('All up to date')
    return n

def watch_and_download():
    ensure_dirs()
    print('Watching (interval:', CONFIG.check_interval, 's)...')
    start = time.time()
    last = set()
    try:
        while time.time() - start < CONFIG.max_wait_time:
            run_git(['pull', 'origin', 'main'])
            status = get_queue_status()
            if status:
                jobs = status.get('jobs', [])
                done = {j['filename'] for j in jobs if j['status'] == 'completed'}
                pend = {j['filename'] for j in jobs if j['status'] in ('pending', 'processing')}
                fail = {j['filename'] for j in jobs if j['status'] == 'failed'}
                if done - last:
                    print('New:', done - last)
                    download_outputs()
                    last = done
                msg = '\rOK:' + str(len(done)) + ' Pend:' + str(len(pend)) + ' NG:' + str(len(fail)) + '  '
                sys.stdout.write(msg)
                sys.stdout.flush()
                if not pend and (done or fail):
                    print('\nAll done!')
                    download_outputs()
                    break
            time.sleep(CONFIG.check_interval)
    except KeyboardInterrupt:
        print('\nStopped')
    print('Results:', CONFIG.local_output_dir)

def full_sync():
    print('=== Full Sync ===')
    if upload_pdfs():
        time.sleep(5)
        watch_and_download()

def main():
    p = argparse.ArgumentParser(description='PDF Processor Sync Tool')
    p.add_argument('cmd', choices=['upload', 'watch', 'sync', 'status', 'download'], help='Command to run')
    p.add_argument('--input-dir', type=Path, help='Local input directory')
    p.add_argument('--output-dir', type=Path, help='Local output directory')
    a = p.parse_args()
    if a.input_dir:
        CONFIG.local_input_dir = a.input_dir
    if a.output_dir:
        CONFIG.local_output_dir = a.output_dir
    cmds = {
        'upload': upload_pdfs,
        'watch': watch_and_download,
        'sync': full_sync,
        'status': check_status,
        'download': download_outputs
    }
    cmds[a.cmd]()

if __name__ == '__main__':
    main()
