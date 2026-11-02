"""
Build or resume maps.db from Navigation.json.

Usage:
    python ingest.py                     # build (or resume if interrupted)
    python ingest.py --rebuild           # force full rebuild
    python ingest.py --no-memory         # skip RAM check, build on disk
    python ingest.py --no-parallel       # build in RAM, then copy to disk
    python ingest.py --parallel          # force parallel RAM+disk build
    python ingest.py --no-fts            # skip FTS5 index (LIKE fallback)
    STEAM_API_KEY=xxx python ingest.py   # with Steam metadata pre-fetch

Fresh builds normally run in parallel: an in-memory DB is built on the
main thread while a background worker streams the same operations into
maps.db on disk. Both DBs end up byte-identical. When --no-parallel is
passed (or RAM is too low for an in-memory build), the old behaviour is
used: build in RAM, then copy once with sqlite3's backup().

Parallel mode is auto-disabled for sources above PARALLEL_MAX_MB
(default 15 MB). Above that size, the FTS5 trigram rebuild dominates
and gets done *twice* in parallel mode (once in RAM, once replayed on
disk), which is strictly slower than a single RAM build + backup().
Force it back on with --parallel (or PARALLEL_MAX_MB=<n>) if you want
the incremental-valid-on-disk guarantee more than the speed.

Resumable: if interrupted during pre-fetch, just re-run. The maps build
(~2 s on a typical source) is skipped once complete; only missing Steam
details and author profiles are fetched. Authors whose profiles are
private / deleted are cached as NULL rows so they are not re-fetched on
every run.

Memory usage: a fresh build is done entirely in RAM when enough is
detected, then written to disk in one shot (much faster than fsync-heavy
on-disk index building). --no-memory forces the on-disk path. RAM
detection uses psutil when installed, /proc/meminfo on Linux, and falls
back to the on-disk path elsewhere.

Rate limiting:
    The details and authors pre-fetch phases run *concurrently* by
    default, each with its own worker pool (DETAILS_WORKERS and
    AUTHORS_WORKERS). Both phases share a single adaptive backoff: a
    429 seen by any worker slows every request across both phases, and
    the delay decays back toward STEAM_SLEEP once things are clean. On
    429, Retry-After is honoured first.

    NOTE: total concurrent requests = DETAILS_WORKERS + AUTHORS_WORKERS.
    The default of 3 + 3 = 6 is conservative. Steam starts rate-limiting
    around ~8-10 total; the shared backoff will absorb some of that but
    throughput will drop. If you're seeing 0.8 req/s per phase, that's
    latency, not throttling — bump workers. If you start seeing the
    "429 rate-limited" lines, lower them. Split the two if one phase
    dominates: details is ~2.5x the batch count of authors and each
    response is heavier, so DETAILS_WORKERS=6 AUTHORS_WORKERS=2 is a
    reasonable first experiment.

SQLite tuning:
    The build runs with PRAGMA threads = os.cpu_count() so CREATE INDEX
    and the FTS5 rebuild sort in parallel. Every post-build connection
    switches to WAL + synchronous=NORMAL, which makes the batch commits
    during pre-fetch cheap. Pre-fetch commits are batched every
    COMMIT_EVERY batches (10) rather than per-batch.

Env vars:
    STEAM_API_KEY     Steam Web API key (required for pre-fetch)
    STEAM_SLEEP       Base seconds between requests per worker (default 0.25)
    STEAM_WORKERS     Default worker count for each prefetch phase (default 3)
    DETAILS_WORKERS   Override worker count for the details phase
                      (default: STEAM_WORKERS)
    AUTHORS_WORKERS   Override worker count for the authors phase
                      (default: STEAM_WORKERS)
    AUTHORS_MIN_MAPS  Only fetch authors with >= N maps (default 1 = all)
    PARALLEL_MAX_MB   Auto-disable parallel mode above this source size
                      in MB (default 15; set to 0 to always parallel, or
                      a huge value to never auto-disable)
    SKIP_FTS          Set to 1 to skip the FTS5 build (same as --no-fts)
"""
import argparse
import json
import os
import queue
import re
import shutil
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

try:
    import requests
except ImportError:
    requests = None

# orjson is a drop-in ~2-3x faster JSON parser on large inputs. Fall back
# to stdlib json if unavailable.
try:
    import orjson as _orjson
except ImportError:
    _orjson = None

DB_PATH   = 'maps.db'
SOURCE    = 'Navigation.json'
TYPES_OUT = 'types.json'
META_OUT  = 'meta.json'

STEAM_KEY         = os.environ.get('STEAM_API_KEY', '')
STEAM_BATCH       = 100
STEAM_SLEEP       = float(os.environ.get('STEAM_SLEEP', '0.25'))
STEAM_WORKERS     = max(1, int(os.environ.get('STEAM_WORKERS', '3')))
STEAM_TIMEOUT     = (10, 30)
STEAM_MAX_RETRIES = 3
STEAM_MAX_429S    = 5
AUTHORS_MIN_MAPS  = max(1, int(os.environ.get('AUTHORS_MIN_MAPS', '1')))

# FIXED: per-phase worker counts. Total concurrent requests is the sum,
# so bumping one without lowering the other increases 429 risk.
DETAILS_WORKERS = max(1, int(os.environ.get('DETAILS_WORKERS',
                                            str(STEAM_WORKERS))))
AUTHORS_WORKERS = max(1, int(os.environ.get('AUTHORS_WORKERS',
                                            str(STEAM_WORKERS))))

# CHANGED: default lowered from 50 to 15 — the FTS rebuild is already the
# dominant cost above ~15 MB of source, so parallel mode loses from there.
PARALLEL_MAX_MB = float(os.environ.get('PARALLEL_MAX_MB', '15'))

# CHANGED: skip FTS via env var in addition to the --no-fts flag.
SKIP_FTS_ENV = os.environ.get('SKIP_FTS', '') not in ('', '0', 'false', 'False')

# CHANGED: number of SQLite auxiliary worker threads for CREATE INDEX and
# the FTS5 rebuild. Caps out silently at SQLITE_MAX_WORKER_THREADS (usually
# 8) on older builds, so passing the raw core count is safe.
_SQLITE_THREADS = os.cpu_count() or 1

# Chunk size for parallel disk writes — small enough to give backpressure
# and fine-grained progress, large enough that per-op overhead is tiny.
DISK_CHUNK = 5000

# CHANGED: commit every N batches during prefetch instead of every batch.
# With WAL + synchronous=NORMAL commits are cheap, but this cuts the
# syscall count by an order of magnitude on fresh runs.
COMMIT_EVERY = 10


RULES = [
    ('deathrun', 'Deathrun',  'deathrun'),
    ('surf',     'Surf',      'surf'),
    ('bhop',     'Bhop',      'bhop'),
    ('kz_',      'KZ',        'kz'),
    ('ze_',      'Zombie',    'zombie'),
    ('zm_',      'Zombie',    'zombie'),
    ('mg_',      'Minigame',  'minigame'),
    ('mini',     'Minigame',  'minigame'),
    ('aim',      'Aim',       'aim'),
    ('awp',      'AWP',       'awp'),
    ('scout',    'Scout',     'awp'),
    ('de_',      'Defuse',    'defuse'),
    ('cs_',      'Hostage',   'hostage'),
    ('ar_',      'Arms Race', 'armsrace'),
    ('fy_',      'Fight Yard','fightyard'),
    ('gg_',      'Gun Game',  'gungame'),
    ('he_',      'HE Only',   'he'),
    ('ka_',      'Knife',     'knife'),
    ('knife',    'Knife',     'knife'),
    ('jb_',      'Jailbreak', 'jail'),
    ('jail',     'Jailbreak', 'jail'),
    ('dm_',      'Deathmatch','dm'),
    ('training', 'Training',  'training'),
]

# Fast prefix lookup: build a dict keyed by the prefix string and probe
# only the unique prefix lengths in descending order. Equivalent to the
# old linear startswith loop (none of the current prefixes are prefixes
# of each other), ~5x faster because it's a handful of dict lookups
# instead of up to 23 str.startswith calls per row.
_CLASSIFY = {p: (label, cls) for p, label, cls in RULES}
_CLASSIFY_LENS = sorted({len(p) for p in _CLASSIFY}, reverse=True)


def classify(bsp):
    n = (bsp or '').lower()
    for L in _CLASSIFY_LENS:
        r = _CLASSIFY.get(n[:L])
        if r is not None:
            return r
    return 'Other', 'other'


_wid_re = re.compile(r'[?&]id=(\d+)')

def workshop_id(link):
    if not link:
        return None
    m = _wid_re.search(link)
    return m.group(1) if m else None


# --- ANSI / terminal detection ----------------------------------------

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def _vlen(s):
    """Visible length of s, ignoring ANSI SGR sequences."""
    return len(_ANSI_RE.sub('', s))


def _enable_windows_vt():
    """Turn on ENABLE_VIRTUAL_TERMINAL_PROCESSING for stdout on Windows.

    Python 3.6+ enables this itself when it detects a real console handle,
    but that detection fails under some PowerShell / wrapper setups, which
    is exactly how you end up with literal "<- [32m" garbage in the terminal.
    This call is idempotent and a no-op on non-Windows platforms.

    Returns True if the calling process can emit ANSI escapes and have
    them interpreted (either non-Windows, or Windows with VT now enabled).
    """
    if not sys.platform.startswith('win'):
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # -11 = STD_OUTPUT_HANDLE
        handle = kernel32.GetStdHandle(-11)
        if handle in (0, -1):
            return False
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        if not kernel32.SetConsoleMode(
                handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING):
            return False
        return True
    except Exception:
        return False


_IS_TTY = sys.stdout.isatty()
_VT_OK  = _enable_windows_vt() if _IS_TTY else False


def _supports_color():
    if os.environ.get('NO_COLOR'):        # https://no-color.org/
        return False
    if os.environ.get('FORCE_COLOR'):
        return True
    if not _IS_TTY or not _VT_OK:
        return False
    if os.environ.get('TERM') == 'dumb':
        return False
    return True


_COLOR = _supports_color()


def _c(code, s):
    return f'\x1b[{code}m{s}\x1b[0m' if _COLOR else s


# --- truecolor palette ------------------------------------------------
# Tuned for Windows Terminal's default dark theme but reads fine on most
# dark terminals. RGB triplets, emitted as 24-bit SGR.
P_BRAND = (110, 170, 255)   # borders, headers
P_ACCENT= (192, 140, 255)   # icons, section labels
P_OK    = (110, 220, 150)   # success, progress fill
P_WARN  = (250, 200, 100)   # warnings
P_ERR   = (250, 120, 120)   # errors
P_TEXT  = (220, 225, 235)   # primary text
P_DIM   = (130, 135, 150)   # secondary text
P_TRACK = (58, 62, 72)      # progress track (empty portion)


def _fg(rgb, s):
    if not _COLOR:
        return s
    r, g, b = rgb
    return f'\x1b[38;2;{r};{g};{b}m{s}\x1b[0m'


def _bold(s):
    return f'\x1b[1m{s}\x1b[0m' if _COLOR else s


# --- bar rendering -----------------------------------------------------

# Vertical-eighths blocks: 8 sub-cell positions per character cell.
_PARTIAL = ' ▏▎▍▌▋▊▉'   # 0..7 eighths
_FULL    = '█'
_EMPTY   = '░'


def _bar(frac, width):
    frac    = max(0.0, min(1.0, frac))
    eighths = int(frac * width * 8 + 0.5)
    full, rem = divmod(eighths, 8)
    if full >= width:
        return _fg(P_OK, _FULL * width)
    parts = []
    if full:
        parts.append(_fg(P_OK, _FULL * full))
    if rem:
        parts.append(_fg(P_OK, _PARTIAL[rem]))
    empty = width - full - (1 if rem else 0)
    if empty:
        parts.append(_fg(P_TRACK, _EMPTY * empty))
    return ''.join(parts)


# --- duration / size formatting ---------------------------------------

def _fmt_dur(sec):
    if sec < 0:
        sec = 0
    if sec < 60:
        return f'{sec:.0f}s'
    if sec < 3600:
        return f'{sec/60:.0f}m{sec%60:02.0f}s'
    return f'{sec/3600:.0f}h{(sec%3600)/60:.0f}m'


def _fmt_bytes(b):
    for unit in ('B', 'KB', 'MB', 'GB'):
        if b < 1024:
            return f'{b} {unit}' if unit == 'B' else f'{b:.1f} {unit}'
        b /= 1024
    return f'{b:.1f} TB'


def _pct(n, total):
    if not total:
        return '0%'
    return f'{n * 100 / total:.0f}%'


# --- section header ----------------------------------------------------

def _term_width(cap=78):
    cols = shutil.get_terminal_size((110, 24)).columns - 1
    return min(cols, cap)


def _header(title, subtitle=None, color=P_ACCENT):
    """Print a box-drawing section header."""
    width = _term_width()
    inner = width - 2
    title_str = f' ◆ {title.upper()} '
    if len(title_str) > inner - 3:
        title_str = title_str[:inner - 4] + '… '
    right = max(1, inner - 1 - len(title_str))

    if _COLOR:
        print(_fg(P_BRAND, '╭─')
              + _fg(color, title_str)
              + _fg(P_BRAND, '─' * right + '╮'))
    else:
        print('╭─' + title_str + '─' * right + '╮')

    if subtitle:
        for line in subtitle.split('\n'):
            if len(line) > inner - 4:
                line = line[:inner - 5] + '…'
            pad = max(0, inner - 3 - len(line))
            if _COLOR:
                print(_fg(P_BRAND, '│ ')
                      + _fg(P_DIM, line)
                      + ' ' * pad
                      + _fg(P_BRAND, ' │'))
            else:
                print('│ ' + line + ' ' * pad + ' │')
        if _COLOR:
            print(_fg(P_BRAND, '╰' + '─' * inner + '╯'))
        else:
            print('╰' + '─' * inner + '╯')


def _divider():
    width = _term_width()
    if _COLOR:
        print('  ' + _fg(P_TRACK, '─' * (width - 2)))
    else:
        print('  ' + '─' * (width - 2))


# --- step line ---------------------------------------------------------

def _step(icon, icon_color, label, detail='', elapsed=None, label_w=12):
    """Print a build step, right-aligning `elapsed` against the terminal."""
    cols = _term_width()
    prefix_plain = f'  {icon} '
    label_plain = f'{label:<{label_w}}'

    plain = prefix_plain + label_plain
    if detail:
        plain += '  ' + detail

    time_plain = f'{elapsed:.2f}s' if elapsed is not None else ''
    pad = max(1, cols - len(plain) - len(time_plain) - 1) if time_plain else 0

    if _COLOR:
        out = f'  {_fg(icon_color, icon)} '
        out += _fg(P_TEXT, label_plain)
        if detail:
            out += '  ' + _fg(P_DIM, detail)
        if time_plain:
            out += ' ' * pad + _fg(P_DIM, time_plain)
    else:
        out = plain
        if time_plain:
            out += ' ' * pad + time_plain
    print(out)


# --- progress line -----------------------------------------------------

def _truncate_visible(s, width):
    """Truncate to `width` visible columns, preserving ANSI sequences."""
    if _vlen(s) <= width:
        return s
    out, vis, i = [], 0, 0
    while i < len(s) and vis < width:
        m = _ANSI_RE.match(s, i)
        if m:
            out.append(m.group()); i = m.end()
        else:
            out.append(s[i]); vis += 1; i += 1
    if _COLOR:
        out.append('\x1b[0m')
    return ''.join(out)


def _progress(label, done, total, started_at, extra=''):
    elapsed = time.time() - started_at
    frac    = done / total if total else 0.0
    rate    = done / elapsed if elapsed > 0 else 0.0
    remain  = (total - done) / rate if rate > 0 else 0.0
    pct     = frac * 100.0

    width = shutil.get_terminal_size((110, 24)).columns - 1
    if width >= 100:
        bar_w = 26
    elif width >= 70:
        bar_w = 18
    else:
        bar_w = 12

    bar = _bar(frac, bar_w)
    eta = _fmt_dur(remain) if rate > 0 and remain > 0 else '—'

    if _COLOR:
        line = (f'  {_fg(P_ACCENT, "◆")} {_fg(P_TEXT, f"{label:<8}")}  '
                f'{bar}  '
                f'{_fg(P_TEXT, f"{done:>6,}/{total:<6,}")}  '
                f'{_fg(P_OK, f"{pct:5.1f}%")}  '
                f'{_fg(P_DIM, f"{rate:4.1f}/s")}  '
                f'{_fg(P_DIM, f"ETA {eta}")}')
    else:
        line = (f'  {label:<8}  [{bar}]  {done:,}/{total:,} '
                f'{pct:5.1f}%  {rate:4.1f}/s  ETA {eta}')

    if extra and _vlen(extra) <= width - _vlen(line) - 2:
        line += '  ' + extra

    if _vlen(line) > width:
        line = _truncate_visible(line, width)

    if _IS_TTY and _VT_OK:
        sys.stdout.write('\r' + line + '\x1b[K')
    else:
        sys.stdout.write('\r' + line.ljust(width))
    sys.stdout.flush()


def _finish_progress():
    if _IS_TTY and _VT_OK:
        sys.stdout.write('\x1b[K')
    sys.stdout.write('\n')
    sys.stdout.flush()


# --- final summary box -------------------------------------------------

def _summary_box(title, icon, icon_color, rows):
    width = _term_width()
    inner = width - 2
    title_str = f' {icon} {title} '
    right = max(1, inner - 1 - len(title_str))

    if _COLOR:
        print(_fg(P_BRAND, '╭─')
              + _fg(icon_color, title_str)
              + _fg(P_BRAND, '─' * right + '╮'))
        print(_fg(P_BRAND, '│') + ' ' * inner + _fg(P_BRAND, '│'))
    else:
        print('╭─' + title_str + '─' * right + '╮')
        print('│' + ' ' * inner + '│')

    for label, value in rows:
        content_plain = f'  {label:<14} {value}'
        pad = max(0, inner - len(content_plain))
        if _COLOR:
            print(_fg(P_BRAND, '│')
                  + _fg(P_DIM, f'  {label:<14}')
                  + ' ' + _fg(P_TEXT, value)
                  + ' ' * pad
                  + _fg(P_BRAND, '│'))
        else:
            print('│' + content_plain + ' ' * pad + '│')

    if _COLOR:
        print(_fg(P_BRAND, '│') + ' ' * inner + _fg(P_BRAND, '│'))
        print(_fg(P_BRAND, '╰' + '─' * inner + '╯'))
    else:
        print('│' + ' ' * inner + '│')
        print('╰' + '─' * inner + '╯')


def _retry_after(resp, default=10.0):
    raw = resp.headers.get('Retry-After')
    if raw is None:
        return default
    try:
        return max(1.0, float(raw))
    except ValueError:
        return default


class _Backoff:
    """Shared adaptive sleep between workers.

    All workers consult the current delay before each request. A 429 from
    any worker bumps the delay for everyone; a clean run of requests decays
    it back toward the base value.

    The growth curve (x1.2 + 0.25, cap 3s) is deliberately gentle: the
    old curve (x1.5 + 0.5, cap 10s) hit the ceiling in ~6 bumps and took
    ~46 successful requests to decay back down, which turned a single
    429 storm into a multi-minute stall across the whole pool.
    """
    def __init__(self, base):
        self.base = max(0.0, base)
        self._cur = self.base
        self._lock = threading.Lock()

    def sleep(self):
        with self._lock:
            d = self._cur
        if d > 0:
            time.sleep(d)

    def bump(self, wait):
        with self._lock:
            self._cur = min(self._cur * 1.2 + 0.25, 3.0)
        time.sleep(wait)

    def decay(self):
        with self._lock:
            if self._cur > self.base:
                self._cur = max(self._cur * 0.9, self.base)


class _ParallelWriter:
    """Mirrors every DB op into a second on-disk DB on a worker thread.

    The public surface matches the subset of sqlite3.Connection we use:
    executescript / executemany / execute / commit. The memory connection
    runs on the caller's thread so the in-RAM build stays as fast as
    before; the worker thread drains a queue and replays the same ops
    against maps.db in order.

    Order is preserved by the FIFO queue, so schema setup, inserts,
    indices, FTS rebuild and the _build_state flag all land on disk in
    the same sequence they hit memory.

    If the worker hits an error on an individual op it records it and
    keeps draining, so the producer never deadlocks; finish() re-raises
    the first error once the queue is empty.
    """

    def __init__(self, mem_conn, disk_path):
        self.mem = mem_conn
        self.disk_path = disk_path
        self._q = queue.Queue()  # unbounded: chunks are tiny relative to maps
        self._errors = []
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run, name='disk-writer', daemon=True)

    def start(self):
        self._thread.start()

    def _run(self):
        try:
            disk = sqlite3.connect(self.disk_path)
        except Exception as e:
            with self._lock:
                self._errors.append(e)
            return
        try:
            while True:
                item = self._q.get()
                if item is None:
                    break
                op, args = item
                try:
                    getattr(disk, op)(*args)
                    if op in ('executemany', 'execute'):
                        disk.commit()
                except Exception as e:
                    with self._lock:
                        self._errors.append(e)
        finally:
            disk.close()

    # --- writer interface used by _build_fresh ------------------------

    def executescript(self, sql):
        self.mem.executescript(sql)          # runs now, on this thread
        self._q.put(('executescript', (sql,)))

    def executemany(self, sql, rows):
        # Memory side: one big executemany (fast, in-RAM).
        self.mem.executemany(sql, rows)
        # Disk side: chunked so the worker commits incrementally and
        # backpressure stays predictable.
        if not isinstance(rows, list):
            rows = list(rows)
        for i in range(0, len(rows), DISK_CHUNK):
            self._q.put(('executemany', (sql, rows[i:i + DISK_CHUNK])))

    def execute(self, sql, params=()):
        cur = self.mem.execute(sql, params)
        self._q.put(('execute', (sql, params)))
        return cur

    def commit(self):
        self.mem.commit()
        # Worker commits per-op, so nothing to do here.

    def finish(self):
        self._q.put(None)
        self._thread.join()
        with self._lock:
            if self._errors:
                raise self._errors[0]


def _steam_get(session, url, label='request', verbose=True, backoff=None):
    """GET with retry / 429 handling.

    verbose=False silences all output (used from worker threads so we don't
    stomp on the main thread's progress bar). backoff, when provided, lets
    concurrent workers coordinate their sleep via a shared object.

    backoff.sleep() is called at the top of every attempt — including
    retries — so a worker that hits a 429 waits out the shared delay
    before trying again instead of hot-looping.
    """
    failures    = 0
    rate_limits = 0

    while failures <= STEAM_MAX_RETRIES and rate_limits <= STEAM_MAX_429S:
        if backoff is not None:
            backoff.sleep()
        try:
            r = session.get(url, timeout=STEAM_TIMEOUT)

            if r.status_code == 429:
                rate_limits += 1
                wait = _retry_after(r, default=10.0)
                if verbose:
                    _finish_progress()
                    print(f'  {_fg(P_WARN, "⚠")} [{label}] '
                          f'429 rate-limited — sleeping {wait:.0f}s '
                          f'(limit {rate_limits}/{STEAM_MAX_429S})')
                if backoff is not None:
                    backoff.bump(wait)
                else:
                    time.sleep(wait)
                continue

            if 500 <= r.status_code < 600:
                failures += 1
                if failures > STEAM_MAX_RETRIES:
                    break
                wait = 2 ** (failures - 1)
                if verbose:
                    _finish_progress()
                    print(f'  {_fg(P_WARN, "⚠")} [{label}] '
                          f'{r.status_code} server error — retry in {wait}s '
                          f'({failures}/{STEAM_MAX_RETRIES})')
                time.sleep(wait)
                continue

            if backoff is not None:
                backoff.decay()
            r.raise_for_status()
            return r

        except requests.exceptions.RequestException as e:
            failures += 1
            if failures > STEAM_MAX_RETRIES:
                break
            wait = 2 ** (failures - 1)
            if verbose:
                _finish_progress()
                print(f'  {_fg(P_WARN, "⚠")} [{label}] '
                      f'network error: {e} — retry in {wait}s '
                      f'({failures}/{STEAM_MAX_RETRIES})')
            time.sleep(wait)

    if verbose:
        _finish_progress()
        print(f'  {_fg(P_ERR, "✗")} [{label}] giving up after '
              f'{failures} failures, {rate_limits} rate-limits')
    return None


_thread_local = threading.local()

def _thread_session():
    """Each worker gets its own requests.Session (not thread-safe)."""
    s = getattr(_thread_local, 'session', None)
    if s is None:
        s = requests.Session()
        _thread_local.session = s
    return s


def _build_complete():
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            row = conn.execute(
                "SELECT value FROM _build_state WHERE key = 'maps_complete'"
            ).fetchone()
            return row is not None and row[0] == '1'
        except sqlite3.OperationalError:
            return False
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return False


def _wipe_db():
    for name in (DB_PATH, DB_PATH + '-wal', DB_PATH + '-shm'):
        if os.path.exists(name):
            os.remove(name)


def _available_ram():
    """Return available RAM in bytes, or None if we can't tell."""
    try:
        import psutil
        return psutil.virtual_memory().available
    except ImportError:
        pass
    if sys.platform.startswith('linux'):
        try:
            with open('/proc/meminfo') as f:
                for line in f:
                    if line.startswith('MemAvailable:'):
                        return int(line.split()[1]) * 1024
        except (OSError, ValueError):
            pass
    return None


def _should_build_in_memory(source_size):
    """Return True if RAM headroom indicates a full in-memory build fits."""
    avail = _available_ram()
    if avail is None:
        print(f'  {_fg(P_DIM, "·")} RAM check unavailable — building on disk.')
        return False
    # The on-disk DB tends to land around 2-3x the JSON, and FTS5 adds
    # roughly half again; round up generously.
    estimated = source_size * 4 + (256 * 1024 * 1024)
    ok = avail >= estimated
    avail_mb = avail / 1048576
    est_mb   = estimated / 1048576
    if ok:
        print(f'  {_fg(P_OK, "✓")} RAM check: {avail_mb:.0f} MB free · '
              f'~{est_mb:.0f} MB needed — building in RAM.')
    else:
        print(f'  {_fg(P_WARN, "!")} RAM check: {avail_mb:.0f} MB free · '
              f'~{est_mb:.0f} MB needed — building on disk.')
    return ok


def _save_memory_to_disk(mem_conn):
    """Atomically copy an in-memory DB into DB_PATH."""
    _wipe_db()
    disk = sqlite3.connect(DB_PATH)
    try:
        mem_conn.backup(disk)
    finally:
        disk.close()


def _load_json(path):
    """Load JSON via orjson when available, else stdlib json."""
    if _orjson is not None:
        with open(path, 'rb') as f:
            return _orjson.loads(f.read())
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _fts5_available(conn):
    """Probe whether FTS5 with the trigram tokenizer is usable on conn.

    Runs against the memory connection in parallel mode so we don't queue
    a doomed op for the disk worker. Leaves no residue on failure.
    """
    try:
        conn.execute("CREATE VIRTUAL TABLE __fts_probe "
                     "USING fts5(x, tokenize=trigram)")
    except sqlite3.OperationalError:
        return False
    try:
        conn.execute("DROP TABLE __fts_probe")
    except sqlite3.OperationalError:
        pass
    return True


def _row_from(m):
    """Build one INSERT row from a Navigation.json entry.

    CHANGED: extracted to module level so a list comprehension can call
    it without per-iteration closure setup, and to skip the workshop_id
    regex on rows that don't have a Steam workshop link at all.
    """
    bsp = m.get('NameBsp') or ''
    label, cls = classify(bsp)
    link = m.get('SteamLink')
    # Fast '?' / '&'-less guard — avoids the regex on rows without a link.
    wid = workshop_id(link) if link and 'id=' in link else None
    return (
        m.get('FilePath'), m.get('Link'), link, m.get('Аuthor'),
        m.get('Title'), m.get('Description'), bsp,
        int(m.get('Size') or 0), int(m.get('TimeCreated') or 0),
        wid, label, cls,
    )


def _build_fresh(writer, build_fts=True):
    t0 = time.time()
    size_mb = os.path.getsize(SOURCE) / 1048576
    _header('Build', f'{SOURCE} · {size_mb:.1f} MB')
    t_prev = t0

    # CHANGED: PRAGMA threads lets SQLite use auxiliary worker threads for
    # the sort phases in CREATE INDEX and the FTS5 rebuild. Set per-conn
    # (both memory and disk get it because _ParallelWriter mirrors it).
    writer.executescript(f'''
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous  = OFF;
        PRAGMA temp_store   = MEMORY;
        PRAGMA cache_size   = -512000;
        PRAGMA page_size    = 16384;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA threads      = {_SQLITE_THREADS};

        CREATE TABLE maps (
            id            INTEGER PRIMARY KEY,
            file_path     TEXT,
            link          TEXT,
            steam_link    TEXT,
            author        TEXT,
            title         TEXT,
            description   TEXT,
            bsp           TEXT,
            size          INTEGER,
            time_created  INTEGER,
            workshop_id   TEXT,
            type_label    TEXT,
            type_cls      TEXT,
            preview_url   TEXT
        );

        CREATE TABLE details_cache (
            workshop_id TEXT PRIMARY KEY,
            preview_url TEXT,
            images      TEXT,
            description TEXT,
            title       TEXT,
            fetched_at  INTEGER
        );

        CREATE TABLE author_cache (
            steam_id     TEXT PRIMARY KEY,
            personaname  TEXT,
            avatarmedium TEXT,
            fetched_at   INTEGER
        );

        CREATE TABLE _build_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    ''')
    t_now = time.time()
    _step('✓', P_OK, 'Schema', '4 tables', t_now - t_prev)
    t_prev = t_now

    raw = _load_json(SOURCE)
    t_now = time.time()
    _step('✓', P_OK, 'Parsed', f'{len(raw):,} entries', t_now - t_prev)
    t_prev = t_now

    # CHANGED: list comprehension over a module-level helper.
    rows = [_row_from(m) for m in raw]
    del raw
    n_rows = len(rows)
    t_now = time.time()
    _step('✓', P_OK, 'Classified', f'{n_rows:,} rows', t_now - t_prev)
    t_prev = t_now

    writer.executemany('''
        INSERT INTO maps (file_path, link, steam_link, author, title,
                          description, bsp, size, time_created,
                          workshop_id, type_label, type_cls)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ''', rows)
    del rows
    t_now = time.time()
    _step('✓', P_OK, 'Inserted', f'{n_rows:,} rows', t_now - t_prev)
    t_prev = t_now

    # CHANGED: idx_type removed — any WHERE type_cls=? (and ORDER BY
    # type_cls) is already served by the leftmost column of idx_type_time,
    # idx_type_size and idx_type_title. Building it was pure overhead.
    writer.executescript('''
        BEGIN;
        CREATE INDEX idx_time      ON maps(time_created);
        CREATE INDEX idx_size      ON maps(size);
        CREATE INDEX idx_title     ON maps(title COLLATE NOCASE);
        CREATE INDEX idx_workshop  ON maps(workshop_id);
        CREATE INDEX idx_author    ON maps(author);
        CREATE INDEX idx_type_time ON maps(type_cls, time_created);
        CREATE INDEX idx_type_size ON maps(type_cls, size);
        CREATE INDEX idx_type_title ON maps(type_cls, title COLLATE NOCASE);
        COMMIT;
    ''')
    t_now = time.time()
    _step('✓', P_OK, 'Indices', '8 indexes', t_now - t_prev)
    t_prev = t_now

    # FTS5 trigram index. Author is deliberately excluded: 17-digit Steam
    # IDs generate ~15 useless trigrams per row, and idx_author already
    # covers exact author lookups. If you need fuzzy author search, add it
    # back here (and accept the rebuild cost).
    if not build_fts:
        _step('·', P_WARN, 'FTS5', 'skipped (--no-fts) — LIKE fallback', None)
    else:
        probe_conn = writer.mem if isinstance(writer, _ParallelWriter) else writer
        if _fts5_available(probe_conn):
            writer.executescript('''
                CREATE VIRTUAL TABLE maps_fts USING fts5(
                    title, bsp,
                    content='maps', content_rowid='id',
                    tokenize="trigram"
                );
            ''')
            # automerge=0 prevents incremental merges during the bulk load;
            # a single 'optimize' at the end is much cheaper than N merges.
            writer.execute(
                "INSERT INTO maps_fts(maps_fts, rank) VALUES('automerge', 0)")
            writer.execute(
                "INSERT INTO maps_fts(maps_fts) VALUES('rebuild')")
            writer.execute(
                "INSERT INTO maps_fts(maps_fts) VALUES('optimize')")
            t_now = time.time()
            _step('✓', P_OK, 'FTS5', 'trigram (title, bsp)', t_now - t_prev)
            t_prev = t_now
        else:
            _step('·', P_WARN, 'FTS5', 'unavailable — LIKE fallback', None)

    writer.execute('PRAGMA optimize')

    writer.execute(
        "INSERT OR REPLACE INTO _build_state (key, value) "
        "VALUES ('maps_complete', '1')"
    )
    writer.commit()
    _divider()
    _step('✓', P_OK, 'Complete', '', time.time() - t0)
    print()


def _configure_conn(conn):
    """Apply the post-build PRAGMAs every live connection needs.

    CHANGED: WAL + synchronous=NORMAL. The prefetch phases commit every
    COMMIT_EVERY batches; with FULL sync those commits fsync, with NORMAL
    they don't (still safe for a resumable build). busy_timeout lets the
    two prefetch phases coexist without spurious SQLITE_BUSY errors.
    """
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA temp_store=MEMORY')


def _ensure_schema(conn):
    cols = {r[1] for r in conn.execute('PRAGMA table_info(maps)').fetchall()}
    if 'preview_url' not in cols:
        conn.execute('ALTER TABLE maps ADD COLUMN preview_url TEXT')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS details_cache (
            workshop_id TEXT PRIMARY KEY,
            preview_url TEXT,
            images      TEXT,
            description TEXT,
            title       TEXT,
            fetched_at  INTEGER
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS author_cache (
            steam_id     TEXT PRIMARY KEY,
            personaname  TEXT,
            avatarmedium TEXT,
            fetched_at   INTEGER
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS _build_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    # Upgrade path for existing DBs: add compound indexes on resume.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_type_size  '
                 'ON maps(type_cls, size)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_type_title '
                 'ON maps(type_cls, title COLLATE NOCASE)')
    conn.commit()


def _prefetch_details(conn, backoff=None):
    # CHANGED: accept a shared backoff so the authors phase can react to
    # 429s seen here (and vice versa). Return value is the number of rows
    # actually inserted so the caller can skip _sync_preview_url when
    # nothing changed (resume runs with a full cache).
    if backoff is None:
        backoff = _Backoff(STEAM_SLEEP)

    missing = [r[0] for r in conn.execute('''
        SELECT DISTINCT m.workshop_id FROM maps m
        LEFT JOIN details_cache d ON d.workshop_id = m.workshop_id
        WHERE m.workshop_id IS NOT NULL
          AND d.workshop_id IS NULL
    ''').fetchall()]

    if not missing:
        n = conn.execute('SELECT COUNT(*) FROM details_cache').fetchone()[0]
        _step('✓', P_OK, 'Details', f'{n:,} entries cached', None)
        return 0

    n_batches = (len(missing) + STEAM_BATCH - 1) // STEAM_BATCH
    _header('Details pre-fetch',
            f'{len(missing):,} missing · {n_batches} batches · '
            f'{DETAILS_WORKERS} workers · base sleep {STEAM_SLEEP:.2f}s')
    print(f'  {_fg(P_DIM, "Ctrl+C is safe — progress is saved periodically.")}')

    now      = int(time.time())
    t0       = time.time()
    saved    = 0
    failed   = 0
    done     = 0
    pending  = 0  # CHANGED: batches since last commit

    def fetch(bi, chunk):
        s = _thread_session()
        q = (
            f'key={STEAM_KEY}'
            '&includevotes=false'
            '&short_description=false'
            '&includeadditionalpreviews=true'
            + ''.join(f'&publishedfileids[{j}]={w}'
                      for j, w in enumerate(chunk))
        )
        url = ('https://api.steampowered.com/'
               'IPublishedFileService/GetDetails/v1/?' + q)
        r = _steam_get(s, url,
                       label=f'details {bi + 1}/{n_batches}',
                       verbose=False, backoff=backoff)
        if r is None:
            return bi, None
        try:
            data = r.json()
        except ValueError:
            return bi, None

        rows = []
        for item in data.get('response', {}).get('publishedfiledetails', []):
            wid = item.get('publishedfileid')
            if not wid:
                continue
            images = [p['url'] for p in (item.get('previews') or [])
                      if p.get('url')]
            if not images and item.get('preview_url'):
                images.append(item['preview_url'])
            images = list(dict.fromkeys(images))
            rows.append((
                wid, item.get('preview_url'), json.dumps(images),
                item.get('description'), item.get('title'), now,
            ))
        return bi, rows

    with ThreadPoolExecutor(max_workers=DETAILS_WORKERS) as pool:
        futures = [
            pool.submit(fetch, bi, missing[bi*STEAM_BATCH:(bi+1)*STEAM_BATCH])
            for bi in range(n_batches)
        ]
        try:
            for fut in as_completed(futures):
                bi, rows = fut.result()
                done += 1
                if rows is None:
                    failed += 1
                elif rows:
                    # Plain INSERT: the query already filters to missing
                    # workshop_ids, so OR REPLACE just adds useless I/O.
                    conn.executemany(
                        '''INSERT INTO details_cache
                           (workshop_id, preview_url, images, description,
                            title, fetched_at)
                           VALUES (?,?,?,?,?,?)''',
                        rows
                    )
                    # CHANGED: batch commits — one per COMMIT_EVERY batches.
                    pending += 1
                    if pending >= COMMIT_EVERY:
                        conn.commit()
                        pending = 0
                    saved += len(rows)
                extra = f'{_fg(P_DIM, "│")} {_fg(P_OK, f"{saved:,}")} saved'
                if failed:
                    extra += f' {_fg(P_DIM, "│")} {_fg(P_ERR, str(failed))} failed'
                _progress('Details', done, n_batches, t0, extra=extra)
        except KeyboardInterrupt:
            _finish_progress()
            print(f'\n  {_fg(P_WARN, "⚠")} Interrupted — in-flight workers '
                  f'will finish.')
            raise

    # Flush any remaining uncommitted batches.
    if pending:
        conn.commit()

    _finish_progress()
    _divider()
    _step('✓', P_OK, 'Details done', f'{saved:,} saved, {failed} failed',
          time.time() - t0)
    return saved


def _prefetch_authors(conn, backoff=None):
    if backoff is None:
        backoff = _Backoff(STEAM_SLEEP)

    # AUTHORS_MIN_MAPS lets the user skip the long tail of one-off authors
    # (default 1 = fetch everyone). The HAVING COUNT(*) is a no-op at 1.
    missing = [r[0] for r in conn.execute('''
        SELECT m.author
        FROM maps m
        LEFT JOIN author_cache a ON a.steam_id = m.author
        WHERE m.author IS NOT NULL
          AND m.author != ''
          AND a.steam_id IS NULL
        GROUP BY m.author
        HAVING COUNT(*) >= ?
    ''', (AUTHORS_MIN_MAPS,)).fetchall()]

    if not missing:
        n = conn.execute('SELECT COUNT(*) FROM author_cache').fetchone()[0]
        _step('✓', P_OK, 'Authors', f'{n:,} profiles cached', None)
        return 0

    n_batches = (len(missing) + 99) // 100
    sub = (f'{len(missing):,} missing · {n_batches} batches · '
           f'{AUTHORS_WORKERS} workers · base sleep {STEAM_SLEEP:.2f}s')
    if AUTHORS_MIN_MAPS > 1:
        sub += f' · min {AUTHORS_MIN_MAPS} maps/author'
    _header('Authors pre-fetch', sub)

    now     = int(time.time())
    t0      = time.time()
    saved   = 0
    failed  = 0
    done    = 0
    pending = 0  # CHANGED
    stop    = threading.Event()

    def fetch(bi, chunk):
        # Bail immediately if we've been interrupted — don't even sleep.
        if stop.is_set():
            return bi, None
        s = _thread_session()
        url = ('https://api.steampowered.com/'
               'ISteamUser/GetPlayerSummaries/v2/?' +
               urlencode({'key': STEAM_KEY,
                          'steamids': ','.join(chunk)}))
        r = _steam_get(s, url,
                       label=f'authors {bi + 1}/{n_batches}',
                       verbose=False, backoff=backoff)
        if stop.is_set() or r is None:
            return bi, None
        try:
            data = r.json()
        except ValueError:
            return bi, None

        rows = []
        returned = set()
        for p in data.get('response', {}).get('players', []):
            sid = p.get('steamid')
            if sid:
                returned.add(sid)
                rows.append((sid, p.get('personaname'),
                             p.get('avatarmedium'), now))
        # GetPlayerSummaries silently omits private / deleted / renamed
        # IDs. Cache those as NULL rows so subsequent runs don't retry
        # them — this is what turns "every run re-fetches thousands of
        # dead accounts" into a bounded, one-time cost.
        for sid in chunk:
            if sid not in returned:
                rows.append((sid, None, None, now))
        return bi, rows

    # Explicit shutdown so we can pass cancel_futures on interrupt.
    pool    = ThreadPoolExecutor(max_workers=AUTHORS_WORKERS)
    futures = [
        pool.submit(fetch, bi, missing[bi*100:(bi+1)*100])
        for bi in range(n_batches)
    ]
    try:
        for fut in as_completed(futures):
            bi, rows = fut.result()
            done += 1
            if rows is None:
                failed += 1
            elif rows:
                conn.executemany(
                    '''INSERT INTO author_cache
                       (steam_id, personaname, avatarmedium, fetched_at)
                       VALUES (?,?,?,?)''',
                    rows
                )
                # CHANGED: batch commits.
                pending += 1
                if pending >= COMMIT_EVERY:
                    conn.commit()
                    pending = 0
                saved += len(rows)
            extra = f'{_fg(P_DIM, "│")} {_fg(P_OK, f"{saved:,}")} saved'
            if failed:
                extra += f' {_fg(P_DIM, "│")} {_fg(P_ERR, str(failed))} failed'
            _progress('Authors', done, n_batches, t0, extra=extra)
    except KeyboardInterrupt:
        _finish_progress()
        print(f'\n  {_fg(P_WARN, "⚠")} Interrupted — cancelling pending '
              f'batches.')
        stop.set()
        # cancel_futures kills the ~150 queued-but-not-started batches;
        # wait=False skips the join loop so a second Ctrl+C isn't needed.
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    except Exception:
        stop.set()
        pool.shutdown(wait=False, cancel_futures=True)
        raise

    pool.shutdown(wait=True)

    if pending:
        conn.commit()

    _finish_progress()
    _divider()
    _step('✓', P_OK, 'Authors done', f'{saved:,} saved, {failed} failed',
          time.time() - t0)
    return saved


def _run_prefetch(conn_main):
    """Run details + authors pre-fetch phases sequentially.

    Concurrent prefetch is slower on this dataset: Details has ~5x the
    batches of Authors, so splitting the concurrency budget evenly
    throttles the phase that actually needs it. Sequential gives all
    workers to Details, then all workers to Authors.
    """
    shared_backoff = _Backoff(STEAM_SLEEP)
    _prefetch_details(conn_main, shared_backoff)
    _prefetch_authors(conn_main, shared_backoff)


def _preview_sync_needed(conn):
    """CHANGED: cheap EXISTS check so resume runs with a full cache can
    skip the full UPDATE FROM (which would otherwise scan the join even
    though nothing changes)."""
    row = conn.execute('''
        SELECT 1
        FROM maps m
        JOIN details_cache dc ON dc.workshop_id = m.workshop_id
        WHERE dc.preview_url IS NOT NULL
          AND (m.preview_url IS NULL OR m.preview_url != dc.preview_url)
        LIMIT 1
    ''').fetchone()
    return row is not None


def _sync_preview_url(conn):
    if not _preview_sync_needed(conn):
        _step('✓', P_OK, 'Preview URLs', 'already up to date', None)
        return
    print(f'  {_fg(P_DIM, "·")} Syncing preview URLs into maps...')
    # UPDATE FROM only touches rows that actually have a cache entry and
    # whose preview_url differs from the cache — a fraction of the table
    # on resume runs.
    conn.execute('''
        UPDATE maps
        SET preview_url = dc.preview_url
        FROM details_cache dc
        WHERE dc.workshop_id = maps.workshop_id
          AND maps.preview_url IS NOT dc.preview_url
    ''')
    conn.commit()
    n = conn.execute(
        'SELECT COUNT(*) FROM maps WHERE preview_url IS NOT NULL'
    ).fetchone()[0]
    _step('✓', P_OK, 'Preview URLs', f'{n:,} maps', None)


def _write_types(conn):
    type_rows = conn.execute('''
        SELECT type_cls, type_label, COUNT(*) AS c
        FROM maps GROUP BY type_cls ORDER BY c DESC
    ''').fetchall()
    total = conn.execute('SELECT COUNT(*) FROM maps').fetchone()[0]
    with open(TYPES_OUT, 'w', encoding='utf-8') as f:
        json.dump({
            'total': total,
            'types': [{'cls': r[0], 'label': r[1], 'count': r[2]}
                      for r in type_rows],
        }, f, ensure_ascii=False)
    _step('✓', P_OK, TYPES_OUT, f'{total:,} maps · {len(type_rows)} types',
          None)


def _write_meta(conn):
    total        = conn.execute('SELECT COUNT(*) FROM maps').fetchone()[0]
    with_preview = conn.execute(
        'SELECT COUNT(*) FROM maps WHERE preview_url IS NOT NULL'
    ).fetchone()[0]
    with_desc    = conn.execute(
        'SELECT COUNT(*) FROM details_cache WHERE description IS NOT NULL'
    ).fetchone()[0]
    with_authors = conn.execute('SELECT COUNT(*) FROM author_cache').fetchone()[0]

    with open(META_OUT, 'w', encoding='utf-8') as f:
        json.dump({
            'built_at':               int(time.time()),
            'total_maps':             total,
            'with_preview':           with_preview,
            'with_steam_description': with_desc,
            'authors_cached':         with_authors,
            'db_bytes':               os.path.getsize(DB_PATH),
        }, f, indent=2, ensure_ascii=False)
    _step('✓', P_OK, META_OUT, '', None)


def _convert_to_readonly(conn):
    try:
        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        conn.execute('PRAGMA journal_mode=DELETE')
        conn.commit()
    except sqlite3.OperationalError as e:
        _step('!', P_WARN, 'Journal mode', str(e), None)
        return
    _step('✓', P_OK, 'Journal mode', 'DELETE (read-only friendly)', None)


def _print_summary(conn, elapsed_total):
    total        = conn.execute('SELECT COUNT(*) FROM maps').fetchone()[0]
    with_preview = conn.execute(
        'SELECT COUNT(*) FROM maps WHERE preview_url IS NOT NULL'
    ).fetchone()[0]
    with_desc    = conn.execute(
        'SELECT COUNT(*) FROM details_cache WHERE description IS NOT NULL'
    ).fetchone()[0]
    with_authors = conn.execute('SELECT COUNT(*) FROM author_cache').fetchone()[0]
    db_bytes     = os.path.getsize(DB_PATH)

    rows = [
        ('Maps',         f'{total:,}'),
        ('With preview', f'{with_preview:,}   {_fg(P_DIM, f"({_pct(with_preview, total)})")}'),
        ('Descriptions', f'{with_desc:,}   {_fg(P_DIM, f"({_pct(with_desc, total)})")}'),
        ('Authors',      f'{with_authors:,}'),
        ('Database',     _fmt_bytes(db_bytes)),
        ('Elapsed',      _fmt_dur(elapsed_total)),
    ]
    print()
    _summary_box('BUILD COMPLETE', '✓', P_OK, rows)


def main():
    t_start = time.time()

    parser = argparse.ArgumentParser(description='Build or resume maps.db.')
    parser.add_argument('--rebuild', action='store_true',
                        help='force full rebuild, discarding existing DB')
    parser.add_argument('--no-memory', action='store_true',
                        help='skip RAM check and build directly on disk')
    parser.add_argument('--parallel', action='store_true',
                        help='force parallel RAM+disk build even on large '
                             'sources (overrides PARALLEL_MAX_MB)')
    parser.add_argument('--no-parallel', action='store_true',
                        help='disable parallel RAM+disk build (build in RAM, '
                             'then copy to disk in one backup at the end)')
    # CHANGED: --no-fts skips the FTS5 index entirely.
    parser.add_argument('--no-fts', action='store_true',
                        default=SKIP_FTS_ENV,
                        help='skip the FTS5 index; search falls back to LIKE '
                             '(also via SKIP_FTS=1)')
    args = parser.parse_args()

    if args.parallel and args.no_parallel:
        parser.error('--parallel and --no-parallel are mutually exclusive')

    # parallel = None → auto by size; True/False → forced
    parallel = None
    if args.parallel:
        parallel = True
    elif args.no_parallel:
        parallel = False

    if args.rebuild:
        print(f'  {_fg(P_WARN, "!")} --rebuild: removing existing DB.')
        _wipe_db()

    fresh = args.rebuild or not _build_complete()

    in_memory = False
    if fresh:
        if os.path.exists(DB_PATH):
            print(f'  {_fg(P_WARN, "!")} Previous build incomplete — '
                  f'wiping and rebuilding.')
            _wipe_db()
        if not os.path.exists(SOURCE):
            sys.exit(f'{SOURCE} not found.')

        if args.no_memory:
            print(f'  {_fg(P_DIM, "·")} --no-memory: building on disk.')
        else:
            in_memory = _should_build_in_memory(os.path.getsize(SOURCE))
            if in_memory:
                if parallel is None:
                    size_mb = os.path.getsize(SOURCE) / 1048576
                    parallel = size_mb <= PARALLEL_MAX_MB
                    if not parallel:
                        print(f'  {_fg(P_DIM, "·")} Source is {size_mb:.0f} MB '
                              f'(> {PARALLEL_MAX_MB:.0f} MB) — parallel mode '
                              f'auto-disabled.')
                if parallel:
                    print(f'  {_fg(P_DIM, "·")} Building in RAM and on disk '
                          f'in parallel.')
                else:
                    print(f'  {_fg(P_DIM, "·")} Building in RAM; will write '
                          f'to disk when done.')

    build_fts = not args.no_fts

    if fresh and in_memory and parallel:
        # -------- parallel dual-write path --------
        _wipe_db()
        mem_conn = sqlite3.connect(':memory:')
        writer = _ParallelWriter(mem_conn, DB_PATH)
        writer.start()
        try:
            _build_fresh(writer, build_fts=build_fts)
        finally:
            try:
                writer.finish()
            except Exception as e:
                print(f'  {_fg(P_ERR, "✗")} Disk writer error: {e}')
                raise
            mem_conn.close()
        conn = sqlite3.connect(DB_PATH)
        _configure_conn(conn)   # CHANGED: WAL + synchronous=NORMAL + busy_timeout

    elif fresh and in_memory:
        # -------- sequential path: RAM then backup --------
        conn = sqlite3.connect(':memory:')
        _build_fresh(conn, build_fts=build_fts)
        print(f'  {_fg(P_DIM, "·")} Writing to disk...')
        t = time.time()
        _save_memory_to_disk(conn)
        print(f'  {_fg(P_OK, "✓")} Wrote '
              f'{os.path.getsize(DB_PATH) / 1048576:.1f} MB in '
              f'{time.time() - t:.1f}s.\n')
        conn.close()
        conn = sqlite3.connect(DB_PATH)
        _configure_conn(conn)   # CHANGED

    elif fresh:
        # -------- on-disk build (--no-memory or low RAM) --------
        conn = sqlite3.connect(DB_PATH)
        _build_fresh(conn, build_fts=build_fts)
        _configure_conn(conn)   # CHANGED: upgrade the same connection

    else:
        if not os.path.exists(DB_PATH):
            sys.exit('_build_complete said True but DB is missing?!')
        conn = sqlite3.connect(DB_PATH)
        n = conn.execute('SELECT COUNT(*) FROM maps').fetchone()[0]
        print(f'  {_fg(P_OK, "✓")} Resuming — {n:,} maps already in DB.\n')
        _configure_conn(conn)   # CHANGED
        _ensure_schema(conn)

    try:
        if STEAM_KEY and requests:
            _run_prefetch(conn)
        else:
            _step('·', P_WARN, 'Pre-fetch',
                  'skipped — STEAM_API_KEY not set', None)
            print(f'    {_fg(P_DIM, "Cards will use gradient fallbacks; run "
                               "again with the key to add previews.")}')

        _sync_preview_url(conn)
        _write_types(conn)
        _write_meta(conn)
        _convert_to_readonly(conn)

        _print_summary(conn, time.time() - t_start)

    except KeyboardInterrupt:
        print(f'\n\n  {_fg(P_WARN, "⚠")} Interrupted. Progress saved — '
              f're-run to resume.')
    finally:
        try:
            conn.commit()
        except Exception:
            pass
        conn.close()


if __name__ == '__main__':
    main()