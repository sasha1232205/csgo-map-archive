"""
Flask app for the CS:GO map archive.
Run dev:   python app.py
Run prod:  gunicorn -c gunicorn.conf.py app:app
"""
import csv
import hashlib
import io
import json
import os
import sqlite3
import threading
from collections import OrderedDict

from flask import Flask, abort, g, jsonify, render_template, request
from flask_compress import Compress
from flask.json.provider import DefaultJSONProvider

# orjson: 3-4x faster than stdlib json for this payload shape.
# Falls back transparently if the wheel isn't installed on the target box.
try:
    import orjson
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, 'maps.db')
TYPES_PATH = os.path.join(BASE_DIR, 'types.json')
META_PATH  = os.path.join(BASE_DIR, 'meta.json')

ITEMS_PER_PAGE   = int(os.environ.get('MAPS_PER_PAGE', 48))
COUNT_CACHE_MAX  = int(os.environ.get('COUNT_CACHE_MAX', 2048))
HYDRATE_MAX      = int(os.environ.get('HYDRATE_ITEMS_MAX', 200))

IS_DEV = (os.environ.get('FLASK_ENV') == 'development'
          or os.environ.get('MAPS_DEV') == '1')

app = Flask(__name__)
Compress(app)
app.config.update(
    COMPRESS_ALGORITHM = ['br', 'gzip'],
    COMPRESS_BR_LEVEL  = 6,
    COMPRESS_MIMETYPES = [
        'text/html', 'text/css', 'text/javascript',
        'application/javascript', 'application/json',
        'image/svg+xml',
    ],
)


# ---------------------------------------------------------------------------
#  Fast JSON provider (orjson-backed, falls back to stdlib)
# ---------------------------------------------------------------------------
if _HAS_ORJSON:
    class _ORJSONProvider(DefaultJSONProvider):
        """jsonify() and Jinja's |tojson both route through here."""
        default      = None
        ensure_ascii = False
        sort_keys    = False

        def dumps(self, obj, **kwargs):
            return orjson.dumps(obj).decode('utf-8')

        def loads(self, s, **kwargs):
            return orjson.loads(s)

    app.json = _ORJSONProvider(app)


# ---------------------------------------------------------------------------
#  Build metadata / assets
# ---------------------------------------------------------------------------
def _read_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


TYPES_PAYLOAD = _read_json(TYPES_PATH, {'total': 0, 'types': []})
META          = _read_json(META_PATH, {})
DB_EXISTS     = os.path.exists(DB_PATH)
DB_MTIME      = int(os.path.getmtime(DB_PATH)) if DB_EXISTS else 0
DATA_VERSION  = str(DB_MTIME)

if not DB_EXISTS:
    app.logger.error('maps.db missing at %s — run `python ingest.py`', DB_PATH)


def _asset_version(filename):
    try:
        with open(os.path.join(BASE_DIR, 'static', filename), 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()[:8]
    except FileNotFoundError:
        return '0'


ASSET_VERSIONS = {
    'app.js':    _asset_version('app.js'),
    'style.css': _asset_version('style.css'),
    'logo.svg':  _asset_version('logo.svg'),
}


# ---------------------------------------------------------------------------
#  ETag helper — collapses repeat API calls to 0-byte 304s
# ---------------------------------------------------------------------------
def _etag(*parts):
    h = hashlib.md5()
    for p in parts:
        h.update(p if isinstance(p, bytes) else str(p).encode('utf-8'))
        h.update(b'\x00')
    return h.hexdigest()


# ---------------------------------------------------------------------------
#  DB
# ---------------------------------------------------------------------------
def get_db():
    if 'db' not in g:
        if not DB_EXISTS:
            abort(503, 'maps.db not built — run ingest.py')
        uri = f'file:{DB_PATH}?mode=ro&immutable=1'
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA query_only  = ON')
        conn.execute('PRAGMA temp_store  = MEMORY')
        conn.execute('PRAGMA cache_size  = -32000')     # ~32 MB
        conn.execute('PRAGMA mmap_size   = 268435456')  # 256 MB
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop('db', None)
    if db:
        db.close()


# ---------------------------------------------------------------------------
#  Headers
# ---------------------------------------------------------------------------
@app.after_request
def add_cache_headers(resp):
    p = request.path

    if p.startswith('/static/'):
        resp.cache_control.public     = True
        resp.cache_control.max_age    = 31536000
        resp.cache_control.immutable  = True
    elif p == '/api/types':
        resp.cache_control.public     = True
        resp.cache_control.max_age    = 86400
        resp.cache_control.immutable  = True
    elif p.startswith('/api/'):
        # Archive is immutable — everything cacheable.
        resp.cache_control.public                 = True
        resp.cache_control.max_age                = 86400
        resp.cache_control.stale_while_revalidate = 604800
        resp.headers['Vary'] = 'Accept-Encoding'
    return resp


# ---------------------------------------------------------------------------
#  Serialization
# ---------------------------------------------------------------------------
def row_to_map(row, with_description=False):
    keys = row.keys()
    d = {
        'id':          row['id'],
        'FilePath':    row['file_path'],
        'Link':        row['link'],
        'SteamLink':   row['steam_link'],
        'Аuthor':      row['author'],
        'Title':       row['title'],
        'NameBsp':     row['bsp'],
        'Size':        row['size'],
        'TimeCreated': row['time_created'],
        'WorkshopId':  row['workshop_id'],
        'Type':        {'label': row['type_label'], 'cls': row['type_cls']},
        'PreviewUrl':  row['preview_url'] if 'preview_url' in keys else None,
    }
    if 'author_name' in keys:
        d['AuthorName']   = row['author_name']
        d['AuthorAvatar'] = row['author_avatar']
    if with_description:
        d['Description'] = row['description']
    return d


# ---------------------------------------------------------------------------
#  Count cache
# ---------------------------------------------------------------------------
_count_cache = OrderedDict()
_count_lock  = threading.Lock()


def _fts_available(db):
    return db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='maps_fts'"
    ).fetchone() is not None


def _cached_count(db, where_sql, params):
    key = (where_sql, tuple(params))
    with _count_lock:
        v = _count_cache.get(key)
        if v is not None:
            _count_cache.move_to_end(key)
            return v
    n = db.execute(f'SELECT COUNT(*) FROM maps m {where_sql}', params).fetchone()[0]
    with _count_lock:
        _count_cache[key] = n
        _count_cache.move_to_end(key)
        while len(_count_cache) > COUNT_CACHE_MAX:
            _count_cache.popitem(last=False)
    return n


# ---------------------------------------------------------------------------
#  Query builder
# ---------------------------------------------------------------------------
SORT_SQL = {
    'newest':    'ORDER BY m.time_created DESC, m.id DESC',
    'oldest':    'ORDER BY m.time_created ASC,  m.id ASC',
    'title-asc': 'ORDER BY m.title COLLATE NOCASE ASC',
    'size-desc': 'ORDER BY m.size DESC',
    'size-asc':  'ORDER BY m.size ASC',
}


def build_query(args):
    type_filter = args.get('type', 'all')
    q           = (args.get('q') or '').strip()
    sort        = args.get('sort', 'default')

    where, params = [], []
    if type_filter and type_filter != 'all':
        where.append('m.type_cls = ?'); params.append(type_filter)

    if q:
        db = get_db()
        if len(q) >= 3 and _fts_available(db):
            safe = q.replace('"', '""')
            where.append('m.id IN (SELECT rowid FROM maps_fts WHERE maps_fts MATCH ?)')
            params.append(f'"{safe}"')
        else:
            like = f'%{q}%'
            where.append('(m.title LIKE ? COLLATE NOCASE '
                         'OR m.bsp LIKE ? COLLATE NOCASE '
                         'OR m.author LIKE ? COLLATE NOCASE)')
            params.extend([like, like, like])

    for name, op in (('min_size', '>='), ('max_size', '<='),
                     ('from', '>='),     ('to', '<=')):
        raw = args.get(name)
        if not raw:
            continue
        try:
            v = int(raw)
        except (ValueError, TypeError):
            continue
        col = 'size' if 'size' in name else 'time_created'
        where.append(f'm.{col} {op} ?'); params.append(v)

    if args.get('has_workshop') == '1':
        where.append('m.workshop_id IS NOT NULL')

    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    order_sql = SORT_SQL.get(sort, 'ORDER BY m.id ASC')
    return where_sql, params, order_sql


def query_page(args):
    db = get_db()
    try:
        page     = max(1, int(args.get('page', 1)))
        per_page = min(200, max(1, int(args.get('per_page', ITEMS_PER_PAGE))))
    except (ValueError, TypeError):
        abort(400, 'bad page')

    where_sql, params, order_sql = build_query(args)
    total  = _cached_count(db, where_sql, params)
    offset = (page - 1) * per_page

    rows = db.execute(
        f'''SELECT m.id, m.file_path, m.link, m.steam_link, m.author, m.title,
                   m.bsp, m.size, m.time_created, m.workshop_id,
                   m.type_label, m.type_cls, m.preview_url,
                   a.personaname AS author_name, a.avatarmedium AS author_avatar
            FROM maps m
            LEFT JOIN author_cache a ON a.steam_id = m.author
            {where_sql} {order_sql}
            LIMIT ? OFFSET ?''',
        params + [per_page, offset]
    ).fetchall()

    return {
        'maps':         [row_to_map(r) for r in rows],
        'total':        total,
        'page':         page,
        'per_page':     per_page,
        'total_pages':  max(1, (total + per_page - 1) // per_page),
        'data_version': DATA_VERSION,
    }


# ---------------------------------------------------------------------------
#  Routes
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    initial = {
        'types':  TYPES_PAYLOAD,
        'page':   query_page({'page': 1}),
        'meta':   META,
    }
    return render_template('index.html', initial=initial, versions=ASSET_VERSIONS)


@app.route('/api/types')
def api_types():
    return jsonify(TYPES_PAYLOAD)


@app.route('/api/maps')
def api_maps():
    resp = jsonify(query_page(request.args))
    resp.set_etag(_etag('maps', DATA_VERSION, request.query_string))
    return resp.make_conditional(request)


@app.route('/api/map/<int:map_id>')
def api_map(map_id):
    db  = get_db()
    row = db.execute(
        '''SELECT m.id, m.file_path, m.link, m.steam_link, m.author, m.title,
                  m.description, m.bsp, m.size, m.time_created, m.workshop_id,
                  m.type_label, m.type_cls, m.preview_url,
                  a.personaname AS author_name, a.avatarmedium AS author_avatar,
                  d.images, d.description AS steam_description
           FROM maps m
           LEFT JOIN author_cache  a ON a.steam_id     = m.author
           LEFT JOIN details_cache d ON d.workshop_id  = m.workshop_id
           WHERE m.id = ?''', (map_id,)
    ).fetchone()
    if not row:
        abort(404)

    d = row_to_map(row, with_description=True)
    d['Description'] = d['Description'] or row['steam_description'] or ''

    images = json.loads(row['images'] or '[]') if row['images'] else []
    if not images and row['preview_url']:
        images = [row['preview_url']]
    d['Images'] = images

    resp = jsonify(d)
    resp.set_etag(_etag('map', DATA_VERSION, map_id))
    return resp.make_conditional(request)


@app.route('/api/hydrate')
def api_hydrate():
    """Kept for forward-compat: batch-fetch details + authors from DB only."""
    wids    = [x.strip() for x in request.args.get('ids', '').split(',')
               if x.strip().isdigit()][:HYDRATE_MAX]
    authors = [x.strip() for x in request.args.get('authors', '').split(',')
               if x.strip().isdigit()][:HYDRATE_MAX]
    db = get_db()
    out_d, out_a = {}, {}

    if wids:
        ph = ','.join('?' * len(wids))
        for r in db.execute(
            f'SELECT workshop_id, preview_url, images, description, title '
            f'FROM details_cache WHERE workshop_id IN ({ph})', wids
        ):
            out_d[r['workshop_id']] = {
                'preview_url': r['preview_url'],
                'images':      json.loads(r['images'] or '[]'),
                'description': r['description'],
                'title':       r['title'],
            }

    if authors:
        ph = ','.join('?' * len(authors))
        for r in db.execute(
            f'SELECT steam_id, personaname, avatarmedium '
            f'FROM author_cache WHERE steam_id IN ({ph})', authors
        ):
            out_a[r['steam_id']] = {
                'personaname':  r['personaname'],
                'avatarmedium': r['avatarmedium'],
            }

    return jsonify({'details': out_d, 'authors': out_a})


@app.route('/api/export.csv')
def api_export_csv():
    where_sql, params, order_sql = build_query(request.args)
    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(['title', 'bsp', 'author', 'size', 'time_created',
                'workshop_id', 'link', 'steam_link'])
    for r in get_db().execute(
        f'SELECT m.title, m.bsp, m.author, m.size, m.time_created, '
        f'm.workshop_id, m.link, m.steam_link FROM maps m {where_sql} {order_sql}',
        params,
    ):
        w.writerow([r['title'], r['bsp'], r['author'], r['size'],
                    r['time_created'], r['workshop_id'], r['link'], r['steam_link']])
    return app.response_class(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=maps.csv'},
    )


@app.route('/_health')
def health():
    return jsonify({
        'ok':           DB_EXISTS,
        'data_version': DATA_VERSION,
        'meta':         META,
    })


if __name__ == '__main__':
    if not DB_EXISTS:
        raise SystemExit('maps.db missing — run `python ingest.py` first.')
    app.run(host='127.0.0.1', port=5000, debug=IS_DEV, threaded=True)