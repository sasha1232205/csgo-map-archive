/* global window, document, fetch, URLSearchParams, localStorage */
'use strict';

const ITEMS_PER_PAGE = 48;
const MAX_STAGGER    = 20;
const YANDEX_BASE    = 'https://disk.yandex.ru/d/EKAI0qKTkQkXtw/';

// ---------------------------------------------------------------------------
//  Display-name overrides by Steam ID
//  Applied last, so it wins over both the cached personaname from the DB
//  and the raw-ID fallback. Survives `python ingest.py --rebuild`.
// ---------------------------------------------------------------------------
const AUTHOR_OVERRIDES = {
    '76561198082857351': 'CSGO Official',
};

function authorDisplay(id, cachedName) {
    return AUTHOR_OVERRIDES[id] || cachedName || id;
}

// ---------------------------------------------------------------------------
//  State
// ---------------------------------------------------------------------------
let currentPage  = 1;
let totalPages   = 1;
let searchTerm   = '';
let typeFilter   = 'all';
let sortBy       = 'default';
let typesLoaded  = false;

const detailCache = {};      // map.id -> full detail JSON (session cache)
const authorCache = {};      // steamid -> { personaname, avatarmedium }

let galleryImages = [];
let galleryIndex  = 0;
let lastFocusedElement = null;

let inflightMaps  = null;
let inflightModal = null;

// Idle prefetch bookkeeping — bounded by dataset size in practice.
const _prefetchedPages  = new Set();
const _prefetchedDetail = new Set();

// ---------------------------------------------------------------------------
//  DOM
// ---------------------------------------------------------------------------
const $ = (id) => document.getElementById(id);

const loader         = $('loader');
const loadStatus     = $('loadStatus');
const mapContainer   = $('mapContainer');
const searchInput    = $('searchInput');
const searchClear    = $('searchClear');
const sortSelect     = $('sortSelect');
const filterChips    = $('filterChips');
const pagination     = $('pagination');
const firstBtn       = $('firstBtn');
const prevBtn        = $('prevBtn');
const nextBtn        = $('nextBtn');
const lastBtn        = $('lastBtn');
const pageInput      = $('pageInput');
const totalPagesSpan = $('totalPages');
const resultCountSpan= $('resultCount');
const siteHeader     = document.querySelector('.site-header');
const topProgress    = $('topProgress');
const cardTpl        = $('cardTpl');

const modal            = $('mapModal');
const modalClose       = $('modalClose');
const modalBadge       = $('modalBadge');
const modalTitle       = $('modalTitle');
const modalAuthor      = $('modalAuthor');
const modalSize        = $('modalSize');
const modalDate        = $('modalDate');
const modalDescription = $('modalDescription');
const modalSteam       = $('modalSteam');
const modalYandex      = $('modalYandex');
const modalDownload    = $('modalDownload');
const galleryTrack     = $('galleryTrack');
const galleryDots      = $('galleryDots');
const galleryPrev      = $('galleryPrev');
const galleryNext      = $('galleryNext');

// ---------------------------------------------------------------------------
//  Icons & Helpers
// ---------------------------------------------------------------------------
const ICON_STEAM = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>';
const ICON_DOWNLOAD = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>';
const ICON_YANDEX = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M13.32 7.666h-.924c-1.694 0-2.585.858-2.585 2.123 0 1.43.616 2.1 1.881 2.959l1.045.704-3.003 4.487H7.49l2.695-4.014c-1.55-1.111-2.42-2.19-2.42-4.015 0-2.288 1.595-3.85 4.62-3.85h3.003v11.868H13.32V7.666z" fill="currentColor"/></svg>';

function formatSize(bytes) {
    if (!bytes) return '0 B';
    const kb = bytes / 1024;
    return kb < 1024 ? kb.toFixed(0) + ' KB' : (kb / 1024).toFixed(2) + ' MB';
}
function formatDate(ts) {
    if (!ts) return 'unknown';
    return new Date(ts * 1000).toLocaleDateString(undefined, {
        year: 'numeric', month: 'short', day: 'numeric'
    });
}
function safeUrl(url) {
    return String(url ?? '').replace(/["\\\r\n]/g, '');
}
function gradientFor(text) {
    let h = 0;
    const s = String(text || '?');
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
    h = Math.abs(h) % 360;
    return `linear-gradient(135deg, hsl(${h}, 22%, 20%), hsl(${(h + 40) % 360}, 22%, 12%))`;
}

function showTopProgress() { topProgress?.classList.add('active'); }
function hideTopProgress() { topProgress?.classList.remove('active'); }

async function fetchJson(url, opts) {
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
}

// ---------------------------------------------------------------------------
//  URL hash sync
// ---------------------------------------------------------------------------
function readHash() {
    const p = new URLSearchParams(location.hash.slice(1));
    return {
        page: Math.max(1, parseInt(p.get('page') || '1', 10) || 1),
        q:    p.get('q') || '',
        type: p.get('type') || 'all',
        sort: p.get('sort') || 'default',
    };
}
function writeHash() {
    const p = new URLSearchParams();
    if (currentPage > 1)        p.set('page', currentPage);
    if (searchTerm)             p.set('q', searchTerm);
    if (typeFilter !== 'all')   p.set('type', typeFilter);
    if (sortBy !== 'default')   p.set('sort', sortBy);
    const next = p.toString();
    if (location.hash.slice(1) === next) return;
    history.replaceState(null, '', next
        ? '#' + next
        : location.pathname + location.search);
}
function syncDOMToState() {
    searchInput.value = searchTerm;
    searchClear.style.display = searchTerm ? 'block' : 'none';
    sortSelect.value = sortBy;
    filterChips.querySelectorAll('.chip').forEach(c =>
        c.classList.toggle('active', c.dataset.type === typeFilter));
}
function stateEqualsHash() {
    const h = readHash();
    return h.page === currentPage && h.q === searchTerm &&
           h.type === typeFilter && h.sort === sortBy;
}

// ---------------------------------------------------------------------------
//  Types / chips
// ---------------------------------------------------------------------------
function renderChips(typesPayload) {
    const chips = [
        { cls: 'all', label: 'All', count: typesPayload.total },
        ...typesPayload.types.map(t => ({ cls: t.cls, label: t.label, count: t.count })),
    ];
    filterChips.innerHTML = '';
    for (const c of chips) {
        const btn = document.createElement('button');
        btn.className = 'chip' + (c.cls === typeFilter ? ' active' : '');
        btn.dataset.type = c.cls;
        btn.appendChild(document.createTextNode(c.label));
        const span = document.createElement('span');
        span.className = 'chip-count';
        span.textContent = c.count.toLocaleString();
        btn.appendChild(span);
        filterChips.appendChild(btn);
    }
    typesLoaded = true;
}

async function loadTypes() {
    if (typesLoaded) return;
    const data = await fetchJson('/api/types');
    renderChips(data);
}

// ---------------------------------------------------------------------------
//  Prefetch — next page (idle) + detail (on hover)
// ---------------------------------------------------------------------------
function prefetchNextPage() {
    if (currentPage >= totalPages) return;
    const params = new URLSearchParams({
        page:     String(currentPage + 1),
        per_page: String(ITEMS_PER_PAGE),
        type:     typeFilter,
        q:        searchTerm,
        sort:     sortBy,
    });
    const url = '/api/maps?' + params.toString();
    if (_prefetchedPages.has(url)) return;
    _prefetchedPages.add(url);
    // <link rel=prefetch> warms the HTTP cache at low priority so the
    // next loadPage() is a synchronous cache hit for the JSON body.
    const link = document.createElement('link');
    link.rel  = 'prefetch';
    link.href = url;
    document.head.appendChild(link);
}

function scheduleNextPagePrefetch() {
    if ('requestIdleCallback' in window) {
        requestIdleCallback(prefetchNextPage, { timeout: 3000 });
    } else {
        setTimeout(prefetchNextPage, 800);
    }
}

function prefetchDetail(id) {
    if (!id || detailCache[id] || _prefetchedDetail.has(id)) return;
    _prefetchedDetail.add(id);
    fetch('/api/map/' + id)
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (d) detailCache[id] = d; })
        .catch(() => {});
}

// ---------------------------------------------------------------------------
//  Rendering
// ---------------------------------------------------------------------------
function buildCard(map, index) {
    const type     = map.Type || { cls: 'other', label: 'Other' };
    const authorId = map['Аuthor'] || '';

    const card = cardTpl.content.firstElementChild.cloneNode(true);
    card.dataset.type = type.cls;
    card._mapData = map;

    const preview = card.querySelector('.preview');
    const badge   = card.querySelector('.badge');
    const img     = card.querySelector('.map-preview');

    badge.classList.add('badge-' + type.cls);
    badge.textContent = type.label;

    if (map.PreviewUrl) {
        img.src = map.PreviewUrl;
        // The first 4 thumbs are above the fold on every viewport we
        // care about — flag them so LCP isn't deprioritised behind
        // lazy placeholders.
        if (index < 4) {
            img.setAttribute('fetchpriority', 'high');
            img.loading = 'eager';
        }
        img.addEventListener('load', () => {
            img.classList.add('loaded');
            preview.classList.add('has-image');
        }, { once: true });
        img.addEventListener('error', () => {
            img.remove();
            preview.classList.add('no-preview');
            preview.style.backgroundImage = gradientFor(map.Title || map.NameBsp);
        }, { once: true });
    } else {
        img.remove();
        preview.classList.add('no-preview');
        preview.style.backgroundImage = gradientFor(map.Title || map.NameBsp);
    }

    card.querySelector('.card-title').textContent = map.Title || 'Untitled';
    card.querySelector('.card-title').title = map.Title || '';
    card.querySelector('.size').textContent = formatSize(map.Size);
    card.querySelector('.card-date').textContent = 'Added ' + formatDate(map.TimeCreated);

    const authorWrap = card.querySelector('.author');
    const authorName = card.querySelector('.author-name');
    const authorAv   = card.querySelector('.author-avatar');
    if (authorId) {
        authorWrap.dataset.authorId = authorId;
        authorName.textContent = authorDisplay(authorId, map.AuthorName);
        if (map.AuthorAvatar) {
            authorAv.style.backgroundImage = `url("${safeUrl(map.AuthorAvatar)}")`;
            authorAv.classList.add('has-avatar');
        }
        authorCache[authorId] = {
            personaname: authorDisplay(authorId, map.AuthorName),
            avatarmedium: map.AuthorAvatar || null,
        };
    } else {
        authorName.textContent = 'Unknown';
    }

    const btnSteam  = card.querySelector('.btn-steam');
    const btnYandex = card.querySelector('.btn-yandex');
    const btnDl     = card.querySelector('.btn-direct');

    if (map.SteamLink) {
        btnSteam.href = map.SteamLink;
        btnSteam.innerHTML = ICON_STEAM + ' Steam';
    } else {
        btnSteam.remove();
    }

    if (map.FilePath) {
        btnYandex.href = YANDEX_BASE + map.FilePath;
        btnYandex.innerHTML = ICON_YANDEX + ' Mirror';
    } else {
        btnYandex.remove();
    }

    if (map.Link) {
        btnDl.href = map.Link;
        btnDl.innerHTML = ICON_DOWNLOAD + ' Direct';
    } else {
        btnDl.remove();
    }

    return card;
}

function renderPage(data) {
    currentPage = data.page;
    totalPages  = data.total_pages;

    mapContainer.innerHTML = '';

    if (!data.maps.length) {
        renderEmptyState();
        return;
    }

    pagination.style.display = 'flex';
    pageInput.value             = data.page;
    pageInput.max               = data.total_pages;
    totalPagesSpan.textContent  = data.total_pages;
    resultCountSpan.textContent = data.total.toLocaleString();
    firstBtn.disabled = data.page <= 1;
    prevBtn.disabled  = data.page <= 1;
    nextBtn.disabled  = data.page >= data.total_pages;
    lastBtn.disabled  = data.page >= data.total_pages;

    const frag = document.createDocumentFragment();
    data.maps.forEach((map, index) => {
        const card = buildCard(map, index);
        card.style.setProperty('--i', Math.min(index, MAX_STAGGER));
        frag.appendChild(card);
    });
    mapContainer.appendChild(frag);

    scheduleNextPagePrefetch();
}

function renderEmptyState() {
    mapContainer.innerHTML = `
        <div class="empty-state">
            <div class="empty-icon">🔍</div>
            <h2>No maps found</h2>
            <p>Try adjusting your search or filters.</p>
            <button class="btn btn-direct" id="resetFilters">Reset filters</button>
        </div>`;
    pagination.style.display = 'none';
    document.getElementById('resetFilters')?.addEventListener('click', () => {
        searchTerm = '';
        typeFilter = 'all';
        sortBy     = 'default';
        syncDOMToState();
        loadPage(1);
    });
}

async function loadPage(page) {
    page = Math.max(1, page);
    inflightMaps?.abort();
    inflightMaps = new AbortController();
    const signal = inflightMaps.signal;

    showTopProgress();

    const params = new URLSearchParams({
        page:     String(page),
        per_page: String(ITEMS_PER_PAGE),
        type:     typeFilter,
        q:        searchTerm,
        sort:     sortBy,
    });

    let data;
    try {
        data = await fetchJson('/api/maps?' + params.toString(), { signal });
    } catch (e) {
        if (e.name === 'AbortError') return;
        hideTopProgress();
        mapContainer.innerHTML = '';
        renderEmptyState();
        return;
    }

    renderPage(data);
    writeHash();
    hideTopProgress();
}

// ---------------------------------------------------------------------------
//  Modal
// ---------------------------------------------------------------------------
function showModal() {
    lastFocusedElement = document.activeElement;
    modal.hidden = false;
    void modal.offsetWidth;
    modal.classList.add('visible');
    document.body.classList.add('modal-open');
    modalClose.focus();
}
function closeModal() {
    modal.classList.remove('visible');
    document.body.classList.remove('modal-open');
    const done = () => {
        modal.hidden = true;
        if (lastFocusedElement?.focus) lastFocusedElement.focus();
    };
    modal.addEventListener('transitionend', done, { once: true });
    setTimeout(() => { if (!modal.hidden) done(); }, 350);
}

function setAuthorInModal(name, avatar) {
    const nameEl   = modalAuthor.querySelector('.author-name');
    const avatarEl = modalAuthor.querySelector('.author-avatar');
    nameEl.textContent = name || 'Unknown';
    if (avatar) {
        avatarEl.style.backgroundImage = `url("${safeUrl(avatar)}")`;
        avatarEl.classList.add('has-avatar');
    } else {
        avatarEl.style.backgroundImage = '';
        avatarEl.classList.remove('has-avatar');
    }
}

function populateGallery(images) {
    galleryImages = images || [];
    galleryIndex  = 0;
    galleryTrack.innerHTML = '';
    galleryDots.innerHTML  = '';

    if (!galleryImages.length) {
        galleryTrack.innerHTML = '<div class="gallery-empty">No preview available</div>';
        galleryPrev.style.display = 'none';
        galleryNext.style.display = 'none';
        galleryDots.style.display = 'none';
        return;
    }

    for (let i = 0; i < galleryImages.length; i++) {
        const img = document.createElement('img');
        img.src = galleryImages[i];
        img.alt = '';
        img.loading = i < 2 ? 'eager' : 'lazy';
        img.decoding = 'async';
        img.onerror = () => { img.style.visibility = 'hidden'; };
        galleryTrack.appendChild(img);

        const dot = document.createElement('button');
        dot.className = 'dot' + (i === 0 ? ' active' : '');
        dot.type = 'button';
        dot.setAttribute('aria-label', `Show image ${i + 1}`);
        dot.addEventListener('click', () => { galleryIndex = i; updateGallery(); });
        galleryDots.appendChild(dot);
    }

    const multi = galleryImages.length > 1;
    galleryPrev.style.display = multi ? '' : 'none';
    galleryNext.style.display = multi ? '' : 'none';
    galleryDots.style.display = multi ? '' : 'none';
    updateGallery();
}

function updateGallery() {
    if (!galleryImages.length) return;
    galleryTrack.style.transform = `translateX(-${galleryIndex * 100}%)`;
    galleryDots.querySelectorAll('.dot').forEach((d, i) =>
        d.classList.toggle('active', i === galleryIndex));
    if (galleryImages.length > 1) {
        const next = galleryImages[(galleryIndex + 1) % galleryImages.length];
        if (next) { const p = new Image(); p.src = next; }
    }
}
function stepGallery(delta) {
    if (galleryImages.length < 2) return;
    galleryIndex = (galleryIndex + delta + galleryImages.length) % galleryImages.length;
    updateGallery();
}

async function openModal(map) {
    if (!map) return;
    const type = map.Type || { cls: 'other', label: 'Other' };

    modalBadge.className = `badge badge-${type.cls}`;
    modalBadge.textContent = type.label;
    modalTitle.textContent = map.Title || 'Untitled';
    modalSize.textContent  = formatSize(map.Size);
    modalDate.textContent  = 'Added ' + formatDate(map.TimeCreated);

    setAuthorInModal(authorDisplay(map['Аuthor'], map.AuthorName), map.AuthorAvatar);

    if (map.SteamLink) {
        modalSteam.style.display = '';
        modalSteam.href = map.SteamLink;
        modalSteam.innerHTML = ICON_STEAM + ' View on Steam';
    } else {
        modalSteam.style.display = 'none';
    }

    if (map.FilePath) {
        modalYandex.style.display = '';
        modalYandex.href = YANDEX_BASE + map.FilePath;
        modalYandex.innerHTML = ICON_YANDEX + ' Yandex Disk';
    } else {
        modalYandex.style.display = 'none';
    }

    if (map.Link) {
        modalDownload.style.display = '';
        modalDownload.href = map.Link;
        modalDownload.innerHTML = ICON_DOWNLOAD + ' Direct Download';
    } else {
        modalDownload.style.display = 'none';
    }

    modalDescription.textContent = '';
    modalDescription.scrollTop = 0;

    populateGallery(map.PreviewUrl ? [map.PreviewUrl] : []);

    showModal();

    inflightModal?.abort();
    inflightModal = new AbortController();
    const signal = inflightModal.signal;

    try {
        let full = detailCache[map.id];
        if (!full) {
            full = await fetchJson('/api/map/' + map.id, { signal });
            detailCache[map.id] = full;
        }
        if (!modal.classList.contains('visible')) return;

        if (full.Description) {
            modalDescription.textContent = full.Description;
            modalDescription.scrollTop = 0;
        }
        if (Array.isArray(full.Images) && full.Images.length &&
            JSON.stringify(full.Images) !== JSON.stringify(galleryImages)) {
            populateGallery(full.Images);
        }
        if (full.AuthorName || full.AuthorAvatar) {
            setAuthorInModal(authorDisplay(full['Аuthor'], full.AuthorName), full.AuthorAvatar);
        }
    } catch (e) {
        if (e.name === 'AbortError') return;
        console.error(e);
    }

    if (!modalDescription.textContent.trim()) {
        modalDescription.textContent = 'No description provided.';
    }
}

// ---------------------------------------------------------------------------
//  Wiring
// ---------------------------------------------------------------------------
modalClose.addEventListener('click', closeModal);
modal.addEventListener('click', (e) => { if (e.target === modal) closeModal(); });
galleryPrev.addEventListener('click', (e) => { e.stopPropagation(); stepGallery(-1); });
galleryNext.addEventListener('click', (e) => { e.stopPropagation(); stepGallery(1); });

document.addEventListener('keydown', (e) => {
    if (!modal.classList.contains('visible')) {
        if (e.key === '/' && document.activeElement !== searchInput) {
            e.preventDefault();
            searchInput.focus();
            searchInput.select();
        }
        if (e.key === 'Escape' && document.activeElement === searchInput) {
            searchInput.blur();
        }
        return;
    }
    if (e.key === 'Escape') { e.preventDefault(); closeModal(); return; }
    if (e.key === 'ArrowLeft')  { stepGallery(-1); return; }
    if (e.key === 'ArrowRight') { stepGallery(1); return; }
    if (e.key === 'Tab') {
        const focusables = modal.querySelectorAll(
            'button, a[href], input:not([type="hidden"]), [tabindex]:not([tabindex="-1"])'
        );
        if (!focusables.length) return;
        const first = focusables[0];
        const last  = focusables[focusables.length - 1];
        if (e.shiftKey && document.activeElement === first) {
            e.preventDefault(); last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
            e.preventDefault(); first.focus();
        }
    }
});

mapContainer.addEventListener('click', (e) => {
    if (e.target.closest('.btn')) return;
    if (window.getSelection && window.getSelection().toString().length) return;
    const card = e.target.closest('.card');
    if (!card || !card._mapData) return;
    openModal(card._mapData);
});

// Hover-prefetch: 200 ms of dwell on a card warms the detail JSON
// so openModal() renders gallery + description without a loading beat.
let _hoverTimer  = null;
let _hoverCardId = null;
mapContainer.addEventListener('mouseover', (e) => {
    const card = e.target.closest('.card');
    if (!card || !card._mapData) return;
    const id = card._mapData.id;
    if (_hoverCardId === id) return;         // ignore child-element churn
    _hoverCardId = id;
    clearTimeout(_hoverTimer);
    _hoverTimer = setTimeout(() => prefetchDetail(id), 200);
}, { passive: true });

let searchTimer;
searchInput.addEventListener('input', () => {
    searchTerm = searchInput.value;
    searchClear.style.display = searchTerm ? 'block' : 'none';
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => loadPage(1), 220);
});

searchClear.addEventListener('click', () => {
    searchInput.value = '';
    searchTerm = '';
    searchClear.style.display = 'none';
    loadPage(1);
    searchInput.focus();
});

sortSelect.addEventListener('change', () => {
    sortBy = sortSelect.value;
    loadPage(1);
});

filterChips.addEventListener('click', (e) => {
    const chip = e.target.closest('.chip');
    if (!chip) return;
    typeFilter = chip.dataset.type;
    filterChips.querySelectorAll('.chip').forEach(c =>
        c.classList.toggle('active', c === chip));
    loadPage(1);
    window.scrollTo({ top: 0, behavior: 'smooth' });
});

firstBtn.addEventListener('click', () => { loadPage(1); window.scrollTo({ top: 0 }); });
prevBtn.addEventListener('click',  () => { if (currentPage > 1) { loadPage(currentPage - 1); window.scrollTo({ top: 0 }); } });
nextBtn.addEventListener('click',  () => { if (currentPage < totalPages) { loadPage(currentPage + 1); window.scrollTo({ top: 0 }); } });
lastBtn.addEventListener('click',  () => { loadPage(totalPages); window.scrollTo({ top: 0 }); });

pageInput.addEventListener('change', () => {
    const v = parseInt(pageInput.value, 10);
    if (!isNaN(v)) { loadPage(v); window.scrollTo({ top: 0 }); }
    else pageInput.value = currentPage;
});

window.addEventListener('scroll', () => {
    siteHeader.classList.toggle('scrolled', window.scrollY > 8);
}, { passive: true });

window.addEventListener('hashchange', () => {
    if (stateEqualsHash()) return;
    const h = readHash();
    currentPage = h.page;
    searchTerm  = h.q;
    typeFilter  = h.type;
    sortBy      = h.sort;
    syncDOMToState();
    loadPage(currentPage);
});

// ---------------------------------------------------------------------------
//  Boot
// ---------------------------------------------------------------------------
(async function init() {
    const initial     = window.__INITIAL__ || null;
    const hashState   = readHash();
    const matchesSSR  = hashState.page === 1 && !hashState.q &&
                        hashState.type === 'all' && hashState.sort === 'default';

    if (initial && initial.types && initial.page && matchesSSR) {
        renderChips(initial.types);
        renderPage(initial.page);
        syncDOMToState();
        loader.style.display = 'none';
        writeHash();
        return;
    }

    try {
        if (initial && initial.types) renderChips(initial.types);
        else                          await loadTypes();

        if (!matchesSSR) {
            searchTerm = hashState.q;
            typeFilter = hashState.type;
            sortBy     = hashState.sort;
            syncDOMToState();
        }
        await loadPage(hashState.page);
    } catch (e) {
        loadStatus.textContent = 'Failed to load: ' + e.message;
        console.error(e);
    } finally {
        loader.style.display = 'none';
    }
})();