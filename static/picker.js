const $ = s => document.querySelector(s);

// UI
const taxonInput = $("#taxonInput");
const placeInput = $("#placeInput");
const targetInput = $("#targetInput");
const licSelect = $("#licSelect");
const sortSelect = $("#sortSelect");
const btnLoad = $("#btnLoad");
const namesBox = $("#namesBox");
const totalCount = $("#totalCount");
const pickedCount = $("#pickedCount");
const targetCount = $("#targetCount");
const hint = $("#smallHint");
const grid = $("#grid");
const btnPrev = $("#btnPrev"), btnNext = $("#btnNext");
const btnPrev2 = $("#btnPrev2"), btnNext2 = $("#btnNext2");
const uiPage = $("#uiPage"), uiPage2 = $("#uiPage2");
const btnCopy = $("#btnCopy"), btnCSV = $("#btnCSV"), btnJSONL = $("#btnJSONL");

const PAGE_SIZE = 10;

const state = {
    // фильтры
    taxonId: null,
    taxonLatin: "",
    placeId: "",
    target: 200,
    license: "cc0,cc-by,cc-by-nc",
    sort: "faves",

    // iNat странички
    inatPage: 1,
    inatPerPage: 200,
    inatTotal: 0,
    inatFinished: false,

    // таксон инфо
    vern_en: [],
    vern_ru: [],
    species_name: "",
    species_id: null,

    // буфер карточек (плоский список фоток)
    buffer: [],         // [{photo, fields...}]
    // показаемая "UI-страница" по 10 штук
    uiPageIndex: 0,

    // выбор
    picked: new Map(),  // photo_id -> manifest row
    skipped: new Set(), // photo_id

    // техн.
    loading: false,
    seenPhotos: new Set(),
};

// --- утилиты ---

function escapeHtml(s) {
    return (s || "").replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
}

function originalUrl(u) {
    // iNat photo.url обычно .../square.jpg | small | medium | large -> original
    return u.replace(/(square|small|medium|large)\.(jpe?g|png)$/i, "original.$2");
}

function getGenusFamilyFromTaxon(taxon) {
    // пытаемся достать genus/family из ancestors
    let genus = null, family = null;
    const anc = taxon?.ancestors || [];
    for (const a of anc) {
        if (!a || !a.rank) continue;
        if (a.rank === "genus") genus = a.name || genus;
        if (a.rank === "family") family = a.name || family;
    }
    // иногда сам taxon и есть genus
    if (!genus && taxon?.rank === "genus") genus = taxon.name;
    return {genus, family};
}

function updateKPI() {
    targetCount.textContent = state.target;
    pickedCount.textContent = state.picked.size;
    totalCount.textContent = state.inatTotal;
    const pageHuman = state.uiPageIndex + 1;
    uiPage.textContent = pageHuman;
    uiPage2.textContent = pageHuman;
    const canPrev = state.uiPageIndex > 0;
    const canNext = (state.uiPageIndex + 1) * PAGE_SIZE < state.buffer.length || !state.inatFinished;
    btnPrev.disabled = btnPrev2.disabled = !canPrev;
    btnNext.disabled = btnNext2.disabled = !canNext;
}

function bannerIfSmall() {
    // если найдено мало (<100) — предложить выбрать все
    if (state.inatTotal > 0 && state.inatTotal < 100) {
        hint.style.display = "";
        const remaining = state.inatTotal - state.picked.size;
        hint.innerHTML = `
      <b>Мало изображений для этого таксона (всего ${state.inatTotal}).</b>
      Можно забрать всё сразу. <button id="pickAll" class="badge yes">Pick all</button>
      <span class="muted">Мы всё равно будем подгружать карточки</span>`;
        $("#pickAll").onclick = () => {
            // добавляем все, что уже в буфере, и по мере загрузки — тоже
            for (const it of state.buffer) tryPick(it);
            renderPage();
            updateKPI();
        };
    } else {
        hint.style.display = "none";
        hint.innerHTML = "";
    }
}

function manifestRowFromItem(it) {
    return {
        photo_id: it.photo_id,
        observation_id: it.observation_id,
        src: "inat",
        photo_url: originalUrl(it.photo_url),
        observation_url: it.observation_url,
        license: it.license || "",
        attribution: it.attribution || "",
        species_id: it.species_id ?? null,
        species_name: it.species_name || "",
        genus: it.genus || null,
        family: it.family || null,
        vernacular_en: state.vern_en,
        vernacular_ru: state.vern_ru,
        place_id: it.place_id ?? null,
        lat: it.lat ?? null,
        lng: it.lng ?? null,
        positional_accuracy: it.positional_accuracy ?? null,
        observed_on: it.observed_on || "",
        faves: it.faves ?? 0,
        organ: null,
        split: null,
        sha256: null
    };
}

// --- загрузка/парсинг ---

async function resolveTaxonInputToIdAndNames() {
    const v = (taxonInput.value || "").trim();
    state.taxonLatin = v;
    state.vern_en = [];
    state.vern_ru = [];
    state.species_name = "";
    state.species_id = null;

    if (!v) throw new Error("Введите taxon_id или латинское имя");

    if (/^\d+$/.test(v)) {
        state.taxonId = v;
    } else {
        // по имени → taxa
        const r = await fetch(`/api/inat/taxa?q=${encodeURIComponent(v)}&is_active=true&per_page=5`);
        const j = await r.json();
        const first = (j.results || [])[0];
        if (!first) throw new Error("Таксон не найден");
        state.taxonId = String(first.id);
        state.species_name = first.name || v;
        state.species_id = first.id || null;
    }

    // имена EN/RU
    try {
        const rr = await fetch(`/api/latin2common?name=${encodeURIComponent(state.species_name || v)}`);
        const jj = await rr.json();
        state.vern_en = Array.isArray(jj.en) ? jj.en : [];
        state.vern_ru = Array.isArray(jj.ru) ? jj.ru : [];
    } catch {
    }

    namesBox.textContent = `EN: ${state.vern_en.slice(0, 3).join(", ") || "—"} | RU: ${state.vern_ru.slice(0, 3).join(", ") || "—"}`;
}

async function fetchInatPage() {
    if (state.loading || state.inatFinished) return;
    state.loading = true;

    const q = new URLSearchParams({
        taxon_id: state.taxonId,
        per_page: String(state.inatPerPage),
        page: String(state.inatPage),
        quality_grade: "research",
        photo_license: state.license,
        order_by: state.sort,
        order: state.sort === "observed_on" ? "desc" : "desc",
        geo: "true",
        verifiable: "true",
        locale: "en"
    });
    if ((state.placeId || "").trim()) q.set("place_id", state.placeId.trim());

    const r = await fetch(`/api/inat/observations?${q.toString()}`);
    const j = await r.json();

    const results = j.results || [];
    state.inatTotal = j.total_results ?? state.inatTotal;

    // плоский список фоток
    for (const o of results) {
        if (!Array.isArray(o.photos)) continue;
        const taxon = o.taxon || {};
        const {genus, family} = getGenusFamilyFromTaxon(taxon);
        for (const p of o.photos) {
            const pid = p.id;
            if (!pid || state.seenPhotos.has(pid)) continue;
            state.seenPhotos.add(pid);
            state.buffer.push({
                photo_id: pid,
                observation_id: o.id,
                photo_url: p.url || "",
                license: (p.license_code || "").toUpperCase(),
                attribution: p.attribution || "",
                species_id: taxon.id ?? null,
                species_name: taxon.name || "",
                genus, family,
                place_id: (o.place_ids || [])[0] ?? null,
                lat: o.geojson?.coordinates?.[1] ?? null,
                lng: o.geojson?.coordinates?.[0] ?? null,
                positional_accuracy: o.positional_accuracy ?? null,
                observed_on: o.observed_on || "",
                faves: o.faves_count ?? 0,
                observation_url: o.uri || `https://www.inaturalist.org/observations/${o.id}`
            });
        }
    }

    // флаги пагинации
    const received = results.length;
    const per = Number(j.per_page || state.inatPerPage);
    const page = Number(j.page || state.inatPage);
    const total = Number(j.total_results || 0);
    // iNat может возвращать total > per*page даже если фоток нет (фильтр по фото). Ориентируемся на фактические фотки.
    if (received === 0 || per * page >= total) {
        state.inatFinished = true;
    } else {
        state.inatPage += 1;
    }

    state.loading = false;
    bannerIfSmall();
    updateKPI();
}

async function ensureBufferForNextPage() {
    const needUntil = (state.uiPageIndex + 1) * PAGE_SIZE;
    while (state.buffer.length < needUntil && !state.inatFinished) {
        await fetchInatPage();
    }
}

// --- рендер/пагинация ---

function cardHTML(it) {
    const picked = state.picked.has(it.photo_id);
    const skipped = state.skipped.has(it.photo_id);
    const cls = `card ${picked ? "pick" : ""} ${skipped ? "skip" : ""}`;
    const tax = escapeHtml(it.species_name || "");
    const lic = escapeHtml(it.license || "");
    const date = escapeHtml(it.observed_on || "");
    const user = ""; // в манифест не обязателен, можно не показывать логин
    const faves = it.faves ?? 0;

    return `
    <div class="${cls}" data-photo="${it.photo_id}">
      <img class="thumb" src="${it.photo_url}" alt="">
      <div class="muted" style="margin-top:.35rem">${tax || "—"} • ${date || "—"}</div>
      <div class="row2">
        <span class="pill">${lic || "-"}</span>
        <span class="muted">❤ ${faves}</span>
      </div>
      <div class="row2">
        <button class="btnPick">${picked ? "Unpick" : "Pick"}</button>
        <a class="muted" href="${it.observation_url}" target="_blank">open</a>
        <button class="btnSkip" title="Скрыть карточку">Skip</button>
      </div>
    </div>`;
}

function renderPage() {
    const start = state.uiPageIndex * PAGE_SIZE;
    const end = Math.min(start + PAGE_SIZE, state.buffer.length);
    const slice = state.buffer.slice(start, end);

    // если страница пустая, но ещё можно грузить — подгружаем и повторяем
    if (slice.length === 0 && !state.inatFinished) {
        ensureBufferForNextPage().then(renderPage);
        return;
    }

    grid.innerHTML = slice.map(cardHTML).join("");

    // обработчики — быстрый 1-клик
    for (const card of grid.querySelectorAll(".card")) {
        const pid = Number(card.getAttribute("data-photo"));
        const it = state.buffer.find(x => x.photo_id === pid);

        // клик по изображению — toggle pick
        const img = card.querySelector(".thumb");
        img.addEventListener("click", () => {
            togglePick(it);
            // обновим подпись кнопки
            const b = card.querySelector(".btnPick");
            b.textContent = state.picked.has(pid) ? "Unpick" : "Pick";
            card.classList.toggle("pick", state.picked.has(pid));
            updateKPI();
            autoAdvanceIfNeeded();
        });

        // кнопки
        card.querySelector(".btnPick").onclick = () => {
            togglePick(it);
            const b = card.querySelector(".btnPick");
            b.textContent = state.picked.has(pid) ? "Unpick" : "Pick";
            card.classList.toggle("pick", state.picked.has(pid));
            updateKPI();
            autoAdvanceIfNeeded();
        };

        card.querySelector(".btnSkip").onclick = () => {
            state.skipped.add(pid);
            card.classList.add("skip");
            autoAdvanceIfNeeded();
        };
    }

    updateKPI();
}

function nextUiPage() {
    state.uiPageIndex += 1;
    ensureBufferForNextPage().then(renderPage);
}

function prevUiPage() {
    if (state.uiPageIndex > 0) {
        state.uiPageIndex -= 1;
        renderPage();
    }
}

function autoAdvanceIfNeeded() {
    // если на странице все карточки либо выбраны, либо скипнуты — и цель не достигнута — переходим далее
    const start = state.uiPageIndex * PAGE_SIZE;
    const end = Math.min(start + PAGE_SIZE, state.buffer.length);
    let allDone = true;
    for (let i = start; i < end; i++) {
        const pid = state.buffer[i]?.photo_id;
        if (!pid) continue;
        if (!state.picked.has(pid) && !state.skipped.has(pid)) {
            allDone = false;
            break;
        }
    }
    if (allDone && state.picked.size < state.target) {
        nextUiPage();
    }
}

// --- выбор ---

function tryPick(it) {
    if (!it) return;
    if (state.picked.has(it.photo_id)) return;
    state.picked.set(it.photo_id, manifestRowFromItem(it));
}

function togglePick(it) {
    if (!it) return;
    if (state.picked.has(it.photo_id)) {
        state.picked.delete(it.photo_id);
    } else {
        tryPick(it);
    }
}

// --- экспорт ---

function copyURLs() {
    const urls = [...state.picked.values()].map(v => v.photo_url).join("\n");
    navigator.clipboard.writeText(urls).then(() => {
    }, () => {
    });
}

function exportCSV() {
    const headers = ["photo_id", "observation_id", "src", "photo_url", "observation_url", "license", "attribution", "species_id", "species_name", "genus", "family", "vernacular_en", "vernacular_ru", "place_id", "lat", "lng", "positional_accuracy", "observed_on", "faves", "organ", "split", "sha256"];
    const lines = [headers.join(",")];
    for (const v of state.picked.values()) {
        const row = [
            v.photo_id, v.observation_id, v.src, v.photo_url, v.observation_url, v.license, v.attribution,
            v.species_id, v.species_name, v.genus, v.family,
            JSON.stringify(v.vernacular_en || []), JSON.stringify(v.vernacular_ru || []),
            v.place_id, v.lat, v.lng, v.positional_accuracy, v.observed_on, v.faves, v.organ, v.split, v.sha256
        ].map(x => `"${String(x ?? "").replace(/"/g, '""')}"`);
        lines.push(row.join(","));
    }
    const blob = new Blob([lines.join("\n")], {type: "text/csv"});
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "manifest.csv";
    a.click();
}

function exportJSONL() {
    const lines = [...state.picked.values()].map(v => JSON.stringify(v));
    const blob = new Blob([lines.join("\n")], {type: "application/json"});
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "manifest.jsonl";
    a.click();
}

// --- main flow ---

async function startLoad() {
    // сброс
    state.buffer = [];
    state.seenPhotos = new Set();
    state.picked.clear();
    state.skipped.clear();
    state.uiPageIndex = 0;
    state.inatPage = 1;
    state.inatFinished = false;
    state.inatTotal = 0;

    state.placeId = (placeInput.value || "").trim();
    state.target = Math.max(1, parseInt(targetInput.value || "200", 10));
    state.license = licSelect.value || "cc0,cc-by,cc-by-nc";
    state.sort = sortSelect.value || "faves";
    targetCount.textContent = state.target;

    try {
        await resolveTaxonInputToIdAndNames();
    } catch (e) {
        alert(e.message || "Ошибка в taxon");
        return;
    }

    await ensureBufferForNextPage();
    renderPage();
    updateKPI();
}

// нажатия
btnLoad.onclick = startLoad;
btnNext.onclick = nextUiPage;
btnNext2.onclick = nextUiPage;
btnPrev.onclick = prevUiPage;
btnPrev2.onclick = prevUiPage;

btnCopy.onclick = copyURLs;
btnCSV.onclick = exportCSV;
btnJSONL.onclick = exportJSONL;
