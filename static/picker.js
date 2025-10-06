// Plant Picker — очередь синхронизаций + индикатор Queue:N
(() => {
    const $ = (id) => document.getElementById(id);

    // DOM
    const taxonInput = $("taxonInput");
    const targetInput = $("targetInput");
    const btnLoad = $("btnLoad");

    const btnPrevTop = $("btnPrevTop");
    const btnNextTop = $("btnNextTop");
    const pageTop = $("pageTop");
    const btnPrevBottom = $("btnPrevBottom");
    const btnNextBottom = $("btnNextBottom");
    const pageBottom = $("pageBottom");

    const grid = $("grid");
    const pickedCount = $("pickedCount");
    const targetCount = $("targetCount");
    const totalCount = $("totalCount");
    const queueSizeEl = $("queueSize");
    const queueBadge = queueSizeEl ? queueSizeEl.parentElement : null; // ← ДОБАВЬ


    // скрыть лишнее в шапке
    ["placeInput", "licSelect", "sortSelect", "namesBox", "btnCopy", "btnJsonl"].forEach(id => {
        const el = $(id);
        if (el) el.style.display = "none";
    });

    // --- State ---
    let state = {
        inat_taxon_id: null,
        latin: "",
        gbif_id: "",
        common_en: "",
        common_ru: "",
        per_page: 10,
        page: 1,
        total: 0,
        picked: new Map(),   // photo_id -> normalized item
        cache: new Map(),    // "page" -> items[]

        // очередь синхронизаций
        queue: [],           // массив снапшотов
        inflight: false,     // есть активный POST
        snapshotVersion: 0
    };

    const queueKey = () => state.inat_taxon_id ? `pp:queue:${state.inat_taxon_id}` : null;

    // сетка-галерея
    grid.classList.add("grid");
    grid.style.gridTemplateColumns = "repeat(auto-fill, minmax(260px, 1fr))";
    grid.style.gap = "10px";

    function setLoading(on) {
        grid.innerHTML = "";
        if (on) {
            for (let i = 0; i < state.per_page; i++) {
                const sk = document.createElement("div");
                sk.className = "card";
                sk.style.minHeight = "190px";
                grid.appendChild(sk);
            }
        }
    }

    function updateBadges() {
        pickedCount.textContent = String(state.picked.size);
        const target = Number(targetInput.value || 0);
        targetCount.textContent = isFinite(target) && target > 0
            ? (state.total && state.total < target ? `${target} (меньше: ${state.total})` : String(target))
            : "0";
        totalCount.textContent = String(state.total || 0);
        pageTop.textContent = String(state.page);
        pageBottom.textContent = String(state.page);
        if (queueSizeEl) queueSizeEl.textContent = String(state.queue.length);

        if (queueBadge) {
            const has = state.queue.length > 0;
            queueBadge.style.background = has ? "#1f1a0a" : "#0f1a13";
            queueBadge.style.borderColor = has ? "#6f5d2b" : "#1f5134";
            queueBadge.style.color = has ? "#ffe3a1" : "#b3ffd8";
        }

    }

    async function resolveTaxon(q) {
        const url = `/api/resolve_taxon?q=${encodeURIComponent(q)}`;
        const r = await fetch(url);
        const js = await r.json();
        if (!js.ok) throw new Error(js.error || "resolve failed");
        state.inat_taxon_id = js.inat_taxon_id;
        state.latin = js.latin || "";
        state.gbif_id = js.gbif_id || "";
        state.common_en = js.common_en || "";
        state.common_ru = js.common_ru || "";
    }

    function normalizeItem(it) {
        return {
            photo_id: String(it.photo_id),
            observation_id: String(it.observation_id || ""),
            best_url: String(it.best_url || it.thumb_url || ""),
            width: it.width || "",
            height: it.height || "",
            license: String(it.license || ""),
            attribution: String(it.attribution || ""),
            observed_on: String(it.observed_on || ""),
            time_observed_at: String(it.time_observed_at || ""),
            user_login: String(it.user_login || ""),
            place_guess: String(it.place_guess || ""),
            quality_grade: String(it.quality_grade || "")
        };
    }

    async function fetchPage(page) {
        const params = new URLSearchParams({
            taxon_id: state.inat_taxon_id,
            page,
            per_page: state.per_page,
            sort: "faves",
        });
        const r = await fetch(`/api/inat/photos?${params.toString()}`);
        const js = await r.json();
        if (!js.ok) throw new Error(js.error || "list failed");
        state.total = js.total || 0;
        state.cache.set(String(page), js.items || []);
        return js.items || [];
    }

    function makeTile(imgUrl, itRaw) {
        const it = normalizeItem(itRaw);
        const key = it.photo_id;

        const card = document.createElement("div");
        card.className = "card";
        card.style.padding = "0";
        card.style.overflow = "hidden";
        card.style.position = "relative";
        card.style.border = "1px solid #1a2029";

        const box = document.createElement("div");
        box.style.width = "100%";
        box.style.aspectRatio = "4 / 3";
        box.style.position = "relative";
        box.style.background = "#0a0e13";

        const img = document.createElement("img");
        img.loading = "lazy";
        img.src = imgUrl;
        img.alt = key;
        img.style.position = "absolute";
        img.style.inset = "0";
        img.style.width = "100%";
        img.style.height = "100%";
        img.style.objectFit = "cover";

        box.appendChild(img);
        card.appendChild(box);

        const selected = state.picked.has(key);
        card.style.outline = selected ? "3px solid var(--acc)" : "1px solid #1a2029";

        card.addEventListener("click", () => {
            if (state.picked.has(key)) {
                state.picked.delete(key);
                card.style.outline = "1px solid #1a2029";
            } else {
                state.picked.set(key, it);
                card.style.outline = "3px solid var(--acc)";
            }
            updateBadges();
            enqueueSync(); // ← ставим в очередь (индикатор ↑)
        });

        return card;
    }

    async function fetchExistingSelected() {
        if (!state.inat_taxon_id) return;
        const r = await fetch(`/api/collect/selected?taxon_id=${state.inat_taxon_id}`);
        const js = await r.json();
        if (!js.ok) return;
        state.picked.clear();
        (js.items || []).forEach(it => {
            const pid = String(it.photo_id || "");
            if (!pid) return;
            // нормализуем под текущую структуру
            state.picked.set(pid, {
                photo_id: pid,
                observation_id: String(it.observation_id || ""),
                best_url: String(it.best_url || ""),
                width: it.width || "",
                height: it.height || "",
                license: String(it.license || ""),
                attribution: String(it.attribution || ""),
                observed_on: String(it.observed_on || ""),
                time_observed_at: String(it.time_observed_at || ""),
                user_login: String(it.user_login || ""),
                place_guess: String(it.place_guess || ""),
                quality_grade: String(it.quality_grade || "")
            });
        });
    }

    function renderItems(items) {
        grid.innerHTML = "";
        for (const it of items) {
            const card = makeTile(it.best_url || it.thumb_url, it);
            grid.appendChild(card);
        }
        updateBadges();
    }

    async function showPage(p) {
        state.page = p;
        updateBadges();
        const key = String(p);
        if (state.cache.has(key)) {
            renderItems(state.cache.get(key));
        } else {
            setLoading(true);
            const items = await fetchPage(p);
            renderItems(items);
        }
        prefetch(p + 1);
        prefetch(p + 2);
    }

    async function prefetch(p) {
        if (p <= 0) return;
        const key = String(p);
        if (state.cache.has(key)) return;
        try {
            await fetchPage(p);
        } catch (_) {
        }
    }

    // ---------- Очередь/воркер ----------
    function snapshotSelection() {
        return {
            version: ++state.snapshotVersion,
            taxon_id: state.inat_taxon_id,
            latin: state.latin || "",
            gbif_id: state.gbif_id || "",
            common_en: state.common_en || "",
            common_ru: state.common_ru || "",
            selected: Array.from(state.picked.values()) // нормализовано
        };
    }

    function saveQueue() {
        const k = queueKey();
        if (!k) return;
        try {
            localStorage.setItem(k, JSON.stringify({queue: state.queue}));
        } catch (_) {
        }
    }

    function loadQueue() {
        const k = queueKey();
        if (!k) return;
        try {
            const raw = localStorage.getItem(k);
            if (!raw) return;
            const parsed = JSON.parse(raw);
            if (parsed && Array.isArray(parsed.queue)) {
                state.queue = parsed.queue;
            }
        } catch (_) {
        }
    }

    function enqueueSync() {
        const snap = snapshotSelection();
        state.queue.push(snap);   // FIFO — каждый клик отдельной задачей
        saveQueue();
        updateBadges();           // Queue:N ↑
        if (!state.inflight) processQueue();
    }

    async function processQueue() {
        if (state.inflight) return;
        state.inflight = true;
        try {
            while (state.queue.length > 0) {
                const current = state.queue[0]; // НЕ удаляем до 200
                try {
                    const r = await fetch("/api/collect/sync", {
                        method: "POST",
                        headers: {"Content-Type": "application/json"},
                        body: JSON.stringify(current),
                        keepalive: true
                    });
                    // если сервер вернул не-200 — считаем ошибкой
                    if (!r.ok) throw new Error(`HTTP ${r.status}`);
                    await r.json().catch(() => ({}));
                    // успех → снимаем из очереди, обновляем индикатор
                    state.queue.shift();
                    saveQueue();
                    updateBadges(); // Queue:N ↓ (по факту пришедшего 200)
                } catch (e) {
                    // сеть/сервер недоступны — оставляем current в очереди, выходим
                    break;
                }
            }
        } finally {
            state.inflight = false;
            // если пока отправляли добавились новые — дожмём
            if (state.queue.length > 0) processQueue();
        }
    }

    // Перед закрытием — отправим последний снап и оставим очередь в localStorage
    window.addEventListener("beforeunload", () => {
        const last = state.queue.length > 0 ? state.queue[state.queue.length - 1] : snapshotSelection();
        if (!last || !last.taxon_id) return;
        const blob = new Blob([JSON.stringify(last)], {type: "application/json"});
        try {
            navigator.sendBeacon("/api/collect/sync", blob);
        } catch (_) {
        }
        saveQueue();
    });

    // ---------- actions ----------
    btnLoad?.addEventListener("click", async () => {
        try {
            state.picked.clear();
            state.cache.clear();
            state.queue = [];
            state.inflight = false;
            await resolveTaxon(taxonInput.value.trim());
            await fetchExistingSelected();
            state.per_page = 10;
            await showPage(1);
            updateBadges();
            // восстановим хвост очереди для этого таксона (если был)
            loadQueue();
            updateBadges();
            processQueue(); // догнать «хвост»
            // пустой снап для species.csv/папки (как init)
            enqueueSync();
        } catch (e) {
            alert(e.message || e);
        }
    });

    btnPrevTop?.addEventListener("click", () => {
        if (state.page > 1) showPage(state.page - 1);
    });
    btnPrevBottom?.addEventListener("click", () => {
        if (state.page > 1) showPage(state.page - 1);
    });
    btnNextTop?.addEventListener("click", () => {
        showPage(state.page + 1);
    });
    btnNextBottom?.addEventListener("click", () => {
        showPage(state.page + 1);
    });

    // Target badge live
    targetCount.textContent = String(Number(targetInput.value || 0));
    targetInput?.addEventListener("input", () => updateBadges());

    // Enter запускает загрузку
    taxonInput?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") btnLoad?.click();
    });
})();
