(() => {
    const $ = (id) => document.getElementById(id);


    const toolbar = document.querySelector(".toolbar");

// ---- ДОК ДЛЯ ОГОНЬКА (слева в тулбаре)
    const streakDock = document.createElement("div");
    streakDock.className = "pp-streak-dock";
    streakDock.style.display = "inline-flex";
    streakDock.style.alignItems = "center";
    streakDock.style.marginRight = "10px";

    const GOAL = 1;

// ---- ПАНЕЛЬ АВТОРИЗАЦИИ (без огонька)
    const authBar = document.createElement("div");
    authBar.className = "pp-auth";
    authBar.id = "authBar";

// Оба — в начало тулбара: сперва огонь, сразу за ним логин/профиль
    toolbar.prepend(authBar);
    // toolbar.prepend(streakDock);

    // ===== Лидерборд (глобально, один раз)
    const lbWrap = document.createElement("div");
    lbWrap.className = "pp-modal hidden";
    lbWrap.innerHTML = `
  <div class="pp-modal__backdrop"></div>
  <div class="pp-modal__card">
    <div class="pp-modal__head">
      <div class="pp-modal__title">
        <span id="lbTitle">Топ за день</span>
        <div class="pp-seg">
          <button data-window="day"  class="seg-btn seg-on">День</button>
          <button data-window="week" class="seg-btn">Неделя</button>
          <button data-window="month" class="seg-btn">Месяц</button>
          <button data-window="total" class="seg-btn">Все время</button>
        </div>
      </div>
      <button id="lbClose" class="btn ghost">✕</button>
    </div>
    <div class="pp-modal__body">
      <table class="pp-table">
        <thead><tr><th>#</th><th>Пользователь</th><th>Очки</th></tr></thead>
        <tbody id="lbTbody"></tbody>
      </table>
    </div>
  </div>`;
    document.body.appendChild(lbWrap);

    function openLb() {
        lbWrap.classList.remove("hidden");
    }

    function closeLb() {
        lbWrap.classList.add("hidden");
    }

    lbWrap.querySelector("#lbClose").onclick = closeLb;
    lbWrap.querySelector(".pp-modal__backdrop").onclick = closeLb;

    async function loadLeaderboard(windowKey = "day") {
        const r = await fetch(`/api/auth/leaderboard?window=${encodeURIComponent(windowKey)}`, {cache: "no-store"});
        const js = await r.json();
        const tb = document.getElementById("lbTbody");
        tb.innerHTML = "";
        (js.top || []).forEach((row, i) => {
            const tr = document.createElement("tr");
            tr.innerHTML = `<td>${i + 1}</td><td>@${row.user}</td><td>${row.value}</td>`;
            tb.appendChild(tr);
        });
    }

// переключатели окна
    lbWrap.querySelectorAll(".seg-btn").forEach(b => {
        b.onclick = async () => {
            lbWrap.querySelectorAll(".seg-btn").forEach(x => x.classList.remove("seg-on"));
            b.classList.add("seg-on");
            const w = b.getAttribute("data-window");
            document.getElementById("lbTitle").textContent =
                w === "day" ? "Топ за день" : w === "week" ? "Топ за неделю" : w === "month" ? "Топ за месяц" : "Топ за всё время";
            await loadLeaderboard(w);
        };
    });


// SVG/HTML огня (стейт управляется классом .on)
// ВАЖНО: иконка и число на одной линии (inline-flex + align-items:center)
    const flameHTML = (streakDays = 0, on = false) => `
  <span class="pp-flame ${on ? "on" : ""}" title="Серия дней"
        style="display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:10px;border:1px solid #1a2029;background:#0f1621;">
    <svg viewBox="0 0 32 40" width="24" height="30" aria-hidden="true"
         style="display:block;transform:translateY(1px)">
      <defs>
        <linearGradient id="ppFlameGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%"  stop-color="#FF9D3D"/>
          <stop offset="55%" stop-color="#FF5A22"/>
          <stop offset="100%" stop-color="#E22D2A"/>
        </linearGradient>
        <radialGradient id="ppFlameCore" cx="50%" cy="70%" r="60%">
          <stop offset="0%"   stop-color="#FFE7A3"/>
          <stop offset="60%"  stop-color="#FFC46A"/>
          <stop offset="100%" stop-color="#FF7A3A"/>
        </radialGradient>
      </defs>
      <path d="M16 1 C14 6, 7 8, 7 16 c0 6, 5 10, 9 11 c4-1, 9-5, 9-11 c0-5-3-7-5-10 c0 3-3 5-4 5 c0-3-1-6,0-10Z"
            fill="url(#ppFlameGrad)" opacity="${on ? "1" : ".45"}"/>
      <path d="M16 13 c-3 2-5 4-5 7 c0 3 3 5 5 6 c2-1 5-3 5-6 c0-3-2-4-5-7Z"
            fill="url(#ppFlameCore)" opacity="${on ? "1" : ".35"}"/>
    </svg>
    <b id="streakDays" style="font-size:16px;line-height:1;min-width:22px;text-align:center;display:block">
      ${Number(streakDays) || 0}
    </b>
  </span>
`;


    function updateFlameFromCounts() {
        const dayNow = Number((document.getElementById("cntDay")?.textContent) || 0);
        const flame = streakDock.querySelector(".pp-flame");
        if (flame) flame.classList.toggle("on", dayNow >= GOAL);
    }

    function setStreakNumber(n) {
        const el = streakDock.querySelector("#streakDays");
        if (el) el.textContent = String(Math.max(0, Number(n || 0)));
    }


// ---- РЕНДЕР АВТОРИЗАЦИИ + ОГОНЬ СЛЕВА
    function renderAuth(me) {
        // обновляем огонь в левом доке
        if (me?.logged_in) {
            const s = me.stats || {day: 0, week: 0, streak_days: 0, today_done: false};
            streakDock.innerHTML = flameHTML(s.streak_days, !!s.today_done);
            setStreakNumber(s.streak_days || 0);
        } else {
            streakDock.innerHTML = flameHTML(0, false);
            setStreakNumber(0);
        }

        // сам auth-bar без огня
        if (!me?.logged_in) {
            authBar.innerHTML = `
      <div class="pp-auth__login">
        <input id="authUser" class="pp-input" placeholder="Логин">
        <input id="authPass" class="pp-input" type="password" placeholder="Пароль">
        <button id="btnLogin" class="btn">Войти</button>
        <button id="btnTop" class="btn ghost" title="Лидерборд">Топ</button>
      </div>`;
            if (me?.logged_in) setStreakNumber((me.stats || {}).streak_days || 0);

            authBar.querySelector("#btnLogin").onclick = async () => {
                const username = authBar.querySelector("#authUser").value.trim();
                const password = authBar.querySelector("#authPass").value;
                const r = await fetch("/api/auth/login", {
                    method: "POST",
                    headers: {"Content-Type": "application/json"},
                    body: JSON.stringify({username, password})
                });
                const js = await r.json();
                if (!js.ok) {
                    alert(js.error || "login failed");
                    return;
                }
                fetchMe();
            };
            authBar.querySelector("#btnTop").onclick = async () => {
                await loadLeaderboard("day");
                openLb();
            };

        } else {
            const u = me.user;
            const s = me.stats || {day: 0, week: 0, streak_days: 0, today_done: false};
            authBar.innerHTML = `
      <div class="pp-auth__user">
        <span class="pp-user">@${u}</span>
        <span class="badge">Day <b id="cntDay">${s.day}</b></span>
        <span class="badge">Week <b id="cntWeek">${s.week}</b></span>
        <button id="btnTop" class="btn ghost" title="Лидерборд">Топ</button>
        <button id="btnLogout" class="btn ghost">Выйти</button>
      </div>`;
            authBar.querySelector("#btnLogout").onclick = async () => {
                await fetch("/api/auth/logout", {method: "POST"});
                fetchMe();
            };
            authBar.querySelector("#btnTop").onclick = async () => {
                await loadLeaderboard("day");
                openLb();
            };
        }

        updateFlameFromCounts();
    }

    async function fetchMe() {
        try {
            const r = await fetch("/api/auth/me", {cache: "no-store"});
            renderAuth(await r.json());
        } catch {
        }
    }

    fetchMe();


    // --- DOM
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
    const queueBadge = queueSizeEl ? queueSizeEl.parentElement : null;

    // Скрыть лишнее (если есть)
    ["placeInput", "licSelect", "sortSelect", "namesBox", "btnCopy", "btnJsonl"].forEach(id => {
        const el = $(id);
        if (el) el.style.display = "none";
    });

    // --- State
    let state = {
        inat_taxon_id: null,
        latin: "",
        gbif_id: "",
        common_en: "",
        common_ru: "",
        per_page: 12,
        page: 1,
        total: 0,
        picked: new Map(),
        cache: new Map(),
        queue: [],
        inflight: false,
        snapshotVersion: 0
    };

    // --- сетка
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
        if (pickedCount) pickedCount.textContent = String(state.picked.size);
        const target = Number(targetInput?.value || 0);
        if (targetCount) {
            targetCount.textContent = isFinite(target) && target > 0
                ? (state.total && state.total < target ? `${target} (меньше: ${state.total})` : String(target))
                : "0";
        }
        if (totalCount) totalCount.textContent = String(state.total || 0);
        if (pageTop) pageTop.textContent = String(state.page);
        if (pageBottom) pageBottom.textContent = String(state.page);
        if (queueSizeEl) queueSizeEl.textContent = String(state.queue.length);
        if (queueBadge) {
            const has = state.queue.length > 0;
            queueBadge.style.background = has ? "#1f1a0a" : "#0f1a13";
            queueBadge.style.borderColor = has ? "#6f5d2b" : "#1f5134";
            queueBadge.style.color = has ? "#ffe3a1" : "#b3ffd8";
        }
    }

    async function flushCachesAndSW() {
        try {
            if ("serviceWorker" in navigator) {
                const regs = await navigator.serviceWorker.getRegistrations();
                await Promise.all(regs.map(r => r.unregister().catch(() => {
                })));
            }
        } catch {
        }
        try {
            if ("caches" in window) {
                const keys = await caches.keys();
                await Promise.all(keys.map(k => caches.delete(k)));
            }
        } catch {
        }
        try {
            await fetch("/api/maintenance/flush", {method: "POST", cache: "no-store"});
        } catch {
        }
    }

    async function resolveTaxon(q) {
        const url = `/api/resolve_taxon?q=${encodeURIComponent(q)}`;
        const r = await fetch(url, {cache: "no-store", credentials: "same-origin"});
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
        const r = await fetch(`/api/inat/photos?${params.toString()}`, {cache: "no-store", credentials: "same-origin"});
        const js = await r.json();
        if (!js.ok) throw new Error(js.error || "list failed");
        state.total = js.total || 0;
        state.cache.set(String(page), js.items || []);
        return js.items || [];
    }

    // добираем до полноты logical-страницы
    async function fetchAndFill(page) {
        const seen = new Set();
        const acc = [];
        let raw = page;
        while (acc.length < state.per_page) {
            const chunk = await fetchPage(raw);
            if (!chunk || chunk.length === 0) break;
            for (const it of chunk) {
                const id = String(it.photo_id);
                if (seen.has(id)) continue;
                seen.add(id);
                acc.push(it);
                if (acc.length >= state.per_page) break;
            }
            raw += 1;
        }
        state.cache.set(String(page), acc);
        return acc;
    }

    // небольшое преимущество для первых изображений
    let boost = 6;

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
        img.decoding = "async";
        if (boost > 0) {
            try {
                img.fetchPriority = "high";
            } catch {
            }
            boost--;
        }
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
            const was = state.picked.has(key);
            if (was) {
                state.picked.delete(key);
                card.style.outline = "1px solid #1a2029";
            } else {
                state.picked.set(key, it);
                card.style.outline = "3px solid var(--acc)";
            }
            updateBadges();
            enqueueSync();
        });

        return card;
    }

    async function fetchExistingSelected() {
        if (!state.inat_taxon_id) return;
        const r = await fetch(`/api/collect/selected?taxon_id=${state.inat_taxon_id}`, {
            cache: "no-store",
            credentials: "same-origin"
        });
        const js = await r.json();
        if (!js.ok) return;
        state.picked.clear();
        (js.items || []).forEach(it => {
            const pid = String(it.photo_id || "");
            if (!pid) return;
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
        boost = 6; // каждый раз первые 6 — быстрее
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
            const items = await fetchAndFill(p);
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
            await fetchAndFill(p);
        } catch {
        }
    }

    // ---------- Надёжный POST с таймаутом + fallback ----------
    const SYNC_TIMEOUT_MS = 15000;

    async function postSync(payload) {
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), SYNC_TIMEOUT_MS);
        const bodyStr = JSON.stringify(payload);
        try {
            const resp = await fetch("/api/collect/sync", {
                method: "POST",
                headers: {"Content-Type": "application/json", "Cache-Control": "no-store"},
                body: bodyStr,
                signal: controller.signal,
                credentials: "same-origin",
                cache: "no-store",
                keepalive: false
            });
            clearTimeout(t);
            let js = {};
            try {
                js = await resp.json();
            } catch {
            }
            return {ok: resp.ok, status: resp.status, body: js};
        } catch (e) {
            clearTimeout(t);
            try {
                const ok = navigator.sendBeacon &&
                    navigator.sendBeacon("/api/collect/sync", new Blob([bodyStr], {type: "application/json"}));
                if (!ok) console.error("sendBeacon failed");
            } catch (e2) {
                console.error("beacon error", e2);
            }
            throw e;
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
            selected: Array.from(state.picked.values())
        };
    }

    function enqueueSync() {
        const snap = snapshotSelection();
        state.queue = [snap]; // коалесим до одного «последнего»
        updateBadges();
        if (!state.inflight) processQueue();
    }

    async function processQueue() {
        if (state.inflight) return;
        state.inflight = true;
        try {
            while (state.queue.length > 0) {
                const current = state.queue[0];
                try {
                    const res = await postSync(current);

                    // --- моментальное обновление по числам с бэка
                    const added = Number(res?.body?.added || 0);
                    const removed = Number(res?.body?.removed || 0);
                    const delta = added - removed;

                    if (delta !== 0) {
                        const dayEl = document.getElementById("cntDay");
                        const weekEl = document.getElementById("cntWeek");
                        const streakEl = document.getElementById("streakDays"); // число рядом с огнём

                        const prevDay = Number(dayEl?.textContent || 0);
                        const newDay = Math.max(0, prevDay + delta);

                        if (dayEl) dayEl.textContent = String(newDay);
                        if (weekEl) weekEl.textContent = String(Math.max(0, Number(weekEl?.textContent || 0) + delta));

                        // огонёк за/гасить по порогу GOAL
                        updateFlameFromCounts();

                        // стрик: оптимистично — если сегодня перешли через порог вверх → минимум 1; если ушли ниже → 0
                        if (streakEl) {
                            if (prevDay < GOAL && newDay >= GOAL) {
                                const cur = Number(streakEl.textContent || 0);
                                if (cur < 1) streakEl.textContent = "1";
                            } else if (prevDay >= GOAL && newDay < GOAL) {
                                streakEl.textContent = "0";
                            }
                        }

                        if (prevDay < GOAL && newDay >= GOAL) {
                            setStreakNumber(1);
                        } else if (prevDay >= GOAL && newDay < GOAL) {
                            setStreakNumber(0);
                        }

                        // затем подтянуть «истину» с бэка (учтёт цепочку прошлых дней и точный стрик)
                        try {
                            await fetchMe();
                        } catch {
                        }
                    }


                    // и сразу подтягиваем «истину» (стрик, неделя и т.п.)
                    if ((added + removed) > 0) {
                        try {
                            await fetchMe();
                        } catch {
                        }
                    }

                    // даже при ok:false снимаем задание, чтобы очередь не висла
                    state.queue.shift();
                    updateBadges();
                } catch (e) {
                    console.error("sync error", e);
                    break;
                }
            }
        } finally {
            state.inflight = false;
            if (state.queue.length > 0) setTimeout(processQueue, 0);
        }
    }


    document.addEventListener("visibilitychange", () => {
        if (!document.hidden && state.queue.length > 0 && !state.inflight) {
            processQueue();
        }
    });

    // ---------- actions ----------
    btnLoad?.addEventListener("click", async () => {
        try {
            state.picked.clear();
            state.cache.clear();
            state.queue = [];
            state.inflight = false;
            updateBadges();

            await flushCachesAndSW();
            await resolveTaxon(taxonInput.value.trim());
            await fetchExistingSelected();

            state.per_page = 12;
            await showPage(1);
            updateBadges();

            // init sync без очереди
            const initSnap = snapshotSelection();
            try {
                await postSync(initSnap);
            } catch (e) {
                console.error(e);
            }
            updateBadges();
        } catch (e) {
            console.error(e);
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

    if (targetCount) targetCount.textContent = String(Number(targetInput?.value || 0));
    targetInput?.addEventListener("input", () => updateBadges());
    taxonInput?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") btnLoad?.click();
    });
})();
