// Спин: webp-кэш, числовой порядок, полная предзагрузка, абсолютное управление. Загрузка zip — стабильные JSON-ошибки.
const $ = s => document.querySelector(s);

const datasetsWrap = $("#datasets");
const listError = $("#listError");

const uploadForm = $("#uploadForm");
const zipInput = $("#zipInput");
const displayNameInput = $("#displayName");
const pwd = $("#pwd");
const uploadMsg = $("#uploadMsg");

const pageList = $("#pageList");
const pageViewer = $("#pageViewer");
const btnHome = $("#btnHome");
const viewerTitle = $("#viewerTitle");
const viewerMode = $("#viewerMode");
const loading = $("#loading");
const imgSpin = $("#spinImg");
const sliderWrap = $("#spinSliderWrap");
const slider = $("#spinSlider");
const btnDelete = $("#btnDelete");

let scene, camera, renderer, controls, mesh;
let raf = 0;
let currentDatasetId = null;

// canvas для спина
let canvasSpin = null, ctxSpin = null;
let frames = [];     // ImageBitmap[]
let frameIndex = 0;

// абсолютное управление
let dragStartX = 0, dragStartIndex = 0, dragging = false;

document.addEventListener("DOMContentLoaded", () => fetchDatasets());

// ---------- Datasets ----------
async function fetchDatasets() {
    try {
        const res = await fetch("/api/datasets", {cache: "no-store"});
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        datasetsWrap.innerHTML = "";
        listError.style.display = "none";
        data.forEach(d => datasetsWrap.appendChild(card(d)));
        if (!data.length) {
            datasetsWrap.innerHTML = `<div class="card"><div>В папке <code>data/</code> пока ничего не найдено.</div></div>`;
        }
    } catch (e) {
        listError.textContent = "Ошибка загрузки списка наборов.";
        listError.style.display = "block";
        console.error(e);
    }
}

function card(d) {
    const el = document.createElement("div");
    el.className = "card";
    el.innerHTML = `
    <div style="display:flex;justify-content:space-between;gap:.5rem">
      <strong title="${escapeHtml(d.id)}">${escapeHtml(d.title)}</strong>
      <span class="badge ${d.mode === "model" ? "ready" : "processing"}">${d.mode}</span>
    </div>
    <div class="meta">папка: ${escapeHtml(d.id)} • фото: ${d.images}</div>
    ${d.thumb ? `<img src="${d.thumb}" alt="" style="width:100%;height:140px;object-fit:cover;border-radius:.6rem;border:1px solid #1a2029" />` : ""}
    <div class="row"><button data-open ${d.mode === "empty" ? "disabled" : ""}>Открыть</button></div>
  `;
    el.querySelector("[data-open]").onclick = () => openViewer(d);
    return el;
}

function escapeHtml(s) {
    return (s || "").replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]))
}

// ---------- Upload ----------
uploadForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!zipInput?.files?.length) {
        uploadMsg.textContent = "Выберите zip.";
        return;
    }
    if (!pwd?.value?.trim()) {
        uploadMsg.textContent = "Введите пароль.";
        return;
    }

    uploadMsg.textContent = "Загрузка…";
    const btn = uploadForm.querySelector("button[type=submit]");
    btn && (btn.disabled = true);

    const fd = new FormData();
    fd.append("zipfile", zipInput.files[0]);
    if (displayNameInput?.value?.trim()) fd.append("display_name", displayNameInput.value.trim());
    fd.append("password", pwd.value.trim());

    try {
        const r = await fetch("/api/upload_zip", {method: "POST", body: fd});
        const raw = await r.text();
        let j = null;
        try {
            j = JSON.parse(raw);
        } catch {
        }
        if (!r.ok || (j && j.ok === false)) throw new Error((j && (j.error || j.message)) || `HTTP ${r.status}`);
        uploadMsg.textContent = `OK: ${(j?.display_name) || (j?.dataset_id) || "ok"}`;
        await fetchDatasets();
    } catch (e2) {
        uploadMsg.textContent = `Ошибка: ${e2.message}`;
    } finally {
        btn && (btn.disabled = false);
        zipInput.value = "";
        displayNameInput.value = "";
        pwd.value = "";
    }
});

// ---------- Viewer ----------
function openViewer(d) {
    currentDatasetId = d.id;
    pageList.style.display = "none";
    pageViewer.style.display = "";
    btnHome && (btnHome.style.display = "");
    viewerTitle.textContent = d.title || d.id;

    if (d.mode === "model" && d.model_url) {
        viewerMode.textContent = "3D";
        viewerMode.className = "badge ready";
        initThreeView(d.model_type, d.model_url);
    } else {
        viewerMode.textContent = "spin";
        viewerMode.className = "badge processing";
        initSpinView(d.id);
    }
}

btnHome && (btnHome.onclick = () => {
    disposeThree();
    disposeSpin();
    pageViewer.style.display = "none";
    pageList.style.display = "";
    btnHome.style.display = "none";
});

// удаление набора
btnDelete && (btnDelete.onclick = async () => {
    if (!currentDatasetId) return;
    const pw = prompt("Пароль для удаления:");
    if (!pw) return;
    if (!confirm(`Удалить набор «${currentDatasetId}» безвозвратно?`)) return;
    const fd = new FormData();
    fd.append("dataset_id", currentDatasetId);
    fd.append("password", pw);
    try {
        const r = await fetch("/api/delete_dataset", {method: "POST", body: fd});
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j.error || r.statusText);
        btnHome?.click();
        await fetchDatasets();
    } catch (e) {
        alert("Не удалось удалить: " + e.message);
    }
});

// ---------- Spin (Canvas + ImageBitmap, абсолютное управление, без инерции) ----------
async function initSpinView(datasetRel) {
    disposeThree();

    if (!canvasSpin) {
        canvasSpin = document.createElement("canvas");
        canvasSpin.id = "spinCanvas";
        canvasSpin.style.width = "100%";
        canvasSpin.style.height = "100%";
        $("#viewerWrap").appendChild(canvasSpin);
        ctxSpin = canvasSpin.getContext("2d", {alpha: false, desynchronized: true});
    }
    $("#glcanvas").style.display = "none";
    canvasSpin.style.display = "block";
    imgSpin.style.display = "none";
    sliderWrap.style.display = "none";
    loading.style.display = "block";
    loading.textContent = "Подготовка…";
    resizeSpinCanvas();

    // 1) получаем URL'ы и сортируем ЧИСЛОВО
    const res = await fetch(`/api/spin/${encodeURIComponent(datasetRel)}?w=1280&max=90`, {cache: "no-store"});
    let urls = await res.json();
    const num = u => {
        const m = u.match(/(\d+)(?=\.[a-z0-9]+$)/i);
        return m ? parseInt(m[1], 10) : Number.MAX_SAFE_INTEGER;
    };
    urls = urls.slice().sort((a, b) => num(a) - num(b));

    // 2) предзагружаем ВСЕ кадры (порядок сохраняется по индексу)
    const {bitmaps} = await preloadBitmaps(urls, (done, total) => {
        loading.textContent = `Загрузка кадров… (${done}/${total})`;
    }, 6);

    if (!bitmaps.length) {
        loading.textContent = "Нет кадров";
        return;
    }

    frames = bitmaps;
    frameIndex = 0;
    loading.style.display = "none";
    sliderWrap.style.display = "block";
    drawFrame();

    setupSpinControls();   // абсолютное управление
}

function disposeSpin() {
    cancelAnimationFrame(raf);
    window.removeEventListener("resize", resizeSpinCanvas);
    dragging = false;
    if (canvasSpin) {
        const ctx = canvasSpin.getContext("2d");
        ctx && ctx.clearRect(0, 0, canvasSpin.width, canvasSpin.height);
    }
    frames.forEach(b => b.close?.());
    frames = [];
    if (canvasSpin) canvasSpin.style.display = "none";
    sliderWrap.style.display = "none";
}

function resizeSpinCanvas() {
    if (!canvasSpin) return;
    const dpr = window.devicePixelRatio || 1;
    const rect = canvasSpin.getBoundingClientRect();
    canvasSpin.width = Math.max(1, Math.round(rect.width * dpr));
    canvasSpin.height = Math.max(1, Math.round(rect.height * dpr));
    drawFrame();
}

window.addEventListener("resize", resizeSpinCanvas);

function drawFrame() {
    if (!ctxSpin || !frames.length) return;
    const bmp = frames[((frameIndex % frames.length) + frames.length) % frames.length];
    const cw = canvasSpin.width, ch = canvasSpin.height;
    const r = Math.min(cw / bmp.width, ch / bmp.height);
    const w = Math.round(bmp.width * r), h = Math.round(bmp.height * r);
    const x = (cw - w) >> 1, y = (ch - h) >> 1;
    ctxSpin.imageSmoothingEnabled = true;
    ctxSpin.imageSmoothingQuality = "high";
    ctxSpin.clearRect(0, 0, cw, ch);
    ctxSpin.drawImage(bmp, x, y, w, h);
}

function pxPerFrame() {
    const w = canvasSpin?.clientWidth || 320;
    const n = Math.max(1, frames.length || 60);
    return Math.max(3, Math.round(w / n));
}

function setupSpinControls() {
    slider.max = Math.max(0, frames.length - 1);
    slider.value = 0;
    slider.oninput = (e) => {
        frameIndex = parseInt(e.target.value, 10);
        drawFrame();
    };

    const wrap = canvasSpin;

    const start = x => {
        dragging = true;
        dragStartX = x;
        dragStartIndex = frameIndex;
    };
    const move = x => {
        if (!dragging) return;
        const dx = x - dragStartX;
        const step = Math.trunc(dx / pxPerFrame());      // кадр = функция абсолютного dx
        frameIndex = (dragStartIndex + step) % frames.length;
        if (frameIndex < 0) frameIndex += frames.length;
        slider.value = frameIndex;
        drawFrame();
    };
    const end = () => {
        dragging = false;
    };

    wrap.onmousedown = e => start(e.clientX);
    window.onmousemove = e => move(e.clientX);
    window.onmouseup = () => end();

    wrap.ontouchstart = e => {
        const t = e.touches[0];
        start(t.clientX);
    };
    wrap.ontouchmove = e => {
        e.preventDefault();
        const t = e.touches[0];
        move(t.clientX);
    };
    wrap.ontouchend = () => end();

    // колесо — дискретные шаги, без инерции
    wrap.onwheel = e => {
        e.preventDefault();
        const delta = (Math.abs(e.deltaX) > Math.abs(e.deltaY)) ? e.deltaX : -e.deltaY;
        const steps = Math.sign(delta) * Math.max(1, Math.round(Math.abs(delta) / 35));
        frameIndex = (frameIndex + steps) % frames.length;
        if (frameIndex < 0) frameIndex += frames.length;
        slider.value = frameIndex;
        drawFrame();
    };
}

// предзагрузка ImageBitmap по индексам (сохранение порядка)
async function preloadBitmaps(urls, onProgress, concurrency = 6) {
    const out = new Array(urls.length).fill(null);
    let done = 0;

    async function loadOne(i) {
        const url = urls[i];
        try {
            const r = await fetch(url, {cache: "force-cache"});
            const b = await r.blob();
            const bmp = await createImageBitmap(b, {colorSpaceConversion: "none", premultiplyAlpha: "none"});
            out[i] = bmp;
        } catch {
            out[i] = null;
        } finally {
            done++;
            onProgress?.(done, urls.length);
        }
    }

    const q = urls.map((_, i) => i);
    const workers = new Array(Math.min(concurrency, q.length)).fill(0).map(async () => {
        while (q.length) await loadOne(q.shift());
    });
    await Promise.all(workers);

    return {bitmaps: out.filter(Boolean)}; // порядок индексов сохранён
}

// ---------- Three.js ----------
function initThreeView(type, url) {
    disposeSpin();
    $("#glcanvas").style.display = "block";
    if (!renderer) initThree();
    loadModel(type, url);
}

function initThree() {
    const canvas = $("#glcanvas");
    const w = canvas.clientWidth, h = canvas.clientHeight;
    renderer = new THREE.WebGLRenderer({canvas, antialias: true});
    renderer.setPixelRatio(window.devicePixelRatio || 1);
    renderer.setSize(w, h);
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0a0e13);
    camera = new THREE.PerspectiveCamera(50, w / h, 0.01, 1000);
    camera.position.set(0.7, 0.7, 1.6);
    controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.06;
    const amb = new THREE.AmbientLight(0xaaaaaa, 1.2);
    scene.add(amb);
    const dir = new THREE.DirectionalLight(0xffffff, 1.0);
    dir.position.set(1, 1, 1);
    scene.add(dir);
    const grid = new THREE.GridHelper(10, 10, 0x334455, 0x22303a);
    grid.material.transparent = true;
    grid.material.opacity = .25;
    scene.add(grid);
    window.addEventListener("resize", onResize);
    animate();
}

function disposeThree() {
    cancelAnimationFrame(raf);
    window.removeEventListener("resize", onResize);
    if (mesh) {
        mesh.traverse?.(o => {
            if (o.isMesh) {
                o.geometry?.dispose?.();
                Array.isArray(o.material) ? o.material.forEach(m => m.dispose?.()) : o.material?.dispose?.();
            }
        });
    }
    renderer?.dispose?.();
    scene = camera = renderer = controls = mesh = null;
    $("#glcanvas").style.display = "none";
}

function onResize() {
    if (!renderer || !camera) return;
    const canvas = renderer.domElement;
    const w = canvas.parentElement.clientWidth, h = canvas.parentElement.clientHeight;
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
}

function animate() {
    raf = requestAnimationFrame(animate);
    controls && controls.update();
    renderer && scene && camera && renderer.render(scene, camera);
}

function loadModel(type, url) {
    loading.style.display = "block";
    const end = () => loading.style.display = "none";
    if (type === "ply") {
        const loader = new THREE.PLYLoader();
        loader.load(url, g => {
            g.computeVertexNormals?.();
            g.center?.();
            const m = new THREE.MeshStandardMaterial({metalness: .05, roughness: .85});
            mesh = new THREE.Mesh(g, m);
            scene.add(mesh);
            fitCameraToObject(mesh);
            end();
        }, undefined, end);
    } else if (type === "obj") {
        const loader = new THREE.OBJLoader();
        loader.load(url, obj => {
            mesh = obj;
            scene.add(mesh);
            fitCameraToObject(mesh);
            end();
        }, undefined, end);
    } else {
        const loader = new THREE.GLTFLoader();
        loader.load(url, gltf => {
            mesh = gltf.scene;
            scene.add(mesh);
            fitCameraToObject(mesh);
            end();
        }, undefined, end);
    }
}

function fitCameraToObject(object) {
    const box = new THREE.Box3().setFromObject(object);
    const size = new THREE.Vector3();
    box.getSize(size);
    const center = new THREE.Vector3();
    box.getCenter(center);
    const maxDim = Math.max(size.x, size.y, size.z) || 1;
    const fov = camera.fov * (Math.PI / 180);
    let dist = (maxDim / 2) / Math.tan(fov / 2) * 1.6;
    camera.position.copy(center).add(new THREE.Vector3(dist, dist * 0.6, dist * 1.2));
    camera.near = Math.max(0.001, maxDim / 100);
    camera.far = Math.max(10, maxDim * 100);
    camera.updateProjectionMatrix();
    controls.target.copy(center);
    controls.update();
}
