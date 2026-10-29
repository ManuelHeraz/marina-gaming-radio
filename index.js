'use strict';
/**
 * MARINA GAMING RADIO - AutoDJ v2.1 (Parrilla Horaria CDMX + Podcasts en 3 Partes + Prioridad MongoDB)
 *
 * Arquitectura:
 *   [yt-dlp] -> temp/track-N.m4a -> [ffmpeg DECODER] --PCM--> [ffmpeg ENCODER] -> Icecast/ZenoFM
 *                (uno por pista / segmento)                     (UNO, vive siempre)
 *
 * Funciones:
 *  - Reloj en tiempo real ajustado a CDMX (GMT-6) leyendo programacion.json.
 *  - Peticiones de MongoDB con prioridad absoluta: reproduce cortinilla de petición SOLO
 *    antes de la primera solicitud de una racha; se rearma cuando la cola de MongoDB se vacía.
 *  - Ciclo de shows musicales: Intro al cambiar de programa, y cada 4 canciones inserta:
 *    [Cortinilla Estándar] -> [Comercial] -> [Cortinilla Mid del Show].
 *  - Modo Podcast (Checkpoint Gaming): el episodio se descarga EN SEGUNDO PLANO unos minutos antes
 *    de que empiece el programa (la música sigue sonando mientras tanto), se mide su duración exacta
 *    y se divide virtualmente en 3 partes iguales (sin recodificar en disco), intercalando
 *    Cortinilla Estándar + Canción + Cortinilla del Show entre cada parte.
 *  - Filtro de duración máxima para canciones (descarta vlogs / recopilaciones de horas / directos).
 */

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { spawn, spawnSync } = require('child_process');
const { exec } = require('youtube-dl-exec');
const { MongoClient } = require('mongodb');

// ───────────────────────── CONFIGURACIÓN ─────────────────────────
const env = process.env;

const CONFIG = {
    sampleRate: 44100,
    bitrate: env.BITRATE || '128k',
    bumperEvery: 4,                                               // ciclo cada 4 canciones
    prefetchLeadMs: (Number(env.PREFETCH_LEAD_SECONDS) || 45) * 1000,
    writeWatchdogMs: 20000,                                       // encoder sin aceptar datos => reinicio
    decoderStallMs: 20000,                                        // decoder sin producir datos => descarta pista
    downloadTimeoutMs: 90000,                                     // 90s para canciones normales
    podcastDownloadTimeoutMs: 240000,                             // 4 min para episodios de podcast largos
    podcastPrefetchMinutes: Number(env.PODCAST_PREFETCH_MINUTES) || 12, // cuánto antes se descarga el podcast
    maxSongSec: 720,                                              // canciones de más de 12 min se descartan
    cacheMaxFiles: Number(env.CACHE_MAX_FILES) || 4,             // canciones guardadas como respaldo si YouTube falla
    loudnorm: env.LOUDNORM !== '0',
    dbName: 'marina_radio',
    dbCollection: 'cola_reproduccion',
};

const REQUIRED_ENV = ['ZENO_PASSWORD', 'ZENO_SERVER', 'ZENO_PORT', 'ZENO_MOUNT'];
const missing = REQUIRED_ENV.filter((k) => !env[k]);
if (missing.length) {
    console.error(`[Fatal] Faltan variables en .env: ${missing.join(', ')}`);
    process.exit(1);
}

const ASSETS_DIR = path.join(__dirname, 'assets');
const TEMP_DIR = path.join(__dirname, 'temp');
const PLAYLIST_FILE = path.join(__dirname, 'playlist.json');
const BUMPERS_FILE = path.join(ASSETS_DIR, 'bumpers.json');
const PROGRAMACION_FILE = path.join(__dirname, 'programacion.json');
const CACHE_DIR = path.join(__dirname, 'cache');

const log = (tag, msg) => console.log(`[${tag}] ${msg}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ───────────────────────── FFMPEG ─────────────────────────
function resolveFfmpeg() {
    if (env.FFMPEG_PATH) return env.FFMPEG_PATH;
    const probe = spawnSync('ffmpeg', ['-version'], { stdio: 'ignore' });
    if (!probe.error && probe.status === 0) return 'ffmpeg';
    try { return require('ffmpeg-static'); } catch { return 'ffmpeg'; }
}
const FFMPEG = resolveFfmpeg();

let shuttingDown = false;
const children = new Set();

function track(proc) {
    children.add(proc);
    proc.once('close', () => children.delete(proc));
    return proc;
}

// Obtiene la duración exacta en segundos de un archivo descargado usando ffmpeg -i
function probeDurationSec(filePath) {
    return new Promise((resolve) => {
        const p = spawn(FFMPEG, ['-hide_banner', '-i', filePath], { stdio: ['ignore', 'ignore', 'pipe'] });
        let out = '';
        p.stderr.on('data', (d) => { out += d; });
        const t = setTimeout(() => p.kill('SIGKILL'), 10000);
        p.on('error', () => { clearTimeout(t); resolve(0); });
        p.on('close', () => {
            clearTimeout(t);
            const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(out);
            resolve(m ? Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]) : 0);
        });
    });
}

// ───────────────────────── CARGA DE LISTAS Y BARAJAS ─────────────────────────
// Verifica que una pista exista y no tenga el texto placeholder "PONER_LINK"
function isPlayableTrack(t) {
    return Boolean(
        t &&
        typeof t.title === 'string' &&
        typeof t.source === 'string' &&
        t.source.trim() !== '' &&
        !t.source.includes('PONER_LINK')
    );
}

function loadTracks(file) {
    try {
        if (!fs.existsSync(file)) return [];
        const raw = fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, '');
        const data = JSON.parse(raw);
        const list = Array.isArray(data.tracks) ? data.tracks : (Array.isArray(data) ? data : []);
        return list.filter(isPlayableTrack);
    } catch (e) {
        log('Cerebro', `No se pudo leer ${path.basename(file)}: ${e.message}`);
        return [];
    }
}

const mainLibrary = loadTracks(PLAYLIST_FILE);
if (mainLibrary.length === 0) {
    console.error('[Fatal] playlist.json está vacío o es inválido. Nada que transmitir.');
    process.exit(1);
}
log('Sistema', `Bóveda general cargada: ${mainLibrary.length} pistas.`);

// Sistema de barajas sin repetición por archivo de playlist
const decks = new Map();

function nextFromPlaylistFile(relOrAbsPath) {
    const fullPath = path.isAbsolute(relOrAbsPath) ? relOrAbsPath : path.join(__dirname, relOrAbsPath);
    let tracks = loadTracks(fullPath);

    // Si la playlist del show aún no existe o está vacía, usa la bóveda general
    let key = fullPath;
    if (tracks.length === 0) {
        tracks = mainLibrary;
        key = PLAYLIST_FILE;
    }

    let state = decks.get(key);
    if (!state || state.deck.length === 0 || state.total !== tracks.length) {
        const deck = tracks.map((_, i) => i);
        for (let i = deck.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [deck[i], deck[j]] = [deck[j], deck[i]];
        }
        if (state && deck.length > 1 && deck[deck.length - 1] === state.lastIndex) {
            [deck[0], deck[deck.length - 1]] = [deck[deck.length - 1], deck[0]];
        }
        state = { deck, lastIndex: -1, total: tracks.length };
        decks.set(key, state);
    }

    state.lastIndex = state.deck.pop();
    return { ...tracks[state.lastIndex], trusted: true, isSong: true };
}

function localBumpers() {
    if (!fs.existsSync(ASSETS_DIR)) return [];
    return fs.readdirSync(ASSETS_DIR)
        .filter((f) => f.toLowerCase().endsWith('.mp3'))
        .map((f) => ({ title: `Marina Gaming - ${f.replace(/\.mp3$/i, '')}`, source: path.join(ASSETS_DIR, f), trusted: true }));
}

function allGeneralBumpers() {
    const list = localBumpers();
    if (fs.existsSync(BUMPERS_FILE)) {
        for (const t of loadTracks(BUMPERS_FILE)) list.push({ ...t, trusted: true });
    }
    return list;
}

function pickRandomGeneralBumper() {
    const bumpers = allGeneralBumpers();
    if (bumpers.length === 0) return null;
    return bumpers[Math.floor(Math.random() * bumpers.length)];
}

// ───────────────────────── RELOJ CDMX (GMT-6) Y PROGRAMACIÓN ─────────────────────────
function loadProgramacion() {
    try {
        if (!fs.existsSync(PROGRAMACION_FILE)) return null;
        const raw = fs.readFileSync(PROGRAMACION_FILE, 'utf8').replace(/^\uFEFF/, '');
        return JSON.parse(raw);
    } catch (e) {
        log('Programación', `Error leyendo programacion.json: ${e.message}`);
        return null;
    }
}

// Obtiene día de la semana (0=Dom..6=Sáb), minutos desde medianoche y fecha YYYY-MM-DD en CDMX (GMT-6)
// offsetDays permite consultar "mañana" (1) para detectar programas que empiezan después de medianoche
function getCdmxTimeInfo(offsetDays = 0) {
    // CDMX es permanentemente UTC-6
    const now = new Date();
    const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
    const cdmxDate = new Date(utcMs - (6 * 3600000));
    if (offsetDays) cdmxDate.setDate(cdmxDate.getDate() + offsetDays);

    const day = cdmxDate.getDay(); // 0 (Dom) a 6 (Sáb)
    const hours = cdmxDate.getHours();
    const minutes = cdmxDate.getMinutes();
    const totalMinutes = hours * 60 + minutes;
    const dateKey = `${cdmxDate.getFullYear()}-${String(cdmxDate.getMonth() + 1).padStart(2, '0')}-${String(cdmxDate.getDate()).padStart(2, '0')}`;
    const timeStr = `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;

    return { day, hours, minutes, totalMinutes, dateKey, timeStr };
}

function parseHhMm(str) {
    const [h, m] = String(str || '00:00').split(':').map(Number);
    return (h || 0) * 60 + (m || 0);
}

function getActiveShow(prog) {
    if (!prog || !Array.isArray(prog.horarios) || !prog.shows) return null;
    const { day, totalMinutes, dateKey, timeStr } = getCdmxTimeInfo();

    for (const slot of prog.horarios) {
        if (!Array.isArray(slot.dias) || !slot.dias.includes(day)) continue;
        const startMin = parseHhMm(slot.inicio);
        const endMin = parseHhMm(slot.fin);
        if (totalMinutes >= startMin && totalMinutes < endMin) {
            const showDef = prog.shows[slot.show];
            if (showDef) {
                return {
                    id: slot.show,
                    slotKey: `${dateKey}_${slot.inicio}_${slot.show}`,
                    timeStr,
                    ...showDef,
                };
            }
        }
    }
    return null;
}

// Busca un programa de tipo podcast que EMPIECE dentro de los próximos `leadMin` minutos
function findUpcomingPodcastSlot(prog, leadMin) {
    if (!prog || !Array.isArray(prog.horarios) || !prog.shows) return null;
    const nowInfo = getCdmxTimeInfo(0);

    for (const offset of [0, 1]) { // hoy y mañana (por si arranca poco después de medianoche)
        const info = offset === 0 ? nowInfo : getCdmxTimeInfo(offset);
        for (const slot of prog.horarios) {
            const showDef = prog.shows[slot.show];
            if (!showDef || showDef.tipo !== 'podcast') continue;
            if (!Array.isArray(slot.dias) || !slot.dias.includes(info.day)) continue;

            const delta = parseHhMm(slot.inicio) + offset * 1440 - nowInfo.totalMinutes;
            if (delta >= 0 && delta <= leadMin) {
                return {
                    id: slot.show,
                    slotKey: `${info.dateKey}_${slot.inicio}_${slot.show}`,
                    timeStr: nowInfo.timeStr,
                    ...showDef,
                };
            }
        }
    }
    return null;
}

// Baraja para comerciales sin repetir hasta recorrer todos
let commercialDeck = [];
function pickCommercial(prog) {
    if (!prog || !Array.isArray(prog.comerciales)) return null;
    const valid = prog.comerciales.filter(isPlayableTrack);
    if (valid.length === 0) return null;

    if (commercialDeck.length === 0) {
        commercialDeck = valid.map((_, i) => i);
        for (let i = commercialDeck.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [commercialDeck[i], commercialDeck[j]] = [commercialDeck[j], commercialDeck[i]];
        }
    }
    const idx = commercialDeck.pop() % valid.length;
    return { ...valid[idx], trusted: true };
}

function pickRequestBumper(prog) {
    if (!prog || !Array.isArray(prog.cortinillas_peticion)) return null;
    const valid = prog.cortinillas_peticion.filter(isPlayableTrack);
    if (valid.length === 0) return null;
    return { ...valid[Math.floor(Math.random() * valid.length)], trusted: true };
}

// ───────────────────────── MONGODB (Peticiones con Cortinilla Inicial) ─────────────────────────
let mongo = null;
function getCollection() {
    if (!env.MONGODB_URI) return null;
    if (!mongo) {
        mongo = new MongoClient(env.MONGODB_URI, {
            serverSelectionTimeoutMS: 4000,
            connectTimeoutMS: 4000,
            socketTimeoutMS: 10000,
            maxPoolSize: 2,
        });
    }
    return mongo.db(CONFIG.dbName).collection(CONFIG.dbCollection);
}

function isAllowedUrl(src) {
    try {
        const u = new URL(src);
        if (u.protocol !== 'https:' && u.protocol !== 'http:') return false;
        const h = u.hostname.toLowerCase();
        return ['youtube.com', 'youtu.be', 'soundcloud.com'].some((d) => h === d || h.endsWith(`.${d}`));
    } catch { return false; }
}

async function takeCommunityRequest() {
    const col = getCollection();
    if (!col) return null;
    try {
        for (let i = 0; i < 5; i++) {
            const doc = await col.findOneAndDelete({}, { sort: { _id: 1 }, maxTimeMS: 3000 });
            if (!doc) return null;
            if (typeof doc.source === 'string' && isAllowedUrl(doc.source)) {
                log('Cerebro', `Petición de la comunidad encontrada: "${doc.title || doc.source}". Prioridad máxima.`);
                return { title: String(doc.title || 'Petición de la comunidad'), source: doc.source, trusted: false, community: true, isSong: true };
            }
            log('Cerebro', `Petición inválida descartada: ${String(doc.source).slice(0, 80)}`);
        }
    } catch (e) {
        log('BD', `Error: ${e.message}. Usando programación normal.`);
    }
    return null;
}

// ───────────────────────── DESCARGA (ESTRATEGIA HIDRA) ─────────────────────────
// Separamos clientes SIN cookies (android_vr) y clientes CON cookies que evitan SABR (tv,web_creator,mweb)
// Usamos clientes estándar y de TV que no exigen PO tokens tan agresivos
const HYDRA_STRATEGIES = [
    { label: 'cookies + formato directo', client: null, useCookies: true },
    { label: 'android_vr (sin cookies)', client: 'android_vr', useCookies: false },
    { label: 'ios (con cookies)', client: 'ios', useCookies: true }
];
const PERMANENT_ERRORS = /private video|no longer available|terminated|video unavailable|removed|copyright/i;
let preferredStrategyIdx = 0;
let downloadSeq = 0;

function cleanupPrefix(filePath) {
    const dir = path.dirname(filePath);
    const base = path.basename(filePath);
    try {
        for (const f of fs.readdirSync(dir)) {
            if (f.startsWith(base)) { try { fs.unlinkSync(path.join(dir, f)); } catch {} }
        }
    } catch {}
}

// Captura tanto el WARNING relevante como el ERROR final para no trabajar a ciegas
function summarizeError(err) {
    const text = String(err && (err.stderr || err.message) || err);
    const lines = text.split('\n').map((s) => s.trim()).filter(Boolean);
    const warn = lines.find((l) => /WARNING:.*(skipping|sabr|nsig|po token|cookies)/i.test(l));
    const last = lines[lines.length - 1] || '';
    return (warn ? `${warn} | ${last}` : last).slice(0, 240);
}

let lastCookieWarn = 0;
function warnCookies() {
    if (Date.now() - lastCookieWarn < 600000) return;
    lastCookieWarn = Date.now();
    const c = env.YTDLP_COOKIES;
    if (!c) log('Cookies', '⚠ YouTube pide iniciar sesión y YTDLP_COOKIES no está definida en el entorno.');
    else if (!fs.existsSync(c)) log('Cookies', `⚠ YTDLP_COOKIES apunta a un archivo que no existe: ${c}`);
    else log('Cookies', '⚠ YouTube sigue pidiendo login pese a usar cookies: están expiradas o rotadas. Re-expórtalas desde ventana de incógnito.');
}

// ── Caché de respaldo: las últimas canciones reproducidas con éxito se conservan en disco ──
function cachedSongs() {
    try {
        return fs.readdirSync(CACHE_DIR).filter((f) => f.endsWith('.m4a')).map((f) => path.join(CACHE_DIR, f));
    } catch { return []; }
}

function saveToCache(item) {
    try {
        const slug = String(item.track.title).replace(/[^\p{L}\p{N}\- ]+/gu, '').trim().slice(0, 60) || 'cancion';
        fs.renameSync(item.path, path.join(CACHE_DIR, `${Date.now()}__${slug}.m4a`));
        const files = cachedSongs().sort();
        while (files.length > CONFIG.cacheMaxFiles) { try { fs.unlinkSync(files.shift()); } catch {} }
    } catch (e) {
        log('Caché', `No se pudo guardar en caché: ${e.message}`);
        cleanupPrefix(item.path);
    }
}

// Devuelve { ok } o { ok:false, filtered:true } si el video fue rechazado por el filtro de duración
async function downloadAudio(url, outFile, opts = {}) {
    const { timeoutMs = CONFIG.downloadTimeoutMs, maxSec = 0 } = opts;
    const isYoutube = /youtube\.com|youtu\.be/i.test(url);

    // Ordenamos para probar primero la última estrategia que funcionó
    const order = isYoutube
        ? [
            HYDRA_STRATEGIES[preferredStrategyIdx],
            ...HYDRA_STRATEGIES.filter((_, idx) => idx !== preferredStrategyIdx),
        ]
        : [{ label: 'direct', client: null, useCookies: false }];

    for (const strat of order) {
        if (shuttingDown) return { ok: false };
        // Si la estrategia requiere cookies pero no hay archivo configurado, la saltamos
        if (strat.useCookies && (!env.YTDLP_COOKIES || !fs.existsSync(env.YTDLP_COOKIES))) continue;

        log('yt-dlp', `Descargando [${strat.label}]...`);
        try {
            const flags = {
                // Cascada blindada: busca audio puro -> formato 18 (360p con AAC) -> cualquier mejor formato
                format: 'bestaudio/best[height<=360]/18/best',
                output: outFile,
                noPlaylist: true,
                noPart: true,
                noMtime: true,
                socketTimeout: 10,
                retries: 2,
                jsRuntimes: 'node',
            };
            if (maxSec) flags.matchFilters = `duration<=?${maxSec} & !is_live`;
            if (strat.client) flags.extractorArgs = `youtube:player_client=${strat.client}`;
            if (strat.useCookies && env.YTDLP_COOKIES) flags.cookies = env.YTDLP_COOKIES;

            const res = await exec(url, flags, { timeout: timeoutMs, killSignal: 'SIGKILL' });

            if (fs.existsSync(outFile) && fs.statSync(outFile).size > 10000) {
                const winIdx = HYDRA_STRATEGIES.indexOf(strat);
                if (winIdx >= 0) preferredStrategyIdx = winIdx;
                return { ok: true };
            }
            if (/does not pass filter/i.test(`${res.stdout || ''}${res.stderr || ''}`)) {
                return { ok: false, filtered: true };
            }
            throw new Error('archivo descargado vacío');
        } catch (e) {
            log('yt-dlp', `${strat.label} falló: ${summarizeError(e)}`);
            cleanupPrefix(outFile);
            const errText = String(e.stderr || e.message);
            if (strat.useCookies && /not a bot|sign in to confirm/i.test(errText)) warnCookies();
            if (PERMANENT_ERRORS.test(errText)) break;
        }
    }
    return { ok: false };
}

// ───────────────────────── PODCAST: DESCARGA EN SEGUNDO PLANO ─────────────────────────
// Un episodio por "slot" (fecha + hora + show). Estados: downloading | ready | failed
const podcastPrep = new Map();

async function downloadPodcastEpisode(show) {
    const podPlaylistPath = path.join(__dirname, show.playlist_podcasts || 'playlists/checkpoint_podcasts.json');
    const podTracks = loadTracks(podPlaylistPath);

    if (podTracks.length === 0) {
        log('Podcast', 'No hay episodios válidos en la playlist de podcasts. Se usará música general.');
        return null;
    }

    const chosen = podTracks[Math.floor(Math.random() * podTracks.length)];
    const podFile = path.join(TEMP_DIR, `podcast-${process.pid}-${Date.now()}-${++downloadSeq}.m4a`);

    log('Podcast', `Descargando episodio: "${chosen.title}"...`);
    const dl = await downloadAudio(chosen.source, podFile, { timeoutMs: CONFIG.podcastDownloadTimeoutMs });
    if (!dl.ok) {
        log('Podcast', `Falló la descarga del episodio "${chosen.title}".`);
        return null;
    }

    const totalSec = await probeDurationSec(podFile);
    if (totalSec < 60) {
        log('Podcast', `Duración inválida (${totalSec}s). Descartando archivo de podcast.`);
        cleanupPrefix(podFile);
        return null;
    }

    log('Podcast', `Episodio listo en disco: "${chosen.title}" (${Math.round(totalSec / 60)} min).`);
    return { chosen, podFile, totalSec };
}

function ensurePodcastPrep(show) {
    let prep = podcastPrep.get(show.slotKey);
    if (prep) return prep;

    prep = { status: 'downloading', episode: null, consumed: false, createdAt: Date.now(), announcedWait: false };
    podcastPrep.set(show.slotKey, prep);

    downloadPodcastEpisode(show)
        .then((ep) => { prep.episode = ep; prep.status = ep ? 'ready' : 'failed'; })
        .catch((e) => { log('Podcast', `Error inesperado: ${e.message}`); prep.status = 'failed'; });

    return prep;
}

// Se ejecuta cada 30 s: adelanta la descarga del podcast y limpia episodios que nunca se usaron
function podcastBackgroundTick() {
    if (shuttingDown) return;
    try {
        const prog = loadProgramacion();
        const upcoming = findUpcomingPodcastSlot(prog, CONFIG.podcastPrefetchMinutes);
        if (upcoming && !podcastPrep.has(upcoming.slotKey)) {
            log('Podcast', `"${upcoming.nombre || upcoming.id}" empieza pronto. Descargando episodio en segundo plano...`);
            ensurePodcastPrep(upcoming);
        }

        for (const [key, prep] of podcastPrep) {
            const stale = Date.now() - prep.createdAt > 3 * 3600 * 1000;
            if (stale && prep.status !== 'downloading') {
                if (prep.episode && !prep.consumed) cleanupPrefix(prep.episode.podFile);
                podcastPrep.delete(key);
            }
        }
    } catch (e) {
        log('Podcast', `Error en la revisión en segundo plano: ${e.message}`);
    }
}

// ───────────────────────── EL CEREBRO (Horarios, Bloques y Podcasts) ─────────────────────────
let songsSinceBumper = 0;
let currentShowId = null;
let lastPodcastSlotKey = null;
let enBloquePeticiones = false;
let peticionEnEspera = null;
const colaInterna = []; // Cola FIFO para secuencias (Intro, Cortinilla+Comercial+Mid, o los pasos del Podcast)

// Con el episodio ya descargado, calcula sus 3 tercios y arma la escaleta en colaInterna
function buildPodcastSequence(show, episode) {
    const { chosen, podFile, totalSec } = episode;
    const tercio = Math.floor(totalSec / 3);
    log('Podcast', `Armando escaleta: 3 bloques de ~${Math.round(tercio / 60)} min.`);

    const musicFile = show.playlist_musica || 'playlist.json';

    // 1. Cortinilla de Inicio del Podcast
    if (isPlayableTrack(show.intro)) {
        colaInterna.push({ ...show.intro, trusted: true });
    }

    // 2. Fragmento 1/3 (No borrar archivo al terminar)
    colaInterna.push({
        title: `${chosen.title} [Parte 1/3]`,
        source: podFile,
        trusted: true,
        keepTempFile: true,
        seekSec: 0,
        durationSec: tercio,
    });

    // 3. Corte 1: Cortinilla Estándar + Canción + Cortinilla del Show
    const b1 = pickRandomGeneralBumper();
    if (b1) colaInterna.push(b1);
    colaInterna.push(nextFromPlaylistFile(musicFile));
    if (isPlayableTrack(show.mid)) {
        colaInterna.push({ ...show.mid, trusted: true });
    }

    // 4. Fragmento 2/3 (No borrar archivo al terminar)
    colaInterna.push({
        title: `${chosen.title} [Parte 2/3]`,
        source: podFile,
        trusted: true,
        keepTempFile: true,
        seekSec: tercio,
        durationSec: tercio,
    });

    // 5. Corte 2: Cortinilla Estándar + Canción + Cortinilla del Show
    const b2 = pickRandomGeneralBumper();
    if (b2) colaInterna.push(b2);
    colaInterna.push(nextFromPlaylistFile(musicFile));
    if (isPlayableTrack(show.mid)) {
        colaInterna.push({ ...show.mid, trusted: true });
    }

    // 6. Fragmento 3/3 (Borrar archivo temporal al terminar)
    colaInterna.push({
        title: `${chosen.title} [Parte 3/3]`,
        source: podFile,
        trusted: true,
        keepTempFile: false,
        forceCleanupTemp: true,
        seekSec: tercio * 2,
        durationSec: 0, // hasta el final del archivo
    });
}

async function getNextTrack() {
    const prog = loadProgramacion();

    // ── 1. PRIORIDAD ABSOLUTA: Solicitudes de la Comunidad (MongoDB) ──
    if (peticionEnEspera) {
        const nextReq = peticionEnEspera;
        peticionEnEspera = null;
        return nextReq;
    }

    const request = await takeCommunityRequest();
    if (request) {
        if (!enBloquePeticiones) {
            // Es la PRIMERA solicitud de este bloque: suena cortinilla previa y guardamos la canción en espera
            enBloquePeticiones = true;
            const reqBumper = pickRequestBumper(prog);
            if (reqBumper) {
                log('Cerebro', 'Iniciando bloque de peticiones con cortinilla de la comunidad.');
                peticionEnEspera = request;
                return reqBumper;
            }
        }
        // Si ya estábamos en bloque de peticiones (o no hay cortinilla configurada), suena directo
        return request;
    } else {
        // La cola de MongoDB está vacía: rearmamos la bandera para la próxima racha
        if (enBloquePeticiones) {
            log('Cerebro', 'Cola de peticiones de la comunidad vacía. Retomando programación.');
            enBloquePeticiones = false;
        }
    }

    // ── 2. COLA DE SECUENCIAS EN CURSO (Cortes comerciales o bloques de Podcast) ──
    if (colaInterna.length > 0) {
        return colaInterna.shift();
    }

    // ── 3. VERIFICACIÓN DE HORARIO Y CAMBIO DE SHOW (CDMX GMT-6) ──
    const activeShow = getActiveShow(prog);

    if (activeShow) {
        if (activeShow.id !== currentShowId) {
            log('Horario CDMX', `[${activeShow.timeStr}] Cambiando al bloque: "${activeShow.nombre}"`);
            currentShowId = activeShow.id;
            songsSinceBumper = 0;

            // Si el nuevo bloque es de tipo musical y tiene cortinilla de apertura, la encolamos
            if (activeShow.tipo !== 'podcast' && isPlayableTrack(activeShow.intro)) {
                colaInterna.push({ ...activeShow.intro, trusted: true });
            }
        }

        // Bloque de Podcast que aún no se ha emitido en esta hora.
        // NUNCA se bloquea esperando la descarga: si no está lista, sigue la música y se reintenta en la siguiente pista.
        if (activeShow.tipo === 'podcast' && lastPodcastSlotKey !== activeShow.slotKey) {
            const prep = ensurePodcastPrep(activeShow); // normalmente ya existe (descargado en segundo plano)

            if (prep.status === 'ready') {
                lastPodcastSlotKey = activeShow.slotKey;
                prep.consumed = true;
                buildPodcastSequence(activeShow, prep.episode);
            } else if (prep.status === 'failed') {
                lastPodcastSlotKey = activeShow.slotKey;
                log('Podcast', 'No se pudo preparar el episodio. Este bloque continúa con música.');
            } else if (!prep.announcedWait) {
                prep.announcedWait = true;
                log('Podcast', 'El episodio aún se está descargando. Música mientras tanto; entrará en cuanto esté listo.');
            }
        }
    }

    // Intro de show recién cambiado o escaleta de podcast recién armada: se reproducen primero
    if (colaInterna.length > 0) {
        return colaInterna.shift();
    }

    // ── 4. CICLO DE CADA 4 CANCIONES: [Cortinilla Estándar] -> [Comercial] -> [Cortinilla del Show] ──
    if (songsSinceBumper >= CONFIG.bumperEvery) {
        songsSinceBumper = 0;

        const stdBumper = pickRandomGeneralBumper();
        const commercial = pickCommercial(prog);
        const showMid = (activeShow && isPlayableTrack(activeShow.mid))
            ? { ...activeShow.mid, trusted: true }
            : null;

        if (stdBumper) colaInterna.push(stdBumper);
        if (commercial) colaInterna.push(commercial);
        if (showMid) colaInterna.push(showMid);

        if (colaInterna.length > 0) {
            log('Cerebro', `Insertando corte de estación (${colaInterna.length} elementos).`);
            return colaInterna.shift();
        }
    }

    // ── 5. CANCIÓN DEL SHOW ACTIVO (O BÓVEDA GENERAL) ──
    songsSinceBumper++;
    if (activeShow) {
        const targetPlaylist = activeShow.tipo === 'podcast'
            ? (activeShow.playlist_musica || 'playlist.json')
            : (activeShow.playlist || 'playlist.json');
        return nextFromPlaylistFile(targetPlaylist);
    }

    return nextFromPlaylistFile('playlist.json');
}

// ───────────────────────── RESOLUCIÓN DE FUENTES Y DECODER ─────────────────────────
// Devuelve { path, temp, keepTempFile } | { skipped:true } (descartada por duración) | null (fallo)
async function resolveSource(trk) {
    const src = trk.source;
    const isUrl = /^https?:\/\//i.test(src);

    if (!isUrl) {
        if (!trk.trusted) return null; // una petición nunca puede apuntar a archivos locales
        // Las rutas relativas (ej. "assets/spot1.mp3") se resuelven desde la carpeta del proyecto
        const localPath = path.isAbsolute(src) ? src : path.join(__dirname, src);
        if (!fs.existsSync(localPath)) {
            log('Cerebro', `Archivo local no encontrado: ${localPath}`);
            return null;
        }
        return {
            path: localPath,
            temp: Boolean(trk.forceCleanupTemp),
            keepTempFile: Boolean(trk.keepTempFile),
        };
    }
    if (!trk.trusted && !isAllowedUrl(src)) return null;

    const outFile = path.join(TEMP_DIR, `track-${process.pid}-${Date.now()}-${++downloadSeq}.m4a`);

    // Solo las canciones llevan filtro (máximo 12 min); cortinillas, spots y podcasts pasan siempre
    const dl = await downloadAudio(src, outFile, trk.isSong ? { maxSec: CONFIG.maxSongSec } : {});
    if (dl.filtered) {
        log('Filtro Tiempo', `Descartando "${trk.title}": dura más de ${CONFIG.maxSongSec / 60} min.`);
        return { skipped: true };
    }
    if (!dl.ok) return null;
    return { path: outFile, temp: true, keepTempFile: false };
}

function prepareDecoder(file, title, seekSec = 0, durationSec = 0) {
    const args = ['-nostdin', '-hide_banner', '-nostats', '-loglevel', 'info'];
    if (seekSec > 0) args.push('-ss', String(seekSec));
    args.push('-i', file);
    if (durationSec > 0) args.push('-t', String(durationSec));
    args.push('-vn', '-sn', '-dn');
    if (CONFIG.loudnorm) args.push('-af', 'loudnorm=I=-16:LRA=11:TP=-1.5');
    args.push('-f', 's16le', '-acodec', 'pcm_s16le', '-ar', String(CONFIG.sampleRate), '-ac', '2', 'pipe:1');

    const proc = track(spawn(FFMPEG, args, { stdio: ['ignore', 'pipe', 'pipe'] }));
    const dec = {
        proc,
        title,
        durationMs: durationSec > 0 ? durationSec * 1000 : 0,
        exited: false,
        tail: '',
    };

    let header = '';
    proc.stderr.on('data', (chunk) => {
        const s = chunk.toString();
        dec.tail = (dec.tail + s).slice(-400);
        if (!dec.durationMs && header.length < 8000) {
            header += s;
            const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(header);
            if (m) {
                const totalMs = (Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3])) * 1000;
                dec.durationMs = seekSec > 0 ? Math.max(0, totalMs - seekSec * 1000) : totalMs;
            }
        }
    });
    proc.on('close', () => { dec.exited = true; });
    proc.on('error', (e) => { dec.exited = true; dec.tail = e.message; });
    return dec;
}

// ───────────────────────── ENCODER (único, persistente) ─────────────────────────
const ICECAST_URL =
    `icecast://source:${encodeURIComponent(env.ZENO_PASSWORD)}@${env.ZENO_SERVER}:${env.ZENO_PORT}` +
    `${env.ZENO_MOUNT.startsWith('/') ? '' : '/'}${env.ZENO_MOUNT}`;

const ENCODER_ARGS = [
    '-hide_banner', '-nostats', '-loglevel', 'warning',
    '-re',
    '-f', 's16le', '-ar', String(CONFIG.sampleRate), '-ac', '2', '-i', 'pipe:0',
    '-c:a', 'libmp3lame', '-b:a', CONFIG.bitrate,
    '-write_xing', '0',
    '-ice_name', 'Marina Gaming Radio',
    '-content_type', 'audio/mpeg',
    '-f', 'mp3', ICECAST_URL,
];

let encoder = null;
let restartDelay = 1000;
let lastEncoderLog = 0;

function startEncoder() {
    const proc = track(spawn(FFMPEG, ENCODER_ARGS, { stdio: ['pipe', 'ignore', 'pipe'] }));
    const enc = { proc, startedAt: Date.now(), dead: false };

    proc.stdin.on('error', () => {});
    proc.stderr.on('data', (d) => {
        const now = Date.now();
        if (now - lastEncoderLog > 2000) {
            lastEncoderLog = now;
            log('Encoder', d.toString().trim().split('\n')[0].slice(0, 200));
        }
    });
    proc.on('exit', (code, sig) => { enc.dead = true; log('Encoder', `Terminó (código ${code}, señal ${sig}).`); });
    proc.on('error', (e) => { enc.dead = true; log('Encoder', `Error: ${e.message}`); });

    encoder = enc;
    log('Encoder', 'Conectando a ZenoFM...');
}

async function restartEncoder() {
    if (encoder) {
        try { encoder.proc.kill('SIGKILL'); } catch {}
        restartDelay = Date.now() - encoder.startedAt > 60000 ? 1000 : Math.min(restartDelay * 2, 15000);
        encoder = null;
        log('Encoder', `Reintentando en ${restartDelay / 1000}s...`);
        await sleep(restartDelay);
    }
    if (!shuttingDown) startEncoder();
}

function rawWrite(enc, chunk) {
    return new Promise((resolve, reject) => {
        if (enc.dead || !enc.proc.stdin.writable) return reject(new Error('encoder no disponible'));

        let done = false;
        const finish = (err) => {
            if (done) return;
            done = true;
            clearTimeout(timer);
            enc.proc.off('exit', onExit);
            err ? reject(err) : resolve();
        };
        const onExit = () => finish(new Error('encoder terminó'));
        const timer = setTimeout(() => {
            log('Watchdog', `El encoder no acepta datos hace ${CONFIG.writeWatchdogMs / 1000}s. Reiniciando...`);
            try { enc.proc.kill('SIGKILL'); } catch {}
            finish(new Error('watchdog timeout'));
        }, CONFIG.writeWatchdogMs);

        enc.proc.once('exit', onExit);
        enc.proc.stdin.write(chunk, (err) => finish(err || undefined));
    });
}

async function encoderWrite(chunk) {
    for (;;) {
        if (shuttingDown) throw new Error('apagando');
        if (!encoder || encoder.dead) await restartEncoder();
        if (!encoder) continue;
        try {
            await rawWrite(encoder, chunk);
            return;
        } catch (e) {
            if (encoder) { encoder.dead = true; }
        }
    }
}

// ───────────────────────── REPRODUCCIÓN ─────────────────────────
const SILENCE = Buffer.alloc(Math.floor(CONFIG.sampleRate * 4 * 0.25));
const MIN_BYTES = CONFIG.sampleRate * 4 * 0.5;

async function playDecoder(dec, onFirstChunk) {
    let bytes = 0;
    let stallTimer = null;
    let stalled = false;
    const arm = () => {
        clearTimeout(stallTimer);
        stallTimer = setTimeout(() => {
            stalled = true;
            log('Watchdog', 'El decoder se congeló. Descartando pista...');
            try { dec.proc.kill('SIGKILL'); } catch {}
        }, CONFIG.decoderStallMs);
    };

    arm();
    try {
        for await (const chunk of dec.proc.stdout) {
            clearTimeout(stallTimer);
            if (bytes === 0 && onFirstChunk) onFirstChunk();
            bytes += chunk.length;
            await encoderWrite(chunk);
            arm();
        }
    } finally {
        clearTimeout(stallTimer);
        if (!dec.exited) { try { dec.proc.kill('SIGKILL'); } catch {} }
    }

    if (bytes < MIN_BYTES) throw new Error(`pista vacía o corrupta (${dec.tail.split('\n').pop().trim().slice(0, 120)})`);
    if (stalled) throw new Error('decoder congelado (pista cortada)');
}

function disposeItem(item) {
    if (!item) return;
    try { if (!item.dec.exited) item.dec.proc.kill('SIGKILL'); } catch {}
    if (item.temp && !item.keepTempFile) {
        // Las canciones que sonaron completas se guardan como respaldo; el resto se borra
        if (item.played && item.track.isSong && fs.existsSync(item.path)) saveToCache(item);
        else cleanupPrefix(item.path);
    }
}

async function makeItem(trk, src) {
    return {
        track: trk,
        path: src.path,
        temp: src.temp,
        keepTempFile: src.keepTempFile,
        dec: prepareDecoder(src.path, trk.title, trk.seekSec || 0, trk.durationSec || 0),
    };
}

async function acquireTrack() {
    let failures = 0;
    while (!shuttingDown) {
        let trk;
        try {
            trk = await getNextTrack();
        } catch (e) {
            log('Cerebro', `Error eligiendo pista: ${e.message}`);
            await sleep(2000);
            continue;
        }

        const src = await resolveSource(trk);
        if (src && src.skipped) continue; // descartada por duración: no cuenta como fallo
        if (src) return makeItem(trk, src);

        failures++;
        log('Cerebro', `No se pudo preparar "${trk.title}" (fallo consecutivo ${failures}).`);

        if (failures >= 2) {
            // 1º: canciones reales guardadas en caché (la radio sigue sonando aunque YouTube nos bloquee)
            const cached = cachedSongs();
            if (cached.length > 0) {
                const cachedPath = cached[Math.floor(Math.random() * cached.length)];
                const cachedTitle = path.basename(cachedPath, '.m4a').split('__')[1] || 'Canción en caché';
                log('Modo Resguardo', `YouTube no responde. Reproduciendo desde caché local: ${cachedTitle}`);
                return makeItem(
                    { title: `[Caché] ${cachedTitle}`, source: cachedPath, trusted: true },
                    { path: cachedPath, temp: false, keepTempFile: false }
                );
            }
            // 2º: cortinillas locales
            const locals = localBumpers();
            if (locals.length > 0) {
                const pick = locals[Math.floor(Math.random() * locals.length)];
                log('Modo Resguardo', `Transmitiendo cortinilla local: ${pick.title}`);
                return makeItem(pick, { path: pick.source, temp: false, keepTempFile: false });
            }
            await sleep(Math.min(30000, 5000 * (failures - 1)));
        }
    }
    throw new Error('apagando');
}

async function waitWithSilence(promise) {
    let settled = false;
    promise.then(() => { settled = true; }, () => { settled = true; });
    let announced = false;
    while (!settled) {
        if (!announced) { announced = true; log('Radio', 'Siguiente pista aún no lista. Rellenando con silencio...'); }
        await encoderWrite(SILENCE);
    }
    return promise;
}

// ───────────────────────── BUCLE PRINCIPAL ─────────────────────────
async function runRadio() {
    let nextPromise = acquireTrack();
    nextPromise.catch(() => {});

    for (;;) {
        const startedAt = Date.now();
        const current = await waitWithSilence(nextPromise);
        nextPromise = null;

        let prefetchTimer = null;
        const startPrefetch = () => {
            if (!nextPromise && !shuttingDown) {
                nextPromise = acquireTrack();
                nextPromise.catch(() => {});
            }
        };

        log('AutoDJ', `▶ [${currentShowId || 'general'}] ${current.track.title}`);
        try {
            await playDecoder(current.dec, () => {
                const dur = current.dec.durationMs;
                const wait = dur ? Math.max(0, dur - CONFIG.prefetchLeadMs) : 20000;
                prefetchTimer = setTimeout(startPrefetch, wait);
            });
            current.played = true;
            log('AutoDJ', 'Pista terminada.');
        } catch (e) {
            log('AutoDJ', `Error en la pista: ${e.message}`);
        } finally {
            clearTimeout(prefetchTimer);
            disposeItem(current);
        }

        startPrefetch();

        if (Date.now() - startedAt < 3000) await sleep(2000);
    }
}

// ───────────────────────── APAGADO Y ROBUSTEZ ─────────────────────────
function wipeTemp() {
    try {
        for (const f of fs.readdirSync(TEMP_DIR)) { try { fs.unlinkSync(path.join(TEMP_DIR, f)); } catch {} }
    } catch {}
}

async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    log('Sistema', `${signal} recibido. Deteniendo AutoDJ de Marina Gaming Radio...`);
    for (const p of children) { try { p.kill('SIGKILL'); } catch {} }
    try { if (mongo) await mongo.close(); } catch {}
    wipeTemp();
    log('Sistema', 'Transmisión finalizada. ¡Hasta pronto, ingeniero!');
    process.exit(0);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

process.on('uncaughtException', (e) => log('Error no capturado', e.stack || e.message));
process.on('unhandledRejection', (e) => log('Promesa rechazada', (e && e.stack) || String(e)));

// ───────────────────────── ARRANQUE ─────────────────────────
console.log('--- INICIALIZANDO AUTO-DJ MARINA GAMING RADIO v2.1 ---');
fs.mkdirSync(TEMP_DIR, { recursive: true });
fs.mkdirSync(path.join(__dirname, 'playlists'), { recursive: true });
wipeTemp();
fs.mkdirSync(CACHE_DIR, { recursive: true });

if (!env.YTDLP_COOKIES) log('Cookies', 'YTDLP_COOKIES no definida: yt-dlp trabajará sin sesión (YouTube suele bloquear IPs de datacenter).');
else if (!fs.existsSync(env.YTDLP_COOKIES)) log('Cookies', `⚠ El archivo de cookies no existe: ${env.YTDLP_COOKIES}`);
else log('Cookies', `Usando ${env.YTDLP_COOKIES}`);
log('Caché', `${cachedSongs().length} canciones de respaldo en disco (máx. ${CONFIG.cacheMaxFiles}).`);

// Revisión en segundo plano: descarga el podcast con antelación (cada 30 s)
podcastBackgroundTick();
setInterval(podcastBackgroundTick, 30000).unref();

(async () => {
    for (;;) {
        try {
            await runRadio();
        } catch (e) {
            if (shuttingDown) return;
            log('Fatal', `El bucle principal falló: ${e.message}. Reiniciando en 5s...`);
            await sleep(5000);
        }
    }
})();