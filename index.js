'use strict';
/**
 * MARINA GAMING RADIO - AutoDJ v2.0 (Parrilla Horaria CDMX + Podcasts en 3 Partes + Prioridad MongoDB)
 *
 * Arquitectura:
 *   [yt-dlp] -> temp/track-N.m4a -> [ffmpeg DECODER] --PCM--> [ffmpeg ENCODER] -> Icecast/ZenoFM
 *                (uno por pista / segmento)                     (UNO, vive siempre)
 *
 * Nuevas Funciones:
 *  - Reloj en tiempo real ajustado a CDMX (GMT-6) leyendo programacion.json.
 *  - Peticiones de MongoDB con prioridad absoluta: reproduce cortinilla de petición SOLO
 *    antes de la primera solicitud de una racha; se rearma cuando la cola de MongoDB se vacía.
 *  - Ciclo de shows musicales: Intro al cambiar de programa, y cada 4 canciones inserta:
 *    [Cortinilla Estándar] -> [Comercial] -> [Cortinilla Mid del Show].
 *  - Modo Podcast (Checkpoint Gaming): Descarga un episodio aleatorio, mide su duración exacta
 *    y lo divide virtualmente en 3 partes iguales (sin recodificar en disco), intercalando
 *    Cortinilla Estándar + Canción + Cortinilla del Show entre cada parte.
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
    try {
        const res = spawnSync(FFMPEG, ['-hide_banner', '-i', filePath], { encoding: 'utf8', timeout: 10000 });
        const output = (res.stderr || '') + (res.stdout || '');
        const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(output);
        if (m) {
            return Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]);
        }
    } catch (e) {
        log('Probe', `No se pudo medir duración de ${path.basename(filePath)}: ${e.message}`);
    }
    return 0;
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
    return { ...tracks[state.lastIndex], trusted: true };
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
function getCdmxTimeInfo() {
    // CDMX es permanentemente UTC-6
    const now = new Date();
    const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
    const cdmxDate = new Date(utcMs - (6 * 3600000));

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
                return { title: String(doc.title || 'Petición de la comunidad'), source: doc.source, trusted: false, community: true };
            }
            log('Cerebro', `Petición inválida descartada: ${String(doc.source).slice(0, 80)}`);
        }
    } catch (e) {
        log('BD', `Error: ${e.message}. Usando programación normal.`);
    }
    return null;
}

// ───────────────────────── DESCARGA (ESTRATEGIA HIDRA) ─────────────────────────
const HYDRA_CLIENTS = [null, 'android_vr', 'android', 'ios', 'web'];
let preferredClient = null;
const PERMANENT_ERRORS = /private video|no longer available|terminated|video unavailable|removed|copyright/i;
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

function lastLine(err) {
    const text = String(err && (err.stderr || err.message) || err);
    const lines = text.split('\n').map((s) => s.trim()).filter(Boolean);
    return (lines[lines.length - 1] || '').slice(0, 160);
}

async function downloadAudio(url, outFile) {
    const isYoutube = /youtube\.com|youtu\.be/i.test(url);
    const clients = isYoutube ? HYDRA_CLIENTS : [null];

    for (const client of clients) {
        if (shuttingDown) return false;
        log('yt-dlp', `Descargando${client ? ` (cliente ${client})` : ''}...`);
        try {
            const flags = {
                format: 'ba/b',
                output: outFile,
                noPlaylist: true,
                noPart: true,
                noMtime: true,
                socketTimeout: 10,
                retries: 3,
                jsRuntimes: 'node',
            };
            if (client) flags.extractorArgs = `youtube:player_client=${client}`;
            if (env.YTDLP_COOKIES) flags.cookies = env.YTDLP_COOKIES;

            await exec(url, flags, { timeout: CONFIG.downloadTimeoutMs, killSignal: 'SIGKILL' });

            if (fs.existsSync(outFile) && fs.statSync(outFile).size > 10000) return true;
            throw new Error('archivo descargado vacío');
        } catch (e) {
            log('yt-dlp', `${client || 'default'} falló: ${lastLine(e)}`);
            cleanupPrefix(outFile);
            if (PERMANENT_ERRORS.test(String(e.stderr || e.message))) break; // video muerto: no insistir
        }
    }
    return false;
}

// ───────────────────────── EL CEREBRO (Horarios, Bloques y Podcasts) ─────────────────────────
let songsSinceBumper = 0;
let currentShowId = null;
let lastPodcastSlotKey = null;
let enBloquePeticiones = false;
let peticionEnEspera = null;
const colaInterna = []; // Cola FIFO para secuencias (Intro, Cortinilla+Comercial+Mid, o los 10 pasos del Podcast)

// Descarga un episodio de podcast, calcula sus 3 tercios y arma la escaleta en colaInterna
async function buildPodcastSequence(show) {
    const podPlaylistPath = path.join(__dirname, show.playlist_podcasts || 'playlists/checkpoint_podcasts.json');
    const podTracks = loadTracks(podPlaylistPath);

    if (podTracks.length === 0) {
        log('Podcast', 'No hay episodios válidos en checkpoint_podcasts.json. Usando música general.');
        return false;
    }

    const chosen = podTracks[Math.floor(Math.random() * podTracks.length)];
    const podFile = path.join(TEMP_DIR, `podcast-${process.pid}-${Date.now()}-${++downloadSeq}.m4a`);

    log('Podcast', `Descargando episodio: "${chosen.title}"...`);
    const ok = await downloadAudio(chosen.source, podFile, CONFIG.podcastDownloadTimeoutMs);
    if (!ok) {
        log('Podcast', `Falló la descarga del episodio "${chosen.title}". Pasando a música general.`);
        return false;
    }

    const totalSec = probeDurationSec(podFile);
    if (totalSec < 60) {
        log('Podcast', `Duración inválida (${totalSec}s). Descartando archivo de podcast.`);
        cleanupPrefix(podFile);
        return false;
    }

    const tercio = Math.floor(totalSec / 3);
    log('Podcast', `Episodio listo (${Math.round(totalSec / 60)} min). Dividiendo en 3 bloques de ~${Math.round(tercio / 60)} min.`);

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

    return true;
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

        // Si estamos en el bloque de Podcast y aún no se ha emitido en esta hora
        if (activeShow.tipo === 'podcast' && lastPodcastSlotKey !== activeShow.slotKey) {
            lastPodcastSlotKey = activeShow.slotKey;
            const built = await buildPodcastSequence(activeShow);
            if (built && colaInterna.length > 0) {
                return colaInterna.shift();
            }
        }
    }

    // Si se agregó una intro de show recién cambiado a colaInterna, la reproducimos primero
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
async function resolveSource(trk) {
    const src = trk.source;
    const isUrl = /^https?:\/\//i.test(src);

    if (!isUrl) {
        if (!trk.trusted) return null;
        if (!fs.existsSync(src)) return null;
        return {
            path: src,
            temp: Boolean(trk.forceCleanupTemp),
            keepTempFile: Boolean(trk.keepTempFile),
        };
    }
    if (!trk.trusted && !isAllowedUrl(src)) return null;

    const outFile = path.join(TEMP_DIR, `track-${process.pid}-${Date.now()}-${++downloadSeq}.m4a`);
    const ok = await downloadAudio(src, outFile);
    if (!ok) return null;

        // CANDADO DE DURACIÓN EN VIVO (Solo aplica a canciones normales, NO a podcasts ni cortinillas)
        const durSec = probeDurationSec(outFile);
        if (durSec > 0 && (durSec < 40 || durSec > 720)) {
            log('Filtro Tiempo', `Descartando "${trk.title}" por duración fuera de rango (${Math.round(durSec)}s).`);
            cleanupPrefix(outFile);
            return null; // El AutoDJ salta inmediatamente a otra canción sin transmitirla
        }

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
        cleanupPrefix(item.path);
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
        if (src) return makeItem(trk, src);

        failures++;
        log('Cerebro', `No se pudo preparar "${trk.title}" (fallo consecutivo ${failures}).`);

        if (failures >= 2) {
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
console.log('--- INICIALIZANDO AUTO-DJ MARINA GAMING RADIO v2.0 ---');
fs.mkdirSync(TEMP_DIR, { recursive: true });
fs.mkdirSync(path.join(__dirname, 'playlists'), { recursive: true });
wipeTemp();

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