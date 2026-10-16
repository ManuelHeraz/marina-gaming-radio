'use strict';
/**
 * MARINA GAMING RADIO - AutoDJ (versión estable / sin cortes)
 *
 * Arquitectura:
 *
 *   [yt-dlp] -> temp/track-N.m4a -> [ffmpeg DECODER] --PCM--> [ffmpeg ENCODER] -> Icecast/ZenoFM
 *                (uno por pista)                              (UNO, vive siempre)
 *
 *  - El ENCODER mantiene UNA sola conexión a Icecast. Entre canciones no hay
 *    desconexión, así que los oyentes no sufren cortes ni rebuffering.
 *  - La SIGUIENTE canción se descarga y se pre-decodifica mientras suena la actual
 *    (gapless). Si algo tarda de más, se rellena con silencio en vez de cortar.
 *  - Si Icecast/red se congela, un watchdog reinicia el encoder y la canción
 *    actual continúa donde iba.
 *  - Las peticiones de MongoDB (Discord) conservan prioridad absoluta.
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
    bumperEvery: 4,                                              // cortinilla cada N canciones del respaldo
    prefetchLeadMs: (Number(env.PREFETCH_LEAD_SECONDS) || 45) * 1000, // cuánto antes de terminar se prepara la siguiente
    writeWatchdogMs: 20000,                                      // encoder sin aceptar datos => reinicio
    decoderStallMs: 20000,                                       // decoder sin producir datos => se descarta la pista
    downloadTimeoutMs: 90000,                                    // máximo por intento de yt-dlp
    loudnorm: env.LOUDNORM !== '0',                              // LOUDNORM=0 desactiva la normalización (ahorra CPU)
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
const children = new Set(); // procesos hijos vivos, para matarlos al cerrar

function track(proc) {
    children.add(proc);
    proc.once('close', () => children.delete(proc));
    return proc;
}

// ───────────────────────── CARGA DE LISTAS ─────────────────────────
function loadTracks(file) {
    try {
        const raw = fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, '');
        const data = JSON.parse(raw);
        const list = Array.isArray(data.tracks) ? data.tracks : [];
        return list.filter((t) => t && typeof t.title === 'string' && typeof t.source === 'string' && t.source);
    } catch (e) {
        log('Cerebro', `No se pudo leer ${path.basename(file)}: ${e.message}`);
        return [];
    }
}

const library = loadTracks(PLAYLIST_FILE);
if (library.length === 0) {
    console.error('[Fatal] playlist.json está vacío o es inválido. Nada que transmitir.');
    process.exit(1);
}
log('Sistema', `Bóveda cargada: ${library.length} pistas.`);

function localBumpers() {
    if (!fs.existsSync(ASSETS_DIR)) return [];
    return fs.readdirSync(ASSETS_DIR)
        .filter((f) => f.toLowerCase().endsWith('.mp3'))
        .map((f) => ({ title: `Marina Gaming - ${f.replace(/\.mp3$/i, '')}`, source: path.join(ASSETS_DIR, f), trusted: true }));
}

function allBumpers() {
    const list = localBumpers();
    if (fs.existsSync(BUMPERS_FILE)) {
        for (const t of loadTracks(BUMPERS_FILE)) list.push({ ...t, trusted: true });
    }
    return list;
}

// Baraja sin repeticiones: no se repite ninguna pista hasta recorrer toda la bóveda
let deck = [];
let lastIndex = -1;
function nextFromLibrary() {
    if (deck.length === 0) {
        deck = library.map((_, i) => i);
        for (let i = deck.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [deck[i], deck[j]] = [deck[j], deck[i]];
        }
        if (deck.length > 1 && deck[deck.length - 1] === lastIndex) {
            [deck[0], deck[deck.length - 1]] = [deck[deck.length - 1], deck[0]];
        }
    }
    lastIndex = deck.pop();
    return { ...library[lastIndex], trusted: true };
}

// ───────────────────────── MONGODB (peticiones de la comunidad) ─────────────────────────
let mongo = null;
function getCollection() {
    if (!env.MONGODB_URI) return null;
    if (!mongo) {
        mongo = new MongoClient(env.MONGODB_URI, {
            serverSelectionTimeoutMS: 4000, // si la BD cae, NO bloquea la radio 30s
            connectTimeoutMS: 4000,
            socketTimeoutMS: 10000,
            maxPoolSize: 2,
        });
    }
    return mongo.db(CONFIG.dbName).collection(CONFIG.dbCollection);
}

// Las peticiones vienen de usuarios: solo se aceptan URLs de YouTube/SoundCloud.
// (Evita que alguien meta una ruta local o una URL interna.)
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
                log('Cerebro', 'Petición de la comunidad encontrada. Prioridad máxima.');
                return { title: String(doc.title || 'Petición de la comunidad'), source: doc.source, trusted: false, community: true };
            }
            log('Cerebro', `Petición inválida descartada: ${String(doc.source).slice(0, 80)}`);
        }
    } catch (e) {
        log('BD', `Error: ${e.message}. Usando respaldo.`);
    }
    return null;
}

// ───────────────────────── EL CEREBRO ─────────────────────────
let songsSinceBumper = 0;

async function getNextTrack() {
    // 1. Prioridad absoluta: comunidad
    const request = await takeCommunityRequest();
    if (request) return request;

    // 2. ¿Toca cortinilla?
    if (songsSinceBumper >= CONFIG.bumperEvery) {
        songsSinceBumper = 0;
        const bumpers = allBumpers();
        if (bumpers.length > 0) {
            log('Cerebro', `Insertando cortinilla aleatoria (${bumpers.length} disponibles).`);
            return bumpers[Math.floor(Math.random() * bumpers.length)];
        }
        log('Cerebro', 'No hay cortinillas disponibles. Saltando...');
    }

    // 3. Bóveda aleatoria
    songsSinceBumper++;
    return nextFromLibrary();
}

// ───────────────────────── DESCARGA (ESTRATEGIA HIDRA) ─────────────────────────
const HYDRA_CLIENTS = ['android', 'ios', 'web'];
let preferredClient = 'android'; // el último cliente que funcionó se prueba primero
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
    const clients = isYoutube
        ? [preferredClient, ...HYDRA_CLIENTS.filter((c) => c !== preferredClient)]
        : [null];

    for (const client of clients) {
        if (shuttingDown) return false;
        log('yt-dlp', `Descargando${client ? ` (cliente ${client})` : ''}...`);
        try {
            const flags = {
                format: 'bestaudio/best[height<=480]/best',
                output: outFile,
                noPlaylist: true,
                noPart: true,
                noMtime: true,
                noWarnings: true,
                socketTimeout: 10,
                retries: 3,
            };
            if (client) flags.extractorArgs = `youtube:player_client=${client}`;
            if (env.YTDLP_COOKIES) flags.cookies = env.YTDLP_COOKIES;

            await exec(url, flags, { timeout: CONFIG.downloadTimeoutMs, killSignal: 'SIGKILL' });

            if (fs.existsSync(outFile) && fs.statSync(outFile).size > 10000) {
                if (client) preferredClient = client;
                return true;
            }
            throw new Error('archivo descargado vacío');
        } catch (e) {
            log('yt-dlp', `${client || 'descarga'} falló: ${lastLine(e)}`);
            cleanupPrefix(outFile);
        }
    }
    return false;
}

// Devuelve { path, temp } o null si la pista no se pudo preparar
async function resolveSource(trk) {
    const src = trk.source;
    const isUrl = /^https?:\/\//i.test(src);

    if (!isUrl) {
        if (!trk.trusted) return null; // una petición nunca puede apuntar a archivos locales
        return fs.existsSync(src) ? { path: src, temp: false } : null;
    }
    if (!trk.trusted && !isAllowedUrl(src)) return null;

    const outFile = path.join(TEMP_DIR, `track-${process.pid}-${Date.now()}-${++downloadSeq}.m4a`);
    const ok = await downloadAudio(src, outFile);
    return ok ? { path: outFile, temp: true } : null;
}

// ───────────────────────── DECODER (uno por pista) ─────────────────────────
function prepareDecoder(file, title) {
    const args = ['-nostdin', '-hide_banner', '-nostats', '-loglevel', 'info', '-i', file, '-vn', '-sn', '-dn'];
    if (CONFIG.loudnorm) args.push('-af', 'loudnorm=I=-16:LRA=11:TP=-1.5');
    args.push('-f', 's16le', '-acodec', 'pcm_s16le', '-ar', String(CONFIG.sampleRate), '-ac', '2', 'pipe:1');

    const proc = track(spawn(FFMPEG, args, { stdio: ['ignore', 'pipe', 'pipe'] }));
    const dec = { proc, title, durationMs: 0, exited: false, tail: '' };

    let header = '';
    proc.stderr.on('data', (chunk) => {
        const s = chunk.toString();
        dec.tail = (dec.tail + s).slice(-400);
        if (!dec.durationMs && header.length < 8000) {
            header += s;
            const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(header);
            if (m) dec.durationMs = (Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3])) * 1000;
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
    '-re',                                    // el encoder marca el ritmo real (1x); los decoders van por backpressure
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

    proc.stdin.on('error', () => {}); // los errores llegan por el callback de write()
    proc.stderr.on('data', (d) => {
        const now = Date.now();
        if (now - lastEncoderLog > 2000) { // evita inundar el log
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
        // si vivió estable más de 60s, el backoff se reinicia
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
            log('Watchdog', `El encoder no acepta datos hace ${CONFIG.writeWatchdogMs / 1000}s (red caída/congelada). Reiniciando...`);
            try { enc.proc.kill('SIGKILL'); } catch {}
            finish(new Error('watchdog timeout'));
        }, CONFIG.writeWatchdogMs);

        enc.proc.once('exit', onExit);
        enc.proc.stdin.write(chunk, (err) => finish(err || undefined));
    });
}

// Escribe PCM al encoder; si el encoder muere, lo reinicia y reintenta el MISMO fragmento
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
            // el siguiente ciclo del for reinicia el encoder
        }
    }
}

// ───────────────────────── REPRODUCCIÓN ─────────────────────────
const SILENCE = Buffer.alloc(Math.floor(CONFIG.sampleRate * 4 * 0.25)); // 250 ms de silencio PCM
const MIN_BYTES = CONFIG.sampleRate * 4 * 0.5;                           // < 0.5s => pista corrupta

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
            clearTimeout(stallTimer); // mientras se espera al encoder (backpressure) no cuenta como congelado
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
    if (item.temp) {
        cleanupPrefix(item.path);
    }
}

async function makeItem(trk, src) {
    return { track: trk, path: src.path, temp: src.temp, dec: prepareDecoder(src.path, trk.title) };
}

// Obtiene la siguiente pista ya descargada y con su decoder precalentado
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
            // Modo resguardo: YouTube probablemente nos bloquea; sonamos con material local
            const locals = localBumpers();
            if (locals.length > 0) {
                const pick = locals[Math.floor(Math.random() * locals.length)];
                log('Modo Resguardo', `Transmitiendo cortinilla local: ${pick.title}`);
                return makeItem(pick, { path: pick.source, temp: false });
            }
            await sleep(Math.min(30000, 5000 * (failures - 1)));
        }
    }
    throw new Error('apagando');
}

// Espera una promesa rellenando con silencio para que el stream nunca se corte
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

        log('AutoDJ', `▶ ${current.track.title}`);
        try {
            await playDecoder(current.dec, () => {
                // Prepara la siguiente pista justo antes de que termine esta
                // (así una petición de Discord que llegue a tiempo entra de inmediato)
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

        startPrefetch(); // por si no se alcanzó a disparar (pista corta o fallida)

        // Freno de emergencia: si una pista falló al instante, evita un bucle frenético
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

// Una radio no debe morir por un error suelto
process.on('uncaughtException', (e) => log('Error no capturado', e.stack || e.message));
process.on('unhandledRejection', (e) => log('Promesa rechazada', (e && e.stack) || String(e)));

// ───────────────────────── ARRANQUE ─────────────────────────
console.log('--- INICIALIZANDO AUTO-DJ MARINA GAMING RADIO ---');
fs.mkdirSync(TEMP_DIR, { recursive: true });
wipeTemp(); // restos de ejecuciones anteriores que se cayeron

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