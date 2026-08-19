require('dotenv').config();
const { exec } = require('youtube-dl-exec');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const axios = require('axios');
const { MongoClient } = require('mongodb'); 
const http = require('http');

// Bóveda de música principal
const rawPlaylist = fs.readFileSync(path.join(__dirname, 'playlist.json'));
const playlistData = JSON.parse(rawPlaylist);

const tempDir = path.join(__dirname, 'temp');
if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
}

// Variables globales para el DJ
let tracksPlayedSinceBumper = 0;
const BUMPER_FREQUENCY = 4;

// --- GESTIÓN DE BASE DE DATOS MONGODB ---
let dbClient = null;
async function connectDB() {
    if (!process.env.MONGODB_URI) return null;
    
    if (!dbClient) {
        dbClient = new MongoClient(process.env.MONGODB_URI);
        await dbClient.connect();
        console.log('[Sistema] Conectado a MongoDB Atlas exitosamente.');
    }
    return dbClient.db('marina_radio');
}

// --- EL CEREBRO HÍBRIDO ---
async function getNextTrack() {
    // 1. Buscar en MongoDB
    if (process.env.MONGODB_URI) {
        try {
            const db = await connectDB();
            const collection = db.collection('cola_reproduccion');

            const result = await collection.findOneAndDelete(
                {}, 
                { sort: { _id: 1 } }
            );

            const track = result ? (result.value || result) : null;

            if (track && track.source) {
                console.log(`[Cerebro] Petición de la comunidad encontrada. Prioridad máxima.`);
                return track;
            }
        } catch (error) {
            console.error(`[Base de Datos Error]: ${error.message}`);
            console.log(`[Cerebro] Falló la BD. Activando sistema de respaldo...`);
        }
    }

    // 2. ¿Es hora de una cortinilla?
    if (tracksPlayedSinceBumper >= BUMPER_FREQUENCY) {
        tracksPlayedSinceBumper = 0; 
        
        const assetsDir = path.join(__dirname, 'assets');
        const bumpersPath = path.join(assetsDir, 'bumpers.json');
        
        let allBumpers = [];

        // A. Cargar cortinillas físicas (.mp3)
        if (fs.existsSync(assetsDir)) {
            const mp3Files = fs.readdirSync(assetsDir).filter(file => file.endsWith('.mp3'));
            const localBumpers = mp3Files.map(file => ({
                title: `Marina Gaming - ${file.replace('.mp3', '')}`,
                source: path.join(assetsDir, file)
            }));
            allBumpers = allBumpers.concat(localBumpers);
        }

        // B. Cargar cortinillas de YouTube (bumpers.json)
        if (fs.existsSync(bumpersPath)) {
            try {
                const rawBumpers = fs.readFileSync(bumpersPath, 'utf8');
                const bumpersData = JSON.parse(rawBumpers);
                
                if (bumpersData && bumpersData.tracks && Array.isArray(bumpersData.tracks)) {
                    allBumpers = allBumpers.concat(bumpersData.tracks);
                }
            } catch (error) {
                console.error(`[Cerebro] Error leyendo bumpers.json: ${error.message}`);
            }
        }

        // C. Revolver la bolsa y sacar una al azar
        if (allBumpers.length > 0) {
            console.log(`[Cerebro] Insertando cortinilla aleatoria... (${allBumpers.length} opciones disponibles en total)`);
            const randomIndex = Math.floor(Math.random() * allBumpers.length);
            return allBumpers[randomIndex];
        } else {
            console.log(`[Cerebro] No se encontraron cortinillas en absoluto. Saltando...`);
        }
    }

    // 3. Reproducción aleatoria de tu bóveda
    console.log(`[Cerebro] Cola vacía. Seleccionando pista aleatoria del respaldo.`);
    const randomIndex = Math.floor(Math.random() * playlistData.tracks.length);
    
    tracksPlayedSinceBumper++; 
    
    return playlistData.tracks[randomIndex];
}

// --- NÚCLEO DE TRANSMISIÓN ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function streamToZeno(filePath, trackTitle) {
    await sleep(500); 

    return new Promise((resolve, reject) => {
        const icecastUrl = `icecast://source:${process.env.ZENO_PASSWORD}@${process.env.ZENO_SERVER}:${process.env.ZENO_PORT}${process.env.ZENO_MOUNT}`;
        
        console.log(`[Shell Native] Transmitiendo a ZenoFM -> ${trackTitle}`);
        
        const cmd = `ffmpeg -itsoffset 1 -re -i "${filePath}" -af "loudnorm=I=-16:LRA=11:TP=-1.5" -c:a libmp3lame -b:a 128k -bufsize 64k -content_type audio/mpeg -f mp3 "${icecastUrl}"`;
        
        const ffmpegProcess = spawn(cmd, { shell: true });

        // --- INICIO DEL PERRO GUARDIÁN (WATCHDOG) ---
        let watchdogTimer;
        
        const resetWatchdog = () => {
            clearTimeout(watchdogTimer);
            // Si pasan 15 segundos sin que FFmpeg procese datos, lo matamos
            watchdogTimer = setTimeout(() => {
                console.error(`[Watchdog] FFmpeg se congeló en la red. Forzando reinicio de pista...`);
                ffmpegProcess.kill('SIGKILL'); // Tiro de gracia al proceso zombie
                reject(new Error('Watchdog Timeout: Conexión caída'));
            }, 15000);
        };

        // Activamos el guardián por primera vez
        resetWatchdog();

        // FFmpeg escupe su progreso constantemente por 'stderr'. 
        // Usaremos este latido para calmar al Perro Guardián.
        ffmpegProcess.stderr.on('data', (data) => {
            resetWatchdog(); 
            // Opcional: si quieres ver los logs de FFmpeg de nuevo, descomenta la siguiente línea:
            // console.log(`[FFmpeg]: ${data}`);
        });
        // --- FIN DEL PERRO GUARDIÁN ---

        ffmpegProcess.on('close', (code) => {
            clearTimeout(watchdogTimer); // Apagamos el guardián cuando la canción termina bien
            resolve();
        });

        ffmpegProcess.on('error', (err) => {
            clearTimeout(watchdogTimer);
            reject(new Error(`Fallo Shell Nativo: ${err.message}`));
        });
    });
}

// --- SISTEMA DE DESCARGA BLINDADO (ESTRATEGIA HIDRA) ---
async function descargarConEstrategiaHidra(audioSource, tempFilePath) {
    // Los clientes de la Hidra que ya probaste y sabes que funcionan
    const CLIENTS = ['android', 'ios', 'web'];
    
    for (const clientId of CLIENTS) {
        console.log(`[yt-dlp] Intentando descarga con cliente: ${clientId}...`);
        try {
            await exec(audioSource, {
                f: 'bestaudio/best[height<=480]/best',
                o: tempFilePath,
                'extractor-args': `youtube:player_client=${clientId}`,
                'socket-timeout': '10',
                'retries': '3',
                noWarnings: true
            });
            console.log(`[yt-dlp] ¡Descarga exitosa usando ${clientId}!`);
            return true; // Éxito total, rompemos el bucle
        } catch (error) {
            console.log(`[yt-dlp] Cliente ${clientId} rechazado por YouTube. Probando siguiente...`);
        }
    }
    return false; // Si los tres fallan, la Hidra muere temporalmente
}

// --- BUCLE PRINCIPAL ---

async function startStreaming() {
    const track = await getNextTrack();
    console.log(`\n[AutoDJ] Preparando: ${track.title}`);

    let audioSource = track.source;
    // Si la fuente incluye un enlace externo, sabemos que hay que descargarla primero
    const isExternalUrl = audioSource.includes('youtube.com') || audioSource.includes('youtu.be') || audioSource.includes('soundcloud.com');
    
    let tempFilePath = null;
    let startTime = Date.now();

    if (isExternalUrl) {
        tempFilePath = path.join(tempDir, `track-${Date.now()}.m4a`);
        console.log(`[yt-dlp] Iniciando protocolo Hidra para descarga efímera...`);
        
        let descargaExitosa = await descargarConEstrategiaHidra(audioSource, tempFilePath);
        
        // BUCLE DE RESGUARDO (Cortinillas en loop si YouTube se pone pesado)
        while (!descargaExitosa) {
            console.error(`[Alerta Radiodifusión] YouTube bloqueó el enlace. Entrando en modo de resguardo con cortinillas...`);
            
            // Seleccionamos una cortinilla de emergencia al azar para entretener a la audiencia
            const assetsDir = path.join(__dirname, 'assets');
            const bumpersPath = path.join(assetsDir, 'bumpers.json');
            let bumperAux = { title: "Marina Gaming - Bumper de Emergencia", source: path.join(assetsDir, "c1.mp3") }; // Respaldo duro
            
            if (fs.existsSync(bumpersPath)) {
                try {
                    const raw = fs.readFileSync(bumpersPath, 'utf8');
                    const parsed = JSON.parse(raw);
                    if (parsed && parsed.tracks && parsed.tracks.length > 0) {
                        bumperAux = parsed.tracks[Math.floor(Math.random() * parsed.tracks.length)];
                    }
                } catch(e) {}
            }

            console.log(`[Modo Resguardo] Transmitiendo cortinilla temporal: ${bumperAux.title}`);
            
            // Si la cortinilla es externa, la baja; si es local, la usa directo
            let sourceBumper = bumperAux.source;
            let tempBumperPath = null;
            if (sourceBumper.includes('youtube.com') || sourceBumper.includes('youtu.be')) {
                tempBumperPath = path.join(tempDir, `bumper-${Date.now()}.m4a`);
                await descargarConEstrategiaHidra(sourceBumper, tempBumperPath);
                sourceBumper = tempBumperPath;
            }

            // Transmitimos la cortinilla de relleno mientras la canción principal sigue intentando bajar
            try {
                await streamToZeno(sourceBumper, `[En espera de conexión] ${bumperAux.title}`);
            } catch(err) {}
            
            if (tempBumperPath) cleanup(tempBumperPath);

            // Reintentamos descargar la canción original de nuevo tras tocar la cortinilla
            console.log(`[yt-dlp] Reintentando descargar la canción pendiente...`);
            descargaExitosa = await descargarConEstrategiaHidra(audioSource, tempFilePath);
        }
        
        audioSource = tempFilePath;
    }

    try {
        await streamToZeno(audioSource, track.title);
        console.log(`[AutoDJ] Pista terminada.`);
    } catch (err) {
        console.error(`[Error de Transmisión]: ${err.message}`);
    } finally {
        cleanup(tempFilePath);
        retryWithCooldown(startTime);
    }
}

// --- UTILIDADES ---
function cleanup(filePath) {
    if (filePath && fs.existsSync(filePath)) {
        try {
            fs.unlinkSync(filePath);
            console.log(`[Sistema] Caché temporal limpiada.`);
        } catch (e) {
            console.error(`[Sistema] Error limpiando archivo: ${e.message}`);
        }
    }
}

function retryWithCooldown(startTime) {
    const elapsed = Date.now() - startTime;
    if (elapsed < 5000) {
        console.log(`[Sistema] Freno de emergencia activado. Esperando 5 segundos...`);
        setTimeout(startStreaming, 5000);
    } else {
        startStreaming();
    }
}

process.on('SIGINT', () => {
    console.log('\n[Sistema] Deteniendo AutoDJ de Marina Gaming Radio...');
    if (fs.existsSync(tempDir)) {
        const files = fs.readdirSync(tempDir);
        for (const file of files) {
            try { fs.unlinkSync(path.join(tempDir, file)); } catch (err) {}
        }
    }
    console.log('[Sistema] Transmisión finalizada. ¡Hasta pronto, ingeniero!');
    process.exit(0);
});

// --- ARRANQUE ---
console.log('--- INICIALIZANDO AUTO-DJ MARINA GAMING RADIO ---');
startStreaming();