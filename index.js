require('dotenv').config();
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static');
const { exec } = require('youtube-dl-exec');
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb'); // <-- IMPORTAMOS MONGODB

ffmpeg.setFfmpegPath(ffmpegPath);

const cleanMountPoint = process.env.ZENO_MOUNT.replace(/^\//, '');
const icecastUrl = `icecast://source:${process.env.ZENO_PASSWORD}@${process.env.ZENO_SERVER}:${process.env.ZENO_PORT}/${cleanMountPoint}`;

const rawPlaylist = fs.readFileSync(path.join(__dirname, 'playlist.json'));
const playlistData = JSON.parse(rawPlaylist);

const tempDir = path.join(__dirname, 'temp');
if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
}

let currentTrackIndex = 0;

// --- GESTIÓN DE BASE DE DATOS MONGODB ---
let dbClient = null;
async function connectDB() {
    if (!process.env.MONGODB_URI) return null;
    
    if (!dbClient) {
        dbClient = new MongoClient(process.env.MONGODB_URI);
        await dbClient.connect();
        console.log('[Sistema] Conectado a MongoDB Atlas exitosamente.');
    }
    // Asumimos que la base de datos se llama 'marina_radio'
    return dbClient.db('marina_radio');
}

// Añadimos estas variables globales
let currentTrackIndex = 0;
let tracksPlayedSinceBumper = 0; // El contador
const BUMPER_FREQUENCY = 4; // Cada cuántas canciones suena una cortinilla

// --- EL CEREBRO HÍBRIDO (Nube + Local) ---
async function getNextTrack() {
    // 1. INTENTO A: Buscar en MongoDB
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
        tracksPlayedSinceBumper = 0; // Reiniciamos el contador
        
        const assetsDir = path.join(__dirname, 'assets');
        
        // Verificamos que la carpeta exista para evitar crasheos
        if (fs.existsSync(assetsDir)) {
            // Escaneamos la carpeta y filtramos solo los archivos .mp3
            const mp3Files = fs.readdirSync(assetsDir).filter(file => file.endsWith('.mp3'));
            
            if (mp3Files.length > 0) {
                console.log(`[Cerebro] Insertando cortinilla dinámica local...`);
                // Elegimos un archivo al azar del arreglo
                const randomFile = mp3Files[Math.floor(Math.random() * mp3Files.length)];
                
                return {
                    // Usamos el nombre del archivo (sin el .mp3) como título para el reproductor
                    title: `Marina Gaming - ${randomFile.replace('.mp3', '')}`,
                    source: path.join(assetsDir, randomFile)
                };
            } else {
                console.log(`[Cerebro] Carpeta assets sin archivos .mp3. Saltando cortinilla.`);
            }
        } else {
            console.log(`[Cerebro] No existe la carpeta assets. Saltando cortinilla.`);
        }
    }

    // 3. INTENTO B (FALLBACK): Reproducción aleatoria de tu bóveda
    console.log(`[Cerebro] Cola vacía. Seleccionando pista aleatoria del respaldo.`);
    const randomIndex = Math.floor(Math.random() * playlistData.tracks.length);
    
    // Aumentamos el contador porque acaba de sonar una pista musical normal
    tracksPlayedSinceBumper++; 
    
    return playlistData.tracks[randomIndex];
}

// --- ACTUALIZADA PARA ESPERAR EL ASYNC DE getNextTrack ---
async function startStreaming() {
    const track = await getNextTrack(); // <-- AHORA USA AWAIT
    console.log(`\n[AutoDJ] Preparando: ${track.title}`);

    let audioSource = track.source;
    const isExternalUrl = audioSource.includes('youtube.com') || audioSource.includes('youtu.be') || audioSource.includes('soundcloud.com') || audioSource.includes('on.soundcloud.com');
    
    let tempFilePath = null;
    let startTime = Date.now();

    if (isExternalUrl) {
        tempFilePath = path.join(tempDir, `track-${Date.now()}.m4a`);
        console.log(`[yt-dlp] Descargando la pista en caché efímera...`);
        
        try {
            await exec(audioSource, {
                f: '140/bestaudio[ext=m4a]/bestaudio',
                o: tempFilePath,
                noWarnings: true
            });
            console.log(`[yt-dlp] ¡Descarga completa!`);
            audioSource = tempFilePath;
        } catch (error) {
            console.error(`[Error yt-dlp]: Falló la descarga. Saltando pista.`);
            cleanup(tempFilePath);
            return retryWithCooldown(startTime);
        }
    }

    const command = ffmpeg(audioSource)
        .inputOptions(['-re'])
        .audioCodec('libmp3lame')
        .audioBitrate('128k')
        .audioChannels(2)
        .audioFrequency(44100)
        .format('mp3')
        .outputOptions([
            '-content_type', 'audio/mpeg',
            '-ice_public', '1',
            '-ice_name', 'Marina_Gaming_Radio' 
        ]);

    command
        .output(icecastUrl, { end: false })
        .on('start', () => {
            console.log(`[FFmpeg] Transmitiendo a ZenoFM -> ${track.title}`);
        })
        .on('stderr', (stderrLine) => {
            if (stderrLine.includes('size=')) {
                // console.log(`[Motor FFmpeg]: ${stderrLine}`); 
            }
        })
        .on('error', (err) => {
            console.error(`[Error FFmpeg]: ${err.message}`);
            cleanup(tempFilePath);
            retryWithCooldown(startTime);
        })
        .on('end', () => {
            console.log(`[AutoDJ] Pista terminada.`);
            cleanup(tempFilePath);
            retryWithCooldown(startTime);
        });

    command.run();
}

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
    console.log('[Sistema] Vaciando la caché efímera (Carpeta temp)...');
    
    if (fs.existsSync(tempDir)) {
        const files = fs.readdirSync(tempDir);
        let deletedCount = 0;
        
        for (const file of files) {
            try {
                fs.unlinkSync(path.join(tempDir, file));
                deletedCount++;
            } catch (err) {
                console.error(`[Error] No se pudo borrar ${file}: ${err.message}`);
            }
        }
        console.log(`[Sistema] Limpieza completada: ${deletedCount} archivo(s) eliminado(s).`);
    }
    
    console.log('[Sistema] Transmisión finalizada. ¡Hasta pronto, ingeniero!');
    process.exit(0);
});

console.log('--- INICIALIZANDO AUTO-DJ MARINA GAMING RADIO ---');
startStreaming();