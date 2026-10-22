'use strict';

const fs = require('fs');
const path = require('path');
const { exec } = require('youtube-dl-exec');

// ───────────────────────── PARÁMETROS DEL FILTRO ─────────────────────────
const MIN_DURATION_SEC = 45;        // Menos de 45s suele ser intro de canal, meme corto, jingle o efecto de sonido
const MAX_DURATION_SEC = 12 * 60;   // 12 minutos (720s): corta compilaciones, podcasts, vlogs largos y 1-hour loops

// Puedes ejecutar: node limpiador.js
// O limpiar un archivo específico: node limpiador.js playlists/maquina_tiempo.json
const archivoEntrada = process.argv[2] || 'playlist_extraida.json';
const archivoSalida = process.argv[3] || (process.argv[2] ? process.argv[2] : 'playlist_limpia.json');

// Si pones CONSULTAR_DURACION_FALTANTE = true, consultará con yt-dlp las canciones viejas que no tengan 'duration'
const CONSULTAR_DURACION_FALTANTE = true;

// ───────────────────────── DICCIONARIO ANTI-HABLADO / ANTI-VLOG ─────────────────────────
// Usamos expresiones regulares con límites de palabra (\b) para no borrar canciones por accidente
const PATRONES_PROHIBIDOS = [
    // 1. Duraciones extremas y recopilaciones
    /\b(\d+\s*(hour|hours|hr|hrs|hora|horas))\b/i,
    /\b(extended|extendida|full\s*ost|full\s*album|full\s*soundtrack|all\s*songs|compilation|recopilaci[oó]n|megamix|24\/7)\b/i,

    // 2. Locutores de radio (GTA / Fallout / Juegos) y Comerciales hablados
    /\b(dj\s*banter|all\s*commercials|commercials\s*only|radio\s*commercials|voice\s*lines|all\s*quotes|dialogues?|di[aá]logos)\b/i,
    /\b(talk\s*radio|chatterbox|wctr|wktt|kchat|vcpr|pressing\s*issues|lazlow|fernando\s*martinez|maurice\s*chavez)\b/i,
    /\b(host\s*segments?|only\s*talk|news\s*segments?|weazel\s*news|entrevista|interview|monologue|mon[oó]logo)\b/i,

    // 3. Reacciones, Reseñas, Narraciones y Análisis
    /\b(reaction|reacci[oó]n|reaccionando|reacts\s*to|review|rese[ñn]a|an[aá]lisis|explicaci[oó]n|explained|lore\s*explained)\b/i,
    /\b(storytime|creepypasta|loquelendo|loquendo|teor[ií]a|curiosidades|top\s*\d+|audiolibro|audiobook|narrado|narrated)\b/i,
    /\b(podcast|charla|debate|documentary|documental|ensayo|video\s*essay|iceberg)\b/i,

    // 4. Vlogs de YouTube de antaño, Sketches y Gameplays
    /\b(vlog|blog|mi\s*vida|tag\s*del|preguntas\s*y\s*respuestas|q&a|unboxing|haul|house\s*tour|room\s*tour|setup\s*tour)\b/i,
    /\b(sketch|parodia\s*hablada|broma|prank|reto\s*de|challenge|blooper|bloopers|outtakes|detr[aá]s\s*de\s*c[aá]maras)\b/i,
    /\b(gameplay|walkthrough|playthrough|let'?s\s*play|speedrun|tutorial|gu[ií]a|how\s*to|como\s*hacer|cutscenes|all\s*cinematics)\b/i
];

function coincidePatronProhibido(titulo) {
    for (const regex of PATRONES_PROHIBIDOS) {
        if (regex.test(titulo)) {
            return regex.toString();
        }
    }
    return null;
}

// Consulta la duración y categoría en YouTube para pistas que aún no tienen el campo 'duration'
async function enriquecerMetadatos(pista) {
    try {
        const info = await exec(pista.source, {
            dumpSingleJson: true,
            noPlaylist: true,
            skipDownload: true,
            noWarnings: true,
            socketTimeout: 8
        });
        const data = typeof info === 'string' ? JSON.parse(info) : (info.stdout ? JSON.parse(info.stdout) : info);
        return {
            duration: Number(data.duration) || 0,
            categories: Array.isArray(data.categories) ? data.categories : []
        };
    } catch {
        return { duration: 0, categories: [] };
    }
}

async function ejecutarLimpieza() {
    console.log(`[Paso 1] Leyendo archivo '${archivoEntrada}'...`);
    if (!fs.existsSync(archivoEntrada)) {
        console.error(`⚠️ Error: No existe el archivo '${archivoEntrada}'.`);
        process.exit(1);
    }

    const rawData = fs.readFileSync(archivoEntrada, 'utf8').replace(/^\uFEFF/, '');
    const playlist = JSON.parse(rawData);

    if (!playlist || !Array.isArray(playlist.tracks)) {
        console.error("⚠️ Error Crítico: El archivo no contiene un arreglo 'tracks' válido.");
        process.exit(1);
    }

    const totalInicial = playlist.tracks.length;
    console.log(`[Paso 2] Analizando ${totalInicial} pistas (Rango permitido: ${MIN_DURATION_SEC}s a ${MAX_DURATION_SEC}s [12 min])...\n`);

    const pistasLimpias = [];
    let descartadasPorTexto = 0;
    let descartadasPorTiempo = 0;
    let descartadasPorCategoria = 0;

    for (let i = 0; i < totalInicial; i++) {
        const pista = playlist.tracks[i];
        if (!pista || !pista.title || !pista.source) continue;

        const tituloOriginal = String(pista.title).trim();
        const tituloLower = tituloOriginal.toLowerCase();

        // 1. Filtro de borrados / privados
        if (tituloLower === 'null' || tituloLower === '[private video]' || tituloLower === '[deleted video]') {
            descartadasPorTexto++;
            continue;
        }

        // 2. Filtro por Diccionario Anti-Narración / Anti-Vlog / Anti-Locutores
        const patronDetectado = coincidePatronProhibido(tituloOriginal);
        if (patronDetectado) {
            console.log(` ❌ [Texto/Vlog/Narrado] "${tituloOriginal}"`);
            descartadasPorTexto++;
            continue;
        }

        // 3. Filtro por Duración (y Categoría de YouTube si falta la duración)
        let duracion = Number(pista.duration) || 0;
        let categorias = [];

        if (duracion === 0 && CONSULTAR_DURACION_FALTANTE) {
            process.stdout.write(` 🔍 [${i + 1}/${totalInicial}] Verificando duración de: "${tituloOriginal.slice(0, 45)}..."\r`);
            const meta = await enriquecerMetadatos(pista);
            duracion = meta.duration;
            categorias = meta.categories;
        }

        // Si YouTube reporta explícitamente que la categoría es "People & Blogs", "News & Politics" o "Howto & Style"
        // y NO tiene palabras musicales ("ost", "theme", "song", "music", "remix", "cover", "lyrics", "audio", "official")
        const tieneIndicadorMusical = /\b(ost|soundtrack|theme|song|music|m[uú]sica|remix|cover|lyrics|letra|oficial|official|audio|instrumental|opening|ending)\b/i.test(tituloOriginal);
        const esCategoriaHablada = categorias.some(c => ['People & Blogs', 'News & Politics', 'Howto & Style'].includes(c));

        if (esCategoriaHablada && !tieneIndicadorMusical) {
            console.log(`\n ❌ [Categoría Vlog/Blog (${categorias.join(',')})] "${tituloOriginal}"`);
            descartadasPorCategoria++;
            continue;
        }

        if (duracion > 0) {
            if (duracion < MIN_DURATION_SEC) {
                console.log(`\n ⏱️ [Muy corta: ${duracion}s] "${tituloOriginal}"`);
                descartadasPorTiempo++;
                continue;
            }
            if (duracion > MAX_DURATION_SEC) {
                console.log(`\n ⏱️ [Excede 12 min: ${Math.round(duracion / 60)} min] "${tituloOriginal}"`);
                descartadasPorTiempo++;
                continue;
            }
        }

        // Guardamos la pista limpia conservando su duración para no volver a consultarla en el futuro
        pistasLimpias.push({
            title: tituloOriginal,
            source: pista.source,
            ...(duracion > 0 ? { duration: duracion } : {})
        });
    }

    if (pistasLimpias.length === 0) {
        console.log("\n🛑 ¡Abortando! Todas las pistas fueron rechazadas. No se sobrescribió el archivo.");
        process.exit(1);
    }

    const jsonFinal = JSON.stringify({ tracks: pistasLimpias }, null, 2);
    fs.writeFileSync(archivoSalida, jsonFinal, 'utf8');

    console.log(`\n=============================================================`);
    console.log(`✅ [Limpieza Completada] Archivo guardado en: '${archivoSalida}'`);
    console.log(`   - Pistas originales:              ${totalInicial}`);
    console.log(`   - Eliminadas por Vlog/Locutores:  ${descartadasPorTexto}`);
    console.log(`   - Eliminadas por Categoría Vlog:  ${descartadasPorCategoria}`);
    console.log(`   - Eliminadas por Duración (<45s o >12m): ${descartadasPorTiempo}`);
    console.log(`   - Pistas finales aprobadas:       ${pistasLimpias.length}`);
    console.log(`=============================================================`);
}

ejecutarLimpieza();