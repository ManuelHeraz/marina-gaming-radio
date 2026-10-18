const { exec } = require('youtube-dl-exec');
const fs = require('fs');

// PON AQUÍ TODAS LAS PLAYLISTS DE YOUTUBE QUE QUIERAS MINAR
const PLAYLIST_URLS = [
    "https://youtube.com/playlist?list=PLrnb8c3hFJatjyJ-wFMuFGANNoo7-LZsG&si=o0KyPiCVzGIat5n-",
];

async function extraerPlaylist(url) {
    try {
        const info = await exec(url, {
            dumpSingleJson: true,
            flatPlaylist: true, // Extrae metadatos rápido sin descargar audios
            noWarnings: true
        });

        // youtube-dl-exec a veces devuelve el objeto directamente o en stdout
        let rawData = info;
        if (info.stdout) {
            try { rawData = JSON.parse(info.stdout); } catch {}
        }

        const entries = rawData.entries || [];
        const pistas = [];

        for (const entry of entries) {
            if (!entry || !entry.id) continue;
            const title = entry.title || 'Pista sin título';
            if (title === '[Private video]' || title === '[Deleted video]') continue;

            pistas.push({
                title: title,
                source: `https://www.youtube.com/watch?v=${entry.id}`
            });
        }
        return pistas;
    } catch (error) {
        console.error(`[Aviso] No se pudo extraer la lista ${url}: ${error.message}`);
        return [];
    }
}

async function minarTodas() {
    console.log(`[Minerador Masivo] Iniciando extracción de ${PLAYLIST_URLS.length} playlist(s)...`);
    
    let todasLasPistas = [];
    const urlsVistas = new Set(); // Para evitar duplicados globales

    for (let i = 0; i < PLAYLIST_URLS.length; i++) {
        const url = PLAYLIST_URLS[i];
        console.log(`\n[Minerador] (${i + 1}/${PLAYLIST_URLS.length}) Procesando: ${url}`);
        
        const pistasDeLista = await extraerPlaylist(url);
        console.log(`[Minerador] -> Encontradas ${pistasDeLista.length} pistas válidas.`);

        for (const track of pistasDeLista) {
            if (!urlsVistas.has(track.source)) {
                urlsVistas.add(track.source);
                todasLasPistas.push(track);
            }
        }
    }

    // Mezcla aleatoria (Fisher-Yates Shuffle) para que las canciones de distintas listas queden bien combinadas
    console.log(`\n[Minerador] Mezclando el contenido total de ${todasLasPistas.length} pistas únicas...`);
    for (let i = todasLasPistas.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [todasLasPistas[i], todasLasPistas[j]] = [todasLasPistas[j], todasLasPistas[i]];
    }

    const jsonFinal = JSON.stringify({ tracks: todasLasPistas }, null, 2);
    fs.writeFileSync('playlist_extraida.json', jsonFinal, 'utf8');
    
    console.log(`\n[Éxito Total] Se ha creado 'playlist_extraida.json' con un total de ${todasLasPistas.length} pistas únicas y mezcladas.`);
}

minarTodas();