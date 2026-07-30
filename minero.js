const { exec } = require('youtube-dl-exec');
const fs = require('fs');

// PON AQUÍ EL ENLACE DE UNA PLAYLIST PÚBLICA DE YOUTUBE
// Ejemplo: una lista de 200 soundtracks épicos de videojuegos
const PLAYLIST_URL = "https://youtube.com/playlist?list=PLGzv-vVL30V1oPPa7nVcUzLqyT8AsqwEb&si=bqE2pzihdh5R8wyT"; // <- CAMBIA ESTO

async function extraerPlaylist() {
    console.log(`[Minerador] Extrayendo metadatos de la lista... esto tomará un momento.`);
    try {
        const info = await exec(PLAYLIST_URL, {
            dumpSingleJson: true,
            flatPlaylist: true, // Esto es clave: extrae enlaces sin descargar el audio
            noWarnings: true
        });

        const rawData = JSON.parse(info.stdout);
        const pistas = [];

        console.log(`[Minerador] Se encontraron ${rawData.entries.length} pistas. Formateando...`);

        for (const entry of rawData.entries) {
            // Saltamos videos borrados o privados
            if (entry.title === '[Private video]' || entry.title === '[Deleted video]') continue;

            pistas.push({
                title: entry.title,
                source: `https://www.youtube.com/watch?v=${entry.id}`
            });
        }

        const jsonFinal = JSON.stringify({ tracks: pistas }, null, 2);
        
        fs.writeFileSync('playlist_extraida.json', jsonFinal);
        console.log(`[Éxito] Se ha creado el archivo 'playlist_extraida.json' con ${pistas.length} pistas.`);
        
    } catch (error) {
        console.error(`[Error] Falló la extracción: ${error.message}`);
    }
}

extraerPlaylist();