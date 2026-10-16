const { exec } = require('youtube-dl-exec');
const fs = require('fs');

// PON AQUÍ TODOS LOS ENLACES QUE QUIERAS SEPARADOS POR COMAS Y ENTRE COMILLAS
const PLAYLIST_URLS = [
    "https://youtube.com/playlist?list=PL6v0YAHfvjfmMO-Nk1W2JnWrrx62ZL439&si=m3Murl8-vXlpqkTP",
];

async function procesarPlaylists() {
    console.log(`[Minerador Global] Iniciando procesamiento de ${PLAYLIST_URLS.length} playlists...\n`);
    
    const todasLasPistasLimpias = [];
    let totalOriginales = 0;
    let totalDescartadas = 0;

    for (let i = 0; i < PLAYLIST_URLS.length; i++) {
        const url = PLAYLIST_URLS[i];
        console.log(`[Minerador] --- Procesando Playlist ${i + 1} de ${PLAYLIST_URLS.length} ---`);
        console.log(`[Minerador] Extrayendo metadatos de: ${url}`);
        
        try {
            // --- PASO 1: EXTRACCIÓN ---
            const info = await exec(url, {
                dumpSingleJson: true,
                flatPlaylist: true, 
                noWarnings: true
            });

            const rawData = JSON.parse(info.stdout);
            
            if (!rawData || !rawData.entries || !Array.isArray(rawData.entries)) {
                console.error(`⚠️ Advertencia: No se pudo obtener la lista de pistas para la playlist ${i + 1}. Saltando...`);
                continue; // Pasa a la siguiente URL sin detener todo el script
            }

            totalOriginales += rawData.entries.length;
            console.log(`[Minerador] Se encontraron ${rawData.entries.length} pistas. Iniciando limpieza...`);

            let descartadasLocal = 0;

            // --- PASO 2: LIMPIEZA EN MEMORIA ---
            rawData.entries.forEach((entry, index) => {
                if (!entry || !entry.title || !entry.id) {
                    descartadasLocal++;
                    return;
                }

                const titulo = String(entry.title).trim().toLowerCase();

                if (titulo === 'null' || titulo === '[private video]' || titulo === '[deleted video]') {
                    descartadasLocal++;
                    return;
                }

                todasLasPistasLimpias.push({
                    title: entry.title,
                    source: `https://www.youtube.com/watch?v=${entry.id}`
                });
            });

            totalDescartadas += descartadasLocal;
            console.log(`[Minerador] Playlist ${i + 1} completada. Descartadas localmente: ${descartadasLocal}\n`);

        } catch (error) {
            console.error(`[Error] Falló la extracción en la playlist ${i + 1}: ${error.message}`);
            console.log("Continuando con la siguiente lista de la fila...\n");
        }
    }

    // --- PASO 3: EXPORTACIÓN GLOBAL ---
    if (todasLasPistasLimpias.length === 0) {
        console.log("🛑 ¡Abortando! Todas las pistas de todas las listas fallaron o fueron rechazadas. No se creó el archivo.");
        process.exit(1);
    }

    const jsonFinal = JSON.stringify({ tracks: todasLasPistasLimpias }, null, 2);
    
    fs.writeFileSync('playlist_limpia.json', jsonFinal);
    
    console.log(`======================================================`);
    console.log(`[Resumen Final]`);
    console.log(`- Playlists procesadas: ${PLAYLIST_URLS.length}`);
    console.log(`- Pistas totales analizadas: ${totalOriginales}`);
    console.log(`- Pistas totales descartadas: ${totalDescartadas}`);
    console.log(`[Éxito] Se guardaron ${todasLasPistasLimpias.length} pistas válidas en 'playlist_limpia.json'.`);
    console.log(`======================================================`);
}

procesarPlaylists();