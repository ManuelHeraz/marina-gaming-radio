const { exec } = require('youtube-dl-exec');
const fs = require('fs');

// PON AQUÍ TODOS LOS ENLACES QUE QUIERAS SEPARADOS POR COMAS Y ENTRE COMILLAS
const PLAYLIST_URLS = [
    "https://youtube.com/playlist?list=PL6v0YAHfvjfmMO-Nk1W2JnWrrx62ZL439&si=m3Murl8-vXlpqkTP",
    "https://youtube.com/playlist?list=PL6v0YAHfvjfkKGnjzge5Tm5NGR-guymSG&si=0WSyygM4A_Pt8cWj",
    "https://youtube.com/playlist?list=PLE90wbxeVJe3-p5NlJ9pestQ5NtL1KFOu&si=luGplWe_Klsa2imJ",
    "https://youtube.com/playlist?list=PLyPN4yau_b5ABhovJb8dvTpszMhBLSXbj&si=AQPUyE0aUGIHuXn4",
    "https://youtube.com/playlist?list=PLJ57eLLt_4Beei5eTqf9JVwEzCTvsuRiU&si=FJZ5rAxbi0cKrthk",
    "https://youtube.com/playlist?list=PLH09LP-3uTZf0pslxwCZ3Wl2km2r0qYpC&si=NniXaAWB9I1OAZt8",
    "https://youtube.com/playlist?list=PLBCEdCTyxWQ-gVv_6r2YQLcsSipifWVrZ&si=UsUJ79cFiP6CjJcb",
    "https://youtube.com/playlist?list=PL_9Vs-PDx24xZXmAfNVQXqQ0gCmaXxE3W&si=BIaPlZwgjuv1Soxd",
    "https://youtube.com/playlist?list=PLE90wbxeVJe1WSigGnSP8ARFJ9pUhQy4w&si=Yl0pj83cE5FIp1rJ",
    "https://youtube.com/playlist?list=PLCutjjq7vO82JIyKJ97sIJxXCnRMXfndQ&si=hGBkBXlFQuIZbnB5",
    "https://youtube.com/playlist?list=PLCutjjq7vO83dxVqiq4wUmdi4u-W4i-Kb&si=6sSuM0Dd6cIMpYlz"
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