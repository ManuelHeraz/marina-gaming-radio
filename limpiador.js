const fs = require('fs');

try {
    console.log("[Paso 1] Leyendo archivo playlist.json...");
const rawData = fs.readFileSync('playlist.json', 'utf8');

    // Eliminamos caracteres invisibles (BOM) que a veces se pegan al copiar/pegar
    const cleanRawData = rawData.replace(/^\uFEFF/, '');
    const playlist = JSON.parse(cleanRawData);

    console.log(`[Paso 2] Archivo parseado correctamente. ¿Es un objeto? ${typeof playlist}`);
    console.log(`[Paso 3] ¿Tiene la propiedad 'tracks'? ${!!playlist.tracks}`);
    
    if (playlist.tracks) {
        console.log(`[Paso 4] ¿Cuántas pistas hay en 'tracks'? ${playlist.tracks.length}`);
    }

    // Salvaguarda
    if (!playlist || !playlist.tracks || !Array.isArray(playlist.tracks)) {
        console.error("⚠️ Error Crítico: Node.js no puede ver el arreglo 'tracks'. Abortando.");
        process.exit(1);
    }

    console.log("[Paso 5] Iniciando filtrado de pistas nulas/borradas...");
    const pistasLimpias = playlist.tracks.filter((pista, index) => {
        if (!pista || !pista.title || !pista.source) {
            console.log(`  - Pista defectuosa descartada en el índice ${index}`);
            return false;
        }

        const titulo = String(pista.title).trim().toLowerCase();

        if (titulo === 'null' || titulo === '[private video]' || titulo === '[deleted video]') {
            console.log(`  - Video borrado/privado descartado en el índice ${index}`);
            return false;
        }

        return true; 
    });

    if (pistasLimpias.length === 0) {
        console.log("🛑 ¡Abortando! Todas las pistas fueron rechazadas. No se modificó nada.");
        process.exit(1);
    }

    const jsonFinal = JSON.stringify({ tracks: pistasLimpias }, null, 2);
    fs.writeFileSync('playlist_limpia.json', jsonFinal);

    console.log(`\n[Limpieza Finalizada] Pasamos de ${playlist.tracks.length} a ${pistasLimpias.length} pistas válidas.`);
    console.log(`[Éxito] Archivo guardado como 'playlist_limpia.json'.`);

} catch (error) {
    console.error(`[Error fatal de sintaxis JSON]: ${error.message}`);
}