const fs = require('fs');

try {
    console.log("[Paso 1] Leyendo archivo playlist_extraida.json...");
    if (!fs.existsSync('playlist_extraida.json')) {
        console.error("⚠️ Error Crítico: No se encuentra 'playlist_extraida.json'. Ejecuta 'minero.js' primero.");
        process.exit(1);
    }

    const rawData = fs.readFileSync('playlist_extraida.json', 'utf8');

    // Eliminamos caracteres invisibles (BOM)
    const cleanRawData = rawData.replace(/^\uFEFF/, '');
    const playlist = JSON.parse(cleanRawData);

    console.log(`[Paso 2] Archivo parseado correctamente.`);
    console.log(`[Paso 3] ¿Tiene la propiedad 'tracks'? ${!!playlist.tracks}`);
    
    if (playlist.tracks) {
        console.log(`[Paso 4] ¿Cuántas pistas hay en 'tracks'? ${playlist.tracks.length}`);
    }

    // Salvaguarda
    if (!playlist || !playlist.tracks || !Array.isArray(playlist.tracks)) {
        console.error("⚠️ Error Crítico: Node.js no puede ver el arreglo 'tracks'. Abortando.");
        process.exit(1);
    }

    console.log("[Paso 5] Iniciando filtrado avanzado de pistas (Basura, Relleno y Videos Largos)...");
    
    // Palabras prohibidas en el título que delatan videos de 1 hora, mix o recopilaciones
    const palabrasProhibidas = [
        '1 hour', '2 hours', '3 hours', '4 hours', '5 hours', '10 hours',
        '1 hora', '2 horas', '10 horas',
        'extended', 'extendido', 'version extendida',
        'mix', 'playlist', 'album', 'soundtrack full', 'full ost',
        'live', 'en vivo', '24/7', 'radio stream', 'compilation', 'recopilación'
    ];

    const pistasLimpias = playlist.tracks.filter((pista, index) => {
        if (!pista || !pista.title || !pista.source) {
            console.log(`  - Pista defectuosa descartada en el índice ${index}`);
            return false;
        }

        const tituloOriginal = String(pista.title).trim();
        const titulo = tituloOriginal.toLowerCase();

        // 1. Descartar nulos o eliminados
        if (titulo === 'null' || titulo === '[private video]' || titulo === '[deleted video]') {
            console.log(`  - Video borrado/privado descartado: "${tituloOriginal}"`);
            return false;
        }

        // 2. Filtro antitrampa por palabras clave (Mixes de horas, recopilaciones, etc.)
        const tienePalabraProhibida = palabrasProhibidas.some(palabra => titulo.includes(palabra));
        if (tienePalabraProhibida) {
            console.log(`  - Descartado por ser mix/recopilación/largo: "${tituloOriginal}"`);
            return false;
        }

        return true; 
    });

    if (pistasLimpias.length === 0) {
        console.log("🛑 ¡Abortando! Todas las pistas fueron rechazadas. No se modificó nada.");
        process.exit(1);
    }

    const jsonFinal = JSON.stringify({ tracks: pistasLimpias }, null, 2);
    fs.writeFileSync('playlist_limpia.json', jsonFinal, 'utf8');

    console.log(`\n[Limpieza Finalizada] Pasamos de ${playlist.tracks.length} a ${pistasLimpias.length} pistas válidas.`);
    console.log(`[Éxito] Archivo guardado como 'playlist_limpia.json'.`);

} catch (error) {
    console.error(`[Error fatal de sintaxis JSON]: ${error.message}`);
}