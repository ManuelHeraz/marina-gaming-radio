'use strict';

const fs = require('fs');
const path = require('path');

const PLAYLIST_PRINCIPAL = path.join(__dirname, 'playlist.json');
const PLAYLISTS_DIR = path.join(__dirname, 'playlists');

function loadTracksFromFile(filePath) {
    try {
        if (!fs.existsSync(filePath)) return [];
        const raw = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '');
        const data = JSON.parse(raw);
        const list = Array.isArray(data.tracks) ? data.tracks : (Array.isArray(data) ? data : []);
        return list.filter(t => t && typeof t.title === 'string' && typeof t.source === 'string' && t.source.trim() !== '');
    } catch (e) {
        console.log(`[Aviso] No se pudo leer ${path.basename(filePath)}: ${e.message}`);
        return [];
    }
}

function unificarPlaylists() {
    console.log('[Unificador] Iniciando unificación de la bóveda musical...');

    const urlsVistas = new Set();
    const pistasUnificadas = [];

    // 1. CARGAR LO QUE YA ESTABA DENTRO DE playlist.json (Respetar lo existente)
    console.log('[Unificador] Leyendo playlist.json actual...');
    const tracksPrincipales = loadTracksFromFile(PLAYLIST_PRINCIPAL);
    
    for (const track of tracksPrincipales) {
        if (!urlsVistas.has(track.source)) {
            urlsVistas.add(track.source);
            pistasUnificadas.push(track);
        }
    }
    console.log(`[Unificador] -> Conservadas ${pistasUnificadas.length} pistas originales de playlist.json.`);

    // 2. LEER TODOS LOS JSON DE LA CARPETA playlists/
    if (!fs.existsSync(PLAYLISTS_DIR)) {
        console.log(`[Aviso] La carpeta 'playlists/' no existe. Creándola...`);
        fs.mkdirSync(PLAYLISTS_DIR, { recursive: true });
        return;
    }

    const archivos = fs.readdirSync(PLAYLISTS_DIR).filter(f => f.toLowerCase().endsWith('.json'));
    console.log(`[Unificador] Encontrados ${archivos.length} archivos de listas en 'playlists/'.`);

    let totalNuevasAgregadas = 0;

    for (const archivo of archivos) {
        // Excluimos la lista de podcasts ya que esos van por separado en el motor del podcast
        if (archivo === 'checkpoint_podcasts.json') continue;

        const rutaArchivo = path.join(PLAYLISTS_DIR, archivo);
        const pistasShow = loadTracksFromFile(rutaArchivo);
        
        let agregadasDelShow = 0;
        for (const track of pistasShow) {
            if (!urlsVistas.has(track.source)) {
                urlsVistas.add(track.source);
                pistasUnificadas.push(track);
                agregadasDelShow++;
                totalNuevasAgregadas++;
            }
        }
        console.log(`  - Procesado [${archivo}]: ${pistasShow.length} leídas, ${agregadasDelShow} nuevas incorporadas.`);
    }

    // 3. MEZCLA ALEATORIA GLOBAL (Fisher-Yates Shuffle)
    console.log(`[Unificador] Mezclando el total de ${pistasUnificadas.length} pistas de la bóveda...`);
    for (let i = pistasUnificadas.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [pistasUnificadas[i], pistasUnificadas[j]] = [pistasUnificadas[j], pistasUnificadas[i]];
    }

    // 4. GUARDAR EL NUEVO playlist.json
    const jsonFinal = JSON.stringify({ tracks: pistasUnificadas }, null, 2);
    fs.writeFileSync(PLAYLIST_PRINCIPAL, jsonFinal, 'utf8');

    console.log(`\n[Éxito Total] Unificación completada.`);
    console.log(` - Pistas nuevas añadidas desde la carpeta playlists/: ${totalNuevasAgregadas}`);
    console.log(` - Tamaño total de la bóveda unificada en 'playlist.json': ${pistasUnificadas.length} pistas.`);
}

unificarPlaylists();