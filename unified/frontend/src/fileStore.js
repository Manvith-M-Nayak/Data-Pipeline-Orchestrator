// Keeps the selected data file across page reloads. A File cannot go into
// localStorage, but IndexedDB stores it natively (structured clone), so a
// refresh no longer leaves a restored plan without its data file.
// Local to this browser; nothing is uploaded anywhere by this.

const DB_NAME = "pipeline-orchestrator";
const STORE = "files";
const KEY = "data_file";
// Very large files are not kept (browser storage quotas vary); the user just
// re-selects them after a reload, as before.
export const MAX_PERSIST_BYTES = 200 * 1024 * 1024;

function openDb() {
  return new Promise((resolve, reject) => {
    if (!window.indexedDB) { reject(new Error("IndexedDB unavailable")); return; }
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(STORE);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function tx(mode, fn) {
  return openDb().then((db) => new Promise((resolve, reject) => {
    const t = db.transaction(STORE, mode);
    const out = fn(t.objectStore(STORE));
    t.oncomplete = () => { db.close(); resolve(out?.result); };
    t.onerror = () => { db.close(); reject(t.error); };
    t.onabort = () => { db.close(); reject(t.error); };
  }));
}

export async function saveDataFile(file) {
  try {
    if (!file) { await tx("readwrite", (s) => s.delete(KEY)); return false; }
    if (file.size > MAX_PERSIST_BYTES) { await tx("readwrite", (s) => s.delete(KEY)); return false; }
    await tx("readwrite", (s) => s.put(file, KEY));
    return true;
  } catch {
    return false;          // private mode / quota — the file stays in memory only
  }
}

export async function loadDataFile() {
  try {
    const f = await tx("readonly", (s) => s.get(KEY));
    return f instanceof Blob ? f : null;
  } catch {
    return null;
  }
}
