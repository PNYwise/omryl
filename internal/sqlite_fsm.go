package internal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// SQLiteFSM kelola DB + lastApplied
type SQLiteFSM struct {
	dbPath      string
	db          *sql.DB
	mu          sync.RWMutex // -> RWMutex untuk read-heavy ops
	lastApplied uint64
}

// Pastikan SQLiteFSM mengimplementasikan raft.FSM
var _ raft.FSM = (*SQLiteFSM)(nil)

// NewSQLiteFSM membuat instance SQLiteFSM baru dan menginisialisasi database.
func NewSQLiteFSM(dbPath string) *SQLiteFSM {
	fsm := &SQLiteFSM{dbPath: dbPath}
	db, err := openSQLite(dbPath)
	if err != nil {
		log.Fatalf("Gagal membuka database SQLite: %v", err)
	}
	fsm.db = db

	// Schema
	if _, err = fsm.db.Exec(`
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL
        );
    `); err != nil {
		log.Fatalf("Gagal membuat tabel di database FSM: %v", err)
	}

	fsm.initMeta()
	log.Printf("SQLite (go-sqlite3) siap di: %s", dbPath)
	return fsm
}

// initMeta: inisialisasi & load lastApplied
func (fsm *SQLiteFSM) initMeta() {
	if _, err := fsm.db.Exec(`CREATE TABLE IF NOT EXISTS raft_meta (
		id INTEGER PRIMARY KEY,
		last_applied INTEGER NOT NULL DEFAULT 0
	);`); err != nil {
		log.Fatalf("Gagal membuat tabel raft_meta: %v", err)
	}
	var idx uint64
	err := fsm.db.QueryRow(`SELECT last_applied FROM raft_meta WHERE id=1;`).Scan(&idx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, e := fsm.db.Exec(`INSERT INTO raft_meta (id, last_applied) VALUES (1, 0);`); e != nil {
			log.Fatalf("Gagal insert initial raft_meta: %v", e)
		}
		fsm.lastApplied = 0
	case err != nil:
		log.Fatalf("Gagal membaca raft_meta: %v", err)
	default:
		fsm.lastApplied = idx
	}
}

// updateLastApplied: prepared update (sedikit hemat alloc)
func (fsm *SQLiteFSM) updateLastApplied(idx uint64) {
	if _, err := fsm.db.Exec(`UPDATE raft_meta SET last_applied=? WHERE id=1;`, idx); err != nil {
		log.Printf("Gagal update last_applied: %v", err)
	}
}

// Apply dipanggil oleh Raft ketika log siap diterapkan.
func (fsm *SQLiteFSM) Apply(le *raft.Log) any {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Idempotensi: lewati log lama
	if le.Index <= fsm.lastApplied {
		// log.Printf("Log #%d sudah diterapkan, skip.", le.Index)
		return nil
	}

	var cmd Command
	if err := json.Unmarshal(le.Data, &cmd); err != nil {
		log.Printf("Unmarshal perintah dari log Raft gagal: %v", err)
		return fmt.Errorf("unmarshal perintah: %w", err)
	}

	sqlText := strings.TrimSpace(cmd.SQL)
	if sqlText == "" {
		// Jangan eksekusi SQL kosong
		log.Printf("Peringatan: SQL kosong pada index %d, diabaikan.", le.Index)
		fsm.lastApplied = le.Index
		fsm.updateLastApplied(le.Index)
		return nil
	}

	// Eksekusi
	if _, err := fsm.db.Exec(sqlText); err != nil {
		log.Printf("Kesalahan saat eksekusi SQL (idx=%d): %v; sql=%q", le.Index, err, limitLen(sqlText, 200))
		return fmt.Errorf("exec sql: %w", err)
	}

	fsm.lastApplied = le.Index
	fsm.updateLastApplied(le.Index)
	return nil
}

// Snapshot buat snapshot konsisten.
// Gunakan VACUUM INTO jika ada (SQLite >= 3.27); fallback ke copy file biasa.
func (fsm *SQLiteFSM) Snapshot() (raft.FSMSnapshot, error) {
	// CHANGE: pakai full Lock demi konsistensi ketika VACUUM/Checkpoint
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	tmp, err := os.CreateTemp("", "sqlite_snapshot_*.db")
	if err != nil {
		return nil, fmt.Errorf("buat file sementara snapshot: %w", err)
	}
	tmpName := tmp.Name()
	_ = tmp.Close()

	// gunakan literal yang di-quote
	vacuumSQL := fmt.Sprintf(`VACUUM INTO %q;`, tmpName)
	if _, err := fsm.db.Exec(vacuumSQL); err != nil {
		// pastikan WAL ter-flush dulu sebelum fallback copy
		// (TRUNCATE > FULL supaya wal file kosong)
		_, _ = fsm.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`)

		// Bersihkan file tmp yang dibuat sebelumnya
		_ = os.Remove(tmpName)

		if copyErr := copyFile(fsm.dbPath, tmpName); copyErr != nil {
			return nil, fmt.Errorf("snapshot gagal (vacuum=%v, copy=%v)", err, copyErr)
		}
	}

	log.Printf("Snapshot SQLite dibuat di: %s", tmpName)
	return &SQLiteSnapshot{snapshotPath: tmpName}, nil
}

// Restore timpa file DB dengan snapshot & reload lastApplied
func (fsm *SQLiteFSM) Restore(rc io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	defer rc.Close()

	log.Printf("Memulai restore database SQLite dari snapshot...")

	// Tutup koneksi lama bila ada
	if fsm.db != nil {
		if err := fsm.db.Close(); err != nil {
			log.Printf("Peringatan: gagal menutup koneksi DB sebelum restore: %v", err)
		}
	}

	// Timpa file database
	tmp := fsm.dbPath + ".restore.tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("buat file sementara restore: %w", err)
	}
	if _, err := io.Copy(out, rc); err != nil {
		out.Close()
		os.Remove(tmp)
		return fmt.Errorf("salin snapshot ke file sementara: %w", err)
	}
	out.Close()

	// Replace atomically
	if err := os.Rename(tmp, fsm.dbPath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename file restore: %w", err)
	}

	// Reopen dengan DSN yang sama (bukan plain path)
	db, err := openSQLite(fsm.dbPath)
	if err != nil {
		return fmt.Errorf("buka DB setelah restore: %w", err)
	}
	fsm.db = db
	fsm.initMeta() // <-- penting: set ulang lastApplied dari raft_meta

	log.Printf("Restore selesai ke: %s (lastApplied=%d)", fsm.dbPath, fsm.lastApplied)
	return nil
}

// SQLiteSnapshot implementasi FSMSnapshot – salin file snapshot ke sink, lalu bersihkan.
type SQLiteSnapshot struct {
	snapshotPath string
}

var _ raft.FSMSnapshot = (*SQLiteSnapshot)(nil)

// Persist menyalin snapshot ke sink Raft.
func (s *SQLiteSnapshot) Persist(sink raft.SnapshotSink) error {
	src, err := os.Open(s.snapshotPath)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("buka snapshot: %w", err)
	}
	defer src.Close()

	if _, err := io.Copy(sink, src); err != nil {
		sink.Cancel()
		return fmt.Errorf("copy snapshot: %w", err)
	}
	if err := sink.Close(); err != nil {
		return err
	}
	return nil
}

// Release membersihkan snapshot file setelah disimpan.
func (s *SQLiteSnapshot) Release() {
	if s.snapshotPath != "" {
		_ = os.Remove(s.snapshotPath)
	}
}

// --- util kecil ---

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func limitLen(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}

// Tambah helper reuse DSN:
func openSQLite(dbPath string) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"file:%s?_foreign_keys=on&_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL",
		dbPath,
	)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	// (Opsional) PRAGMA redundan—tak masalah kalau dipanggil ulang:
	_, _ = db.Exec(`PRAGMA journal_mode=WAL;`)
	_, _ = db.Exec(`PRAGMA synchronous=NORMAL;`)
	_, _ = db.Exec(`PRAGMA foreign_keys=ON;`)
	_, _ = db.Exec(`PRAGMA busy_timeout=5000;`)
	return db, nil
}
