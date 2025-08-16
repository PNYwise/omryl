package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/hashicorp/raft"
)

// SQLiteFSM adalah Finite State Machine (FSM) untuk Raft, yang mengelola database SQLite.
type SQLiteFSM struct {
	dbPath      string
	db          *sql.DB
	mu          sync.Mutex // Melindungi akses ke database SQLite
	lastApplied uint64
}

// NewSQLiteFSM membuat instance SQLiteFSM baru dan menginisialisasi database.
func NewSQLiteFSM(dbPath string) *SQLiteFSM {
	fsm := &SQLiteFSM{dbPath: dbPath}
	var err error
	fsm.db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Gagal membuka database SQLite untuk FSM: %v", err)
	}

	// Buat tabel contoh jika belum ada.
	// Ini adalah perintah DDL yang juga akan direplikasi jika diajukan melalui Raft.
	_, err = fsm.db.Exec(`
		CREATE TABLE IF NOT EXISTS items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			quantity INTEGER NOT NULL
		);
	`)
	if err != nil {
		log.Fatalf("Gagal membuat tabel di database FSM: %v", err)
	}
	fsm.InitMeta()
	log.Printf("Database SQLite FSM berhasil diinisialisasi di: %s", dbPath)
	return fsm
}

// InitMeta Tambahkan fungsi untuk inisialisasi dan load lastApplied
func (fsm *SQLiteFSM) InitMeta() {
	_, err := fsm.db.Exec(`CREATE TABLE IF NOT EXISTS raft_meta (id INTEGER PRIMARY KEY, last_applied INTEGER NOT NULL DEFAULT 0);`)
	if err != nil {
		log.Fatalf("Gagal membuat tabel raft_meta: %v", err)
	}
	var idx uint64
	err = fsm.db.QueryRow(`SELECT last_applied FROM raft_meta WHERE id=1;`).Scan(&idx)
	if err == sql.ErrNoRows {
		_, err = fsm.db.Exec(`INSERT INTO raft_meta (id, last_applied) VALUES (1, 0);`)
		if err != nil {
			log.Fatalf("Gagal insert initial raft_meta: %v", err)
		}
		fsm.lastApplied = 0
	} else if err != nil {
		log.Fatalf("Gagal membaca raft_meta: %v", err)
	} else {
		fsm.lastApplied = idx
	}
}

// UpdateLastApplied memperbarui nilai last_applied di tabel raft_meta.
func (fsm *SQLiteFSM) UpdateLastApplied(idx uint64) {
	_, err := fsm.db.Exec(`UPDATE raft_meta SET last_applied=? WHERE id=1;`, idx)
	if err != nil {
		log.Printf("Gagal update last_applied: %v", err)
	}
}

// Apply dipanggil oleh Raft ketika sebuah log telah disepakati dan siap untuk diterapkan ke FSM.
func (fsm *SQLiteFSM) Apply(logEntry *raft.Log) any {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if logEntry.Index <= fsm.lastApplied {
		log.Printf("Log #%d sudah diterapkan, skip.", logEntry.Index)
		return nil
	}

	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("Gagal meng-unmarshal perintah dari log Raft: %v", err)
		return fmt.Errorf("gagal meng-unmarshal perintah: %w", err)
	}

	log.Printf("Menerapkan perintah SQL: %s", cmd.SQL)
	_, err := fsm.db.Exec(cmd.SQL)
	if err != nil {
		log.Printf("Kesalahan saat mengeksekusi SQL '%s': %v", cmd.SQL, err)
		return fmt.Errorf("kesalahan saat mengeksekusi SQL: %w", err)
	}

	fsm.lastApplied = logEntry.Index
	fsm.UpdateLastApplied(logEntry.Index)
	return nil
}

// Snapshot digunakan untuk membuat snapshot FSM (keadaan database).
// Ini penting untuk pemangkasan log Raft dan pemulihan node.
func (fsm *SQLiteFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Untuk SQLite, snapshot berarti menyalin seluruh file database.
	// Ini bisa menjadi operasi yang mahal untuk database besar atau yang sangat aktif.
	// Untuk aplikasi produksi yang sangat besar, pertimbangkan menggunakan:
	// 1. SQLite Online Backup API (melalui cgo) untuk backup non-blocking.
	// 2. Pendekatan log-structured storage atau database yang dirancang untuk snapshotting.
	// 3. Hanya mengambil snapshot jika database tidak aktif menulis.

	tmpFile, err := os.CreateTemp("", "sqlite_snapshot_*.db")
	if err != nil {
		return nil, fmt.Errorf("gagal membuat file sementara untuk snapshot: %w", err)
	}
	defer tmpFile.Close() // Pastikan file temp ditutup

	sourceFile, err := os.Open(fsm.dbPath)
	if err != nil {
		os.Remove(tmpFile.Name()) // Bersihkan file temp jika gagal
		return nil, fmt.Errorf("gagal membuka database sumber untuk snapshot: %w", err)
	}
	defer sourceFile.Close()

	if _, err := io.Copy(tmpFile, sourceFile); err != nil {
		os.Remove(tmpFile.Name()) // Bersihkan file temp jika gagal
		return nil, fmt.Errorf("gagal menyalin database untuk snapshot: %w", err)
	}

	log.Printf("Snapshot SQLite dibuat di: %s", tmpFile.Name())
	return &SQLiteSnapshot{snapshotPath: tmpFile.Name()}, nil
}

// Restore dipanggil untuk mengembalikan FSM dari snapshot.
func (fsm *SQLiteFSM) Restore(rc io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	defer rc.Close()

	log.Printf("Memulai pemulihan database SQLite dari snapshot...")

	// Tutup koneksi DB yang ada sebelum menghapus dan menulis ulang file.
	if fsm.db != nil {
		if err := fsm.db.Close(); err != nil {
			log.Printf("Peringatan: Gagal menutup koneksi DB yang ada sebelum restore: %v", err)
		}
	}

	// Hapus file DB yang ada
	if err := os.Remove(fsm.dbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("gagal menghapus database yang ada sebelum restore: %w", err)
	}

	// Buat file baru dan salin data snapshot ke dalamnya
	newFile, err := os.Create(fsm.dbPath)
	if err != nil {
		return fmt.Errorf("gagal membuat file database untuk restore: %w", err)
	}
	defer newFile.Close()

	if _, err := io.Copy(newFile, rc); err != nil {
		return fmt.Errorf("gagal menyalin data snapshot ke file database baru: %w", err)
	}

	// Buka kembali koneksi DB
	fsm.db, err = sql.Open("sqlite3", fsm.dbPath)
	if err != nil {
		return fmt.Errorf("gagal membuka database SQLite yang dipulihkan: %w", err)
	}
	log.Printf("Database SQLite berhasil dipulihkan ke: %s", fsm.dbPath)
	return nil
}
