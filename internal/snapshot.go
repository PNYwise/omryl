package internal

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/hashicorp/raft"
)

// SQLiteSnapshot adalah implementasi raft.FSMSnapshot.
type SQLiteSnapshot struct {
	snapshotPath string
}

// Persist digunakan untuk menulis data snapshot ke SnapshotSink Raft.
func (s *SQLiteSnapshot) Persist(sink raft.SnapshotSink) error {
	file, err := os.Open(s.snapshotPath)
	if err != nil {
		sink.Cancel() // Batalkan sink jika gagal membuka file
		return fmt.Errorf("gagal membuka file snapshot: %w", err)
	}
	defer os.Remove(s.snapshotPath) // Hapus file sementara setelah digunakan
	defer file.Close()

	if _, err := io.Copy(sink, file); err != nil {
		sink.Cancel() // Batalkan sink jika gagal menyalin
		return fmt.Errorf("gagal menyalin snapshot ke sink: %w", err)
	}
	log.Printf("Snapshot SQLite berhasil di-persist dari: %s", s.snapshotPath)
	return sink.Close()
}

// Release dipanggil setelah snapshot berhasil di-persist atau dibatalkan.
func (s *SQLiteSnapshot) Release() {
	// Tidak ada yang perlu dirilis di sini karena file temp sudah dihapus di Persist.
}
