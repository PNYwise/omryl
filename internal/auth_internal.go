package internal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// InternalAuth mewakili otentikasi internal antar node Raft.
// Ini digunakan untuk memastikan bahwa permintaan antar node aman dan terverifikasi.
// Ini menggunakan HMAC dengan shared secret untuk menandatangani permintaan.
// NodeID adalah ID unik untuk node yang mengirim permintaan.
// Secret adalah shared secret yang digunakan untuk HMAC.
// Skew adalah toleransi clock skew antara node (mis. 60 detik).
// Allowlist opsional: daftar ID peer yang diizinkan untuk mengirim permintaan.
type InternalAuth struct {
	NodeID    string           // ID node pengirim (untuk SignRequest)
	Secret    string           // shared secret antar node
	Skew      time.Duration    // toleransi clock skew, mis. 60 * time.Second
	Allowlist map[string]bool  // opsional: ID peer yang diizinkan; nil = terima semua
	Now       func() time.Time // opsional: stub untuk pengujian; default time.Now().UTC
}

func (a InternalAuth) nowUTC() time.Time {
	if a.Now != nil {
		return a.Now().UTC()
	}
	return time.Now().UTC()
}

// canonicalTarget mengembalikan path + optional query (RequestURI) agar tanda tangan
// mengikat baik path maupun query string.
func canonicalTarget(r *http.Request) string {
	uri := r.URL.RequestURI()
	if uri == "" {
		uri = r.URL.Path
	}
	return uri
}

func (a InternalAuth) signString(method, target string, body []byte, ts string) string {
	mac := hmac.New(sha256.New, []byte(a.Secret))
	// method \n target \n body \n ts
	mac.Write([]byte(strings.ToUpper(method)))
	mac.Write([]byte("\n"))
	mac.Write([]byte(target))
	mac.Write([]byte("\n"))
	mac.Write(body)
	mac.Write([]byte("\n"))
	mac.Write([]byte(ts))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyOrError verifikasi detail (mengembalikan alasan jika gagal).
func (a InternalAuth) VerifyOrError(r *http.Request, body []byte) error {
	id := r.Header.Get("X-Internal-Id")
	ts := r.Header.Get("X-Internal-Ts")
	sig := r.Header.Get("X-Internal-Sign")

	if id == "" || ts == "" || sig == "" {
		return ErrAuthMissingHeaders
	}
	if a.Allowlist != nil && !a.Allowlist[id] {
		return ErrAuthPeerNotAllowed
	}

	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrAuthBadTimestamp
	}

	now := a.nowUTC()
	when := time.Unix(sec, 0).UTC()
	dt := now.Sub(when)
	if dt < -a.Skew || dt > a.Skew {
		return ErrAuthSkew
	}

	expected := a.signString(r.Method, canonicalTarget(r), body, ts)
	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return ErrAuthBadSignature
	}
	return nil
}

// VerifyRequest versi ringkas (true/false).
func (a InternalAuth) VerifyRequest(r *http.Request, body []byte) bool {
	return a.VerifyOrError(r, body) == nil
}

// SignRequest menandatangani permintaan internal.
func (a InternalAuth) SignRequest(req *http.Request, rawBody []byte) {
	ts := strconv.FormatInt(a.nowUTC().Unix(), 10)
	target := canonicalTarget(req)
	sig := a.signString(req.Method, target, rawBody, ts)
	req.Header.Set("X-Internal-Id", a.NodeID)
	req.Header.Set("X-Internal-Ts", ts)
	req.Header.Set("X-Internal-Sign", sig)
}

// --- Errors (opsional typed errors) ---
var (
	// ErrAuthMissingHeaders jika permintaan tidak memiliki header yang diperlukan.
	ErrAuthMissingHeaders = &AuthError{"missing headers"}

	// ErrAuthPeerNotAllowed jika ID peer tidak ada dalam allowlist.
	ErrAuthPeerNotAllowed = &AuthError{"peer not allowed"}
	
	// ErrAuthBadTimestamp jika timestamp tidak valid (tidak bisa di-parse).
	ErrAuthBadTimestamp   = &AuthError{"bad timestamp"}

	// ErrAuthSkew jika timestamp terlalu jauh dari waktu sekarang (clock skew).
	ErrAuthSkew           = &AuthError{"timestamp out of range"}

	// ErrAuthBadSignature jika signature tidak cocok.
	ErrAuthBadSignature   = &AuthError{"bad signature"}
)

// AuthError mewakili kesalahan otentikasi internal.
type AuthError struct{ msg string }

func (e *AuthError) Error() string { return "internal auth: " + e.msg }
