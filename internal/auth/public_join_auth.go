package auth

import (
	"net/http"
)

// IPublicJoinAuth interface untuk otentikasi public antar node Raft.
type IPublicJoinAuth interface {
	// SignRequest(req *http.Request, body []byte)
	VerifyRequest(r *http.Request) bool
}

// PublicJoinAuth mewakili otentikasi publik sederhana menggunakan token API.
// Ini digunakan untuk memastikan bahwa permintaan publik memiliki token yang benar.
// Token API publik sederhana disertakan dalam header "X-Token".
type PublicJoinAuth struct {
	PublicJoinAPIToken string // token API publik sederhana
}

// VerifyRequest versi ringkas (true/false).
func (a PublicJoinAuth) VerifyRequest(req *http.Request) bool {
	return a.verifyOrError(req) == nil
}

// VerifyOrError verifikasi detail (mengembalikan alasan jika gagal).
func (a PublicJoinAuth) verifyOrError(r *http.Request) error {
	if a.PublicJoinAPIToken == "" {
		return nil // tidak ada otentikasi diperlukan
	}
	if r.Header.Get("X-Token-Join") == a.PublicJoinAPIToken {
		return nil
	}
	return ErrAuthBadToken
}
