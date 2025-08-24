package auth

import (
	"net/http"
)

// IPublicAuth interface untuk otentikasi public antar node Raft.
type IPublicAuth interface {
	// SignRequest(req *http.Request, body []byte)
	VerifyRequest(r *http.Request) bool
}

// PublicAuth mewakili otentikasi publik sederhana menggunakan token API.
// Ini digunakan untuk memastikan bahwa permintaan publik memiliki token yang benar.
// Token API publik sederhana disertakan dalam header "X-Token".
type PublicAuth struct {
	PublicAPIToken string // token API publik sederhana
}

// VerifyRequest versi ringkas (true/false).
func (a PublicAuth) VerifyRequest(req *http.Request) bool {
	return a.verifyOrError(req) == nil
}

// VerifyOrError verifikasi detail (mengembalikan alasan jika gagal).
func (a PublicAuth) verifyOrError(r *http.Request) error {
	if a.PublicAPIToken == "" {
		return nil // tidak ada otentikasi diperlukan
	}
	if r.Header.Get("X-Token") == a.PublicAPIToken {
		return nil
	}
	return ErrAuthBadToken
}


