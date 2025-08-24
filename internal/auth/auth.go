package auth

// AuthError mewakili kesalahan otentikasi internal.
type authError struct{ msg string }

func (e *authError) Error() string { return "internal auth: " + e.msg }

// --- Errors (opsional typed errors) ---
var (
	// ErrAuthMissingHeaders jika permintaan tidak memiliki header yang diperlukan.
	ErrAuthMissingHeaders = &authError{"missing headers"}

	// ErrAuthPeerNotAllowed jika ID peer tidak ada dalam allowlist.
	ErrAuthPeerNotAllowed = &authError{"peer not allowed"}

	// ErrAuthBadTimestamp jika timestamp tidak valid (tidak bisa di-parse).
	ErrAuthBadTimestamp = &authError{"bad timestamp"}

	// ErrAuthSkew jika timestamp terlalu jauh dari waktu sekarang (clock skew).
	ErrAuthSkew = &authError{"timestamp out of range"}

	// ErrAuthBadSignature jika signature tidak cocok.
	ErrAuthBadSignature = &authError{"bad signature"}

	// ErrAuthBadToken jika token tidak cocok.
	ErrAuthBadToken = &authError{"bad token"}
)
