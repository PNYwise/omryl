package internal

// Command merepresentasikan perintah SQL yang akan direplikasi.
type Command struct {
	SQL string `json:"sql"`
}

type joinReq struct {
	ID       string `json:"id"`
	RaftAddr string `json:"address"` // raft addr
	HTTPAddr string `json:"http"`    // http addr
}
