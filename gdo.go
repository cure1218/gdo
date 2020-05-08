package gdo

import (
	"crypto/sha512"
	"database/sql"
)

//================================================================
// GDO
//================================================================
type Gdo interface {
	Ping() error
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) (*sql.Row, error)
	Exec(string, ...interface{}) (sql.Result, error)
	Begin() (*sql.Tx, error)
	Close() error
}

//================================================================
//
//================================================================
func Sha512BinaryStr(s string) string {
	hash := sha512.Sum512([]byte(s))
	checksum := make([]byte, sha512.Size)
	for i := range hash {
		checksum[i] = hash[i]
	}

	return string(checksum)
}
