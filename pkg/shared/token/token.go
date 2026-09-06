// Package token mints and hashes the opaque bearer tokens agents present to
// Franz (003.9). Franz stores only the hash; the plaintext is shown once.
package token

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// Prefix marks a Franz agent token. It is part of the plaintext and the hash
// input.
const Prefix = "frnat_"

const secretBytes = 32

// Generate returns a fresh (plaintext, hash) pair. Give the plaintext to the
// caller once; persist only the hash.
func Generate() (plaintext, hash string, err error) {
	buf := make([]byte, secretBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	plaintext = Prefix + base64.RawURLEncoding.EncodeToString(buf)
	return plaintext, Hash(plaintext), nil
}

// Hash is the stable digest stored for a token. Comparison is on the hash, never
// the plaintext.
func Hash(plaintext string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(plaintext)))
	return hex.EncodeToString(sum[:])
}
