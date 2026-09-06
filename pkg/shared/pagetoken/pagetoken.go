// Package pagetoken implements the opaque pagination cursor from 003.1
// ("Pagination"): list results are ordered by `name` ascending, the page size
// defaults to 50 and is capped at 1000, and `page_token` is an opaque string the
// client must echo back unchanged. The token carries the last name of the
// previous page plus a hash of the query that produced it, so replaying a token
// against a different filter/order is rejected.
package pagetoken

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

const (
	// DefaultSize is used when the request leaves page_size at 0.
	DefaultSize = 50
	// MaxSize caps page_size (003.1).
	MaxSize = 1000
)

// ClampSize normalises a client-supplied page size: 0 -> DefaultSize, negative
// -> DefaultSize, anything above MaxSize -> MaxSize.
func ClampSize(requested int32) int {
	switch {
	case requested <= 0:
		return DefaultSize
	case requested > MaxSize:
		return MaxSize
	default:
		return int(requested)
	}
}

// token is the wire form carried (base64-encoded) in page_token.
type token struct {
	// After is the `name` of the last row on the previous page. The next page is
	// everything with name > After.
	After string `json:"a"`
	// QueryHash binds the token to the exact query (filter + order) that issued
	// it. A different query rejects the token.
	QueryHash string `json:"q"`
}

// QueryKey is a stable fingerprint of the list query the token belongs to.
// Callers build it from the request's filter/selector and any order option;
// the parent resource name should be included when a list is scoped.
func QueryKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		// length-prefix so ("a","bc") and ("ab","c") differ
		fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))[:16]
}

// Encode builds the page_token for the page that follows lastName. Returns "" for
// an empty lastName (no further pages).
func Encode(lastName, queryKey string) string {
	if lastName == "" {
		return ""
	}
	b, _ := json.Marshal(token{After: lastName, QueryHash: queryKey})
	return base64.RawURLEncoding.EncodeToString(b)
}

// Decode parses a client-supplied page_token and checks it belongs to queryKey.
// An empty token means "first page" and returns after == "". A malformed token,
// or one minted for a different query, is an INVALID_ARGUMENT domain error on
// the "page_token" field.
func Decode(raw, queryKey string) (after string, err error) {
	if raw == "" {
		return "", nil
	}
	b, decErr := base64.RawURLEncoding.DecodeString(raw)
	if decErr != nil {
		return "", errs.InvalidField("page_token", "malformed pagination token")
	}
	var tk token
	if json.Unmarshal(b, &tk) != nil {
		return "", errs.InvalidField("page_token", "malformed pagination token")
	}
	if tk.QueryHash != queryKey {
		return "", errs.InvalidField("page_token",
			"pagination token does not match this query; restart from the first page")
	}
	return tk.After, nil
}
