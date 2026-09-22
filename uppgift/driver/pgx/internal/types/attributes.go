package types

import (
	"encoding/json/v2"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	_ pgtype.BytesValuer  = (Attributes)(nil)
	_ pgtype.BytesScanner = (*Attributes)(nil)
)

// Attributes wraps a map[string]string metadata map with pgx byte scanner and valuer support.
type Attributes map[string]string

// BytesValue implements pgtype.BytesValuer for pgx.
func (a Attributes) BytesValue() ([]byte, error) {
	if len(a) == 0 {
		return nil, nil
	}
	return json.Marshal(a)
}

// ScanBytes implements pgtype.BytesScanner for pgx binary format.
func (a *Attributes) ScanBytes(v []byte) error {
	if len(v) == 0 || string(v) == "null" {
		*a = nil
		return nil
	}
	data := v
	if len(data) > 0 && data[0] == 1 {
		data = data[1:]
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("unmarshal Attributes from bytes: %w", err)
	}
	*a = m
	return nil
}
