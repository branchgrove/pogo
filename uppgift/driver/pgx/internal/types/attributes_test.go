package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttributesBytesValuer(t *testing.T) {
	// Nil attributes returns nil bytes
	var nilAttr Attributes
	bytesVal, err := nilAttr.BytesValue()
	require.NoError(t, err)
	assert.Nil(t, bytesVal)

	// Non-nil attributes
	attr := Attributes{"foo": "bar", "env": "prod"}
	bytesVal, err = attr.BytesValue()
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar","env":"prod"}`, string(bytesVal))
}

func TestAttributesScanBytes(t *testing.T) {
	var attr Attributes

	// Scan nil / empty
	err := attr.ScanBytes(nil)
	require.NoError(t, err)
	assert.Nil(t, attr)

	// Scan normal json bytes
	err = attr.ScanBytes([]byte(`{"key":"value"}`))
	require.NoError(t, err)
	assert.Equal(t, "value", attr["key"])

	// Scan jsonb bytes with 0x01 version prefix
	err = attr.ScanBytes(append([]byte{1}, []byte(`{"env":"prod"}`)...))
	require.NoError(t, err)
	assert.Equal(t, "prod", attr["env"])

	// Invalid json bytes
	err = attr.ScanBytes([]byte(`{invalid`))
	require.Error(t, err)
}
