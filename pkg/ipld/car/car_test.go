package car

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-leb128"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const CIDLen = 36

func TestCIDLen(t *testing.T) {
	// Check whether codecs actually result in a CID sized CIDLen.
	// This is important for our allocation strategies during merklerization.
	codecs := []uint64{
		uint64(multicodec.Raw),
		uint64(multicodec.DagCbor),
	}
	for _, codec := range codecs {
		t.Run(fmt.Sprintf("Codec_%#x", codec), func(t *testing.T) {
			builder := cid.V1Builder{
				Codec:  codec,
				MhType: multihash.SHA2_256,
			}
			id, err := builder.Sum(nil)
			require.NoError(t, err)
			assert.Equal(t, id.ByteLen(), CIDLen)
		})
	}
}

func TestIdentityCIDStr(t *testing.T) {
	assert.Equal(t, "bafkqaaa", DummyCID.String())
}

func TestLeb128Len(t *testing.T) {
	cases := []struct {
		n   uint64
		len int
	}{
		{0, 1},
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 1},
		{5, 1},
		{63, 1},
		{64, 1},
		{65, 1},
		{100, 1},
		{127, 1},
		{128, 2},
		{129, 2},
		{2141192192, 5},
		{^uint64(0), 10},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("N_%d", tc.n), func(t *testing.T) {
			assert.Equal(t, tc.len, leb128Len(tc.n))
		})
	}
}

// Sanity-check asserting that Go Uvarint and LEB128 are compatible.
func TestUvarintIsLeb128(t *testing.T) {
	cases := []uint64{
		0, 1, 2, 3, 4, 5, 63, 64,
		65, 100, 127, 128, 129,
		2141192192, ^uint64(0),
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("N_%d", tc), func(t *testing.T) {
			var v0Buf [binary.MaxVarintLen64]byte
			v0 := v0Buf[:binary.PutUvarint(v0Buf[:], tc)]
			v1 := leb128.FromUInt64(tc)
			assert.Equal(t, v0, v1)
		})
	}
}
