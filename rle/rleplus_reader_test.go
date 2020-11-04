package rlepluslazy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeTable(t *testing.T) {
	for b, decode := range decodeTable {
		{
			i := 0
			for b&0b1 == 0b1 {
				i++
				b >>= 1
			}
			if i != 0 {
				// run of ones
				assert.EqualValues(t, i-1, decode.i, "invalid count")
				assert.EqualValues(t, 1, decode.length, "invalid length")
				assert.EqualValues(t, i, decode.n, "invalid bits to take")
				assert.False(t, decode.varint, "is not varint")
				continue
			}
		}
		if b&0b11 == 0b10 {
			// run of len up to 15
			assert.EqualValues(t, 0, decode.i, "invalid count")
			assert.EqualValues(t, b>>2, decode.length, "invalid length")
			assert.EqualValues(t, 6, decode.n, "invalid bits to take")
			assert.False(t, decode.varint, "is not varint")
			continue
		}
		if b&0b11 == 0b00 {
			// varint
			assert.EqualValues(t, 0, decode.i, "invalid count")
			assert.EqualValues(t, 0, decode.length, "invalid length")
			assert.EqualValues(t, 2, decode.n, "invalid bits to take")
			assert.True(t, decode.varint, "is not varint")
			continue
		}
		t.Fatalf("not handled")
	}
}

func TestAll8bit(t *testing.T) {
	runs := []Run{}
	for k := uint64(1); k < 64; k++ {
		for i := 1; i < 7; i++ {
			for j := 0; j < i; j++ {
				runs = append(runs, Run{Len: 1})
			}
			runs = append(runs, Run{Len: k})
		}
	}
	for i := uint64(3); i < 256; i++ {
		runs = append(runs, Run{Len: i})
	}
	// normalize
	for i := range runs {
		runs[i].Val = i%2 == 0
	}

	origin := &RunSliceIterator{Runs: runs}
	enc, err := EncodeRuns(origin, nil)
	assert.NoError(t, err)

	origin = &RunSliceIterator{Runs: runs}
	originSlice, err := SliceFromRuns(origin)
	assert.NoError(t, err)

	decoded, err := DecodeRLE(enc)
	assert.NoError(t, err)

	decodedSlice, err := SliceFromRuns(decoded)
	assert.NoError(t, err)
	assert.Equal(t, originSlice, decodedSlice)
}
