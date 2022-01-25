package bitfield

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"testing"

	rlepluslazy "github.com/frrist/go-bitfield/rle"
	"github.com/stretchr/testify/require"
)

func benchmark(b *testing.B, cb func(b *testing.B, bf BitField)) {
	for _, size := range []int{
		0,
		1,
		10,
		1000,
		1000000,
	} {
		benchmarkSize(b, size, cb)
	}
}

func benchmarkSize(b *testing.B, size int, cb func(b *testing.B, bf BitField)) {
	b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
		ri := rlepluslazy.NewFromZipfDist(55, size)
		bf, err := NewFromIter(ri)
		if err != nil {
			b.Fatal(err)
		}
		b.Run("basic", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cb(b, bf)
			}
		})

		if size < 1 {
			return
		}

		// Set and unset some bits
		i := uint64(size / 10)
		bf.Set(i)
		bf.Set(i + 1)
		bf.Set(i * 2)
		bf.Unset(i / 2)
		bf.Unset(uint64(size) - 1)

		b.Run("modified", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cb(b, bf)
			}
		})
	})
}

func BenchmarkCount(b *testing.B) {
	benchmark(b, func(b *testing.B, bf BitField) {
		_, err := bf.Count()
		if err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkIsEmpty(b *testing.B) {
	benchmark(b, func(b *testing.B, bf BitField) {
		_, err := bf.IsEmpty()
		if err != nil {
			b.Fatal(err)
		}
	})
}

var Res uint64

func BenchmarkBigDecodeEncode(b *testing.B) {
	bb, err := base64.StdEncoding.DecodeString(bigBitfield)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		bitF, err := NewFromBytes(bb)
		if err != nil {
			b.Fatal(err)
		}
		{
			s, err := bitF.RunIterator()
			if err != nil {
				b.Fatal(err)
			}

			rle, err := rlepluslazy.EncodeRuns(s, []byte{})
			if err != nil {
				b.Fatal(err)
			}
			Res += uint64(rle[1])
		}

	}
}

func BenchmarkBigValidate(b *testing.B) {
	bb, err := base64.StdEncoding.DecodeString(bigBitfield)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bitF, err := NewFromBytes(bb)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = bitF.RunIterator()
	}
}

func BenchmarkBigAllocateSector(b *testing.B) {
	bb, err := base64.StdEncoding.DecodeString(bigBitfield)
	if err != nil {
		b.Fatal(err)
	}
	sectorNo := uint64(0)
	{
		bitF, err := NewFromBytes(bb)
		if err != nil {
			b.Fatal(err)
		}
		last, err := bitF.Last()
		if err != nil {
			b.Fatal(err)
		}
		sectorNo = last + 10
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bitF, err := NewFromBytes(bb)
		if err != nil {
			b.Fatal(err)
		}
		_, err = bitF.IsSet(sectorNo)
		if err != nil {
			b.Fatal(err)
		}
		bitF.Set(sectorNo)
		err = bitF.MarshalCBOR(&bytes.Buffer{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func bitfieldStats(t *testing.T, bf BitField) (size int, runs uint64, last uint64) {
	s, err := bf.RunIterator()
	if err != nil {
		t.Fatal(err)
	}
	for s.HasNext() {
		run, err := s.NextRun()
		if err != nil {
			t.Fatal(err)
		}
		runs++
		last += run.Len
	}
	size = len(bf.rle.Bytes())
	return
}

func TestFillBitfieldUpTo(t *testing.T) {
	bb, err := base64.StdEncoding.DecodeString(bigBitfield)
	if err != nil {
		t.Fatal(err)
	}
	bitF, err := NewFromBytes(bb)
	if err != nil {
		t.Fatal(err)
	}

	size, runs, last := bitfieldStats(t, bitF)
	t.Logf("current size: %d, runs: %d, last %d", size, runs, last)
	s, err := bitF.RunIterator()
	if err != nil {
		t.Fatal(err)
	}
	trimmed, err := rlepluslazy.Or(s, &rlepluslazy.RunSliceIterator{
		Runs: []rlepluslazy.Run{{Val: true, Len: last - 20<<10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	trimEnc, err := rlepluslazy.EncodeRuns(trimmed, nil)
	if err != nil {
		t.Fatal(err)
	}
	bitFTrim, err := NewFromBytes(trimEnc)
	if err != nil {
		t.Fatal(err)
	}
	size, runs, last = bitfieldStats(t, bitFTrim)
	t.Logf("trimed size: %d, runs: %d, last %d", size, runs, last)

}

// This test captures a common case where we need to merge a bunch of bitfields,
// but every nth bitfield is expected to be empty. For example, merging all
// faults, terminations, and unproven sectors.
func BenchmarkMultiMergeEmpty(b *testing.B) {
	var bfs []BitField

	for i := int64(0); i < 30; i++ {
		var bf BitField
		var err error
		switch i % 3 {
		case 0:
			bf = NewFromSet(nil)
		case 1:
			bf, err = NewFromIter(rlepluslazy.NewFromZipfDist(i, 10))
			require.NoError(b, err)
		case 2:
			bf, err = NewFromIter(rlepluslazy.NewFromZipfDist(i, 2000))
			require.NoError(b, err)
		default:
			panic("impossible")
		}
		bfs = append(bfs, bf)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merged, err := MultiMerge(bfs...)
		require.NoError(b, err)
		_, _ = merged.RunIterator()
	}

}

func BenchmarkBitfieldSubtractLargeElement(b *testing.B) {
	bfa := NewFromSet([]uint64{1, 2, math.MaxUint64 - 1})
	bfb := NewFromSet([]uint64{1})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := SubtractBitField(bfa, bfb)
		if err != nil {
			b.Fatal(err)
		}
	}
}
