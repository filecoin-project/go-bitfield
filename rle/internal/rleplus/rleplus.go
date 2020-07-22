package rleplus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"golang.org/x/xerrors"
)

// Version is the 2 lowest bits of this constant
const Version = 0

var (
	// ErrRunLengthTooLarge - data implies a run-length which isn't supported
	ErrRunLengthTooLarge = fmt.Errorf("run length too large for RLE+ version %d", Version)

	// ErrDecode - invalid encoding for this version
	ErrDecode = fmt.Errorf("invalid encoding for RLE+ version %d", Version)

	// ErrWrongVersion - wrong version of RLE+
	ErrWrongVersion = errors.New("invalid RLE+ version")
)

// Encode returns the RLE+ representation of the provided integers.
// Also returned is the number of bits required by this encoding,
// which is not necessarily on a byte boundary.
//
// The RLE+ spec is here: https://github.com/filecoin-project/specs/blob/master/data-structures.md#rle-bitset-encoding
// and is described by the BNF Grammar:
//
//    <encoding> ::= <header> <blocks>
//    <header> ::= <version> <bit>
//    <version> ::= "00"
//    <blocks> ::= <block> <blocks> | ""
//    <block> ::= <block_single> | <block_short> | <block_long>
//    <block_single> ::= "1"
//    <block_short> ::= "01" <bit> <bit> <bit> <bit>
//    <block_long> ::= "00" <unsigned_varint>
//    <bit> ::= "0" | "1"
//
// Filecoin specific:
// The encoding is returned as a []byte, each byte packed starting with the low-order bit (LSB0)
func Encode(ints []uint64) ([]byte, uint, error) {
	v := BitVector{BytePacking: LSB0}
	firstBit, runs := RunLengths(ints)

	// Add version header
	v.Extend(Version, 2, LSB0)

	v.Push(firstBit)

	for _, run := range runs {
		switch {
		case run == 1:
			v.Push(1)
		case run < 16:
			v.Push(0)
			v.Push(1)
			v.Extend(byte(run), 4, LSB0)
		case run >= 16:
			v.Push(0)
			v.Push(0)
			// 10 bytes needed to encode MaxUint64
			buf := make([]byte, 10)
			numBytes := binary.PutUvarint(buf, run)
			for i := 0; i < numBytes; i++ {
				v.Extend(buf[i], 8, LSB0)
			}
		default:
			return nil, 0, ErrRunLengthTooLarge
		}
	}

	// Minimally encode.
	v.Trim()

	return v.Buf, v.Len, nil
}

// Decode returns integers represented by the given RLE+ encoding
//
// The passed []byte should be packed in LSB0 bit numbering
func Decode(buf []byte) (ints []uint64, err error) {
	if len(buf) == 0 {
		return
	} else if buf[len(buf)-1] == 0 {
		// trailing zeros bytes not allowed.
		return nil, xerrors.Errorf("not minimally encoded: %w", ErrDecode)
	}

	v := NewBitVector(buf, LSB0)
	take := v.Iterator(LSB0)

	// Read version and check
	// Version check
	ver := take(2)
	if ver != Version {
		return nil, ErrWrongVersion
	}

	curIdx := uint64(0)
	curBit := take(1)
	var runLength int
	done := false

	for !done {
		y := take(1)
		switch y {
		case 1:
			runLength = 1
		case 0:
			val := take(1)

			if val == 1 {
				// short block
				runLength = int(take(4))
			} else {
				// Modified from the go standard library. Copyright the Go Authors and
				// released under the BSD License.
				var x uint64
				var s uint
				for i := 0; ; i++ {
					if i == 10 {
						return nil, xerrors.Errorf("run too long: %w", ErrDecode)
					}
					b := take(8)
					if b < 0x80 {
						if i > 9 || i == 9 && b > 1 {
							return nil, xerrors.Errorf("run too long: %w", ErrDecode)
						} else if b == 0 && s > 0 {
							return nil, xerrors.Errorf("invalid run: %w", ErrDecode)
						}
						x |= uint64(b) << s
						break
					}
					x |= uint64(b&0x7f) << s
					s += 7
				}

				if x == 0 {
					done = true
				}
				runLength = int(x)
			}
		}

		if curBit == 1 {
			for j := 0; j < runLength; j++ {
				ints = append(ints, curIdx+uint64(j))
			}
		}
		curIdx += uint64(runLength)
		curBit = 1 - curBit
	}

	return
}

// RunLengths transforms integers into its bit-set-run-length representation.
//
// A set of unsigned integers { 0, 2, 4, 5, 6 } can be thought of as
// indices into a bitset { 1, 0, 1, 0, 1, 1, 1 } where bitset[index] == 1.
//
// The bit set run lengths of this set would then be { 1, 1, 1, 1, 3 },
// representing lengths of runs alternating between 1 and 0, starting
// with a first bit of 1.
//
// Duplicated numbers are ignored.
//
// This is a helper function for Encode()
func RunLengths(ints []uint64) (firstBit byte, runs []uint64) {
	if len(ints) == 0 {
		return
	}

	// Sort our incoming numbers
	sort.Slice(ints, func(i, j int) bool { return ints[i] < ints[j] })

	prev := ints[0]

	// Initialize our return value
	if prev == 0 {
		firstBit = 1
	}

	if firstBit == 0 {
		// first run of zeroes
		runs = append(runs, prev)
	}
	runs = append(runs, 1)

	for _, cur := range ints[1:] {
		delta := cur - prev
		switch {
		case delta == 1:
			runs[len(runs)-1]++
		case delta > 1:
			// add run of zeroes if there is a gap
			runs = append(runs, delta-1)
			runs = append(runs, 1)
		default:
			// repeated number?
		}
		prev = cur
	}
	return
}
