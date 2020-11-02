package rlepluslazy

import (
	"golang.org/x/xerrors"
)

type decodeInfo struct {
	length byte // length of the run
	i      byte // i+1 is number of repeats of above run lengths
	n      byte // number of bits to read
	varint bool // varint signifies that futher bits need to be processed as a varint
}

func init() {
	buildDecodeTable()
}

// this is a LUT for all possible 6 bit codes and what they decode into
// possible combinations are:
// 0bxxxxxx1 - 1 run of 1
// 0bxxxxx11 - 2 runs of 1
// up to 0b111111 - 6 runs of 1
// 0bAAAA10 - 1 run of length 0bAAAA
// 0bxxxx00 - var int run, the decode value not defined in LUT
var decodeTable = [1 << 6]decodeInfo{}

func buildDecodeTable() {
	// runs of 1s 0bxxxxxx1, 0bxxxx11 ...
	for i := 1; i <= 6; i++ {
		for j := 0; j < (1<<6)>>i; j++ {
			idx := bitMasks[i] | byte(j<<i)
			decodeTable[idx] = decodeInfo{
				length: 1,
				i:      byte(i - 1),
				n:      byte(i),
			}
		}
	}

	// 01 + 4bit : run of 0 to 15
	for i := 0; i < 16; i++ {
		idx := 0b10 | i<<2
		decodeTable[idx] = decodeInfo{
			length: byte(i),
			i:      0,
			n:      6,
		}
	}
	// 00 + 4 bit
	for i := 0; i < 16; i++ {
		idx := 0b00 | i<<2
		decodeTable[idx] = decodeInfo{
			i:      0,
			n:      2,
			varint: true,
		}
	}
}

func DecodeRLE(buf []byte) (RunIterator, error) {
	if len(buf) > 0 && buf[len(buf)-1] == 0 {
		// trailing zeros bytes not allowed.
		return nil, xerrors.Errorf("not minimally encoded: %w", ErrDecode)
	}

	bv := readBitvec(buf)

	ver := bv.Get(2) // Read version
	if ver != Version {
		return nil, ErrWrongVersion
	}

	it := &rleIterator{bv: bv}

	// next run is previous in relation to prep
	// so we invert the value
	it.lastVal = bv.Get(1) != 1
	if err := it.prep(); err != nil {
		return nil, err
	}
	return it, nil
}

type rleIterator struct {
	bv     *rbitvec
	length uint64

	lastVal bool
	i       uint8
}

func (it *rleIterator) HasNext() bool {
	return it.length != 0
}

func (it *rleIterator) NextRun() (r Run, err error) {
	ret := Run{Len: it.length, Val: !it.lastVal}
	it.lastVal = ret.Val

	if it.i == 0 {
		err = it.prep()
	} else {
		it.i--
	}
	return ret, err
}

func (it *rleIterator) prep() error {
	idx := it.bv.Peek6()
	decode := decodeTable[idx]
	_ = it.bv.Get(decode.n)

	it.i = decode.i
	it.length = uint64(decode.length)
	if decode.varint {
		// Modified from the go standard library. Copyright the Go Authors and
		// released under the BSD License.
		var x uint64
		var s uint
		for i := 0; ; i++ {
			if i == 10 {
				return xerrors.Errorf("run too long: %w", ErrDecode)
			}
			b := it.bv.GetByte()
			if b < 0x80 {
				if i > 9 || i == 9 && b > 1 {
					return xerrors.Errorf("run too long: %w", ErrDecode)
				} else if b == 0 && s > 0 {
					return xerrors.Errorf("invalid run: %w", ErrDecode)
				}
				x |= uint64(b) << s
				break
			}
			x |= uint64(b&0x7f) << s
			s += 7
		}
		it.length = x
	}
	return nil
}
