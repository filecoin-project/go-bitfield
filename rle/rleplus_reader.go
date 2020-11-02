package rlepluslazy

import (
	"golang.org/x/xerrors"
)

type decodeInfo struct {
	runs   [6]Run
	i      byte // index of first decoded run
	n      byte // number of bits to read
	varint bool
}

var decodeTable = [1 << 6]decodeInfo{}

func init() {
	buildDecodeTable()
}
func buildDecodeTable() {
	// 1 : run of 1 bit
	var runs [6]Run
	for i := 0; i < 6; i++ {
		runs[i].Len = 1
	}

	for i := 1; i <= 6; i++ {
		for j := 0; j < 1<<6>>i; j++ {
			idx := bitMasks[i] | byte(j<<i)
			decodeTable[idx] = decodeInfo{
				runs: runs,
				i:    byte(i - 1),
				n:    byte(i),
			}
		}
	}

	// 01 + 4bit : run of 0 to 15
	for i := 0; i < 16; i++ {
		idx := 0b10 | i<<2
		decodeTable[idx] = decodeInfo{
			runs: [6]Run{{Len: uint64(i)}},
			i:    0,
			n:    6,
		}
	}
	// 00 + 4 bit
	for i := 0; i < 16; i++ {
		idx := 0b00 | i<<2
		decodeTable[idx] = decodeInfo{
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
	bv       *rbitvec
	nextRuns [6]Run

	lastVal bool
	i       uint8
}

func (it *rleIterator) HasNext() bool {
	return it.nextRuns[0].Valid()
}

func (it *rleIterator) NextRun() (r Run, err error) {
	ret := it.nextRuns[it.i]
	ret.Val = !it.lastVal
	it.lastVal = ret.Val

	it.i--
	if it.i == 255 { // if i was 0 before subtraction
		err = it.prep()
	}
	return ret, err
}

func (it *rleIterator) prep() error {
	idx := it.bv.Peek6()
	decode := &decodeTable[idx] // taking pointer here is worth -19% perf

	it.bv.Drop(decode.n)
	it.i = decode.i
	if !decode.varint {
		it.nextRuns = decode.runs
	} else {
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
		it.nextRuns[0].Len = x
	}
	return nil
}
