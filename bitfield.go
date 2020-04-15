package bitfield

import (
	"errors"
	"fmt"
	"io"

	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

var ErrBitFieldTooMany = errors.New("to many items in RLE")

type BitField struct {
	rle rlepluslazy.RLE

	set   map[uint64]struct{}
	unset map[uint64]struct{}
}

func New() BitField {
	bf, err := NewFromBytes([]byte{})
	if err != nil {
		panic(fmt.Sprintf("creating empty rle: %+v", err))
	}
	return bf
}

func NewFromBytes(rle []byte) (BitField, error) {
	bf := BitField{}
	rlep, err := rlepluslazy.FromBuf(rle)
	if err != nil {
		return BitField{}, xerrors.Errorf("could not decode rle+: %w", err)
	}
	bf.rle = rlep
	bf.set = make(map[uint64]struct{})
	bf.unset = make(map[uint64]struct{})
	return bf, nil

}

func NewFromSet(setBits []uint64) BitField {
	res := BitField{
		set:   make(map[uint64]struct{}),
		unset: make(map[uint64]struct{}),
	}
	for _, b := range setBits {
		res.set[b] = struct{}{}
	}
	return res
}

func MergeBitFields(a, b BitField) (BitField, error) {
	ra, err := a.sum()
	if err != nil {
		return BitField{}, err
	}

	rb, err := b.sum()
	if err != nil {
		return BitField{}, err
	}

	merge, err := rlepluslazy.Or(ra, rb)
	if err != nil {
		return BitField{}, err
	}

	mergebytes, err := rlepluslazy.EncodeRuns(merge, nil)
	if err != nil {
		return BitField{}, err
	}

	rle, err := rlepluslazy.FromBuf(mergebytes)
	if err != nil {
		return BitField{}, err
	}

	return BitField{
		rle: rle,
		set: make(map[uint64]struct{}),
	}, nil
}

func (bf BitField) sum() (rlepluslazy.RunIterator, error) {
	if len(bf.set) == 0 && len(bf.unset) == 0 {
		// fastpath
		return bf.rle.RunIterator()
	}

	a, err := bf.rle.RunIterator()
	if err != nil {
		return nil, err
	}
	slc := make([]uint64, 0, len(bf.set))
	for b := range bf.set {
		slc = append(slc, b)
	}

	b, err := rlepluslazy.RunsFromSlice(slc)
	if err != nil {
		return nil, err
	}

	or, err := rlepluslazy.Or(a, b)
	if err != nil {
		return nil, err
	}
	if len(bf.unset) == 0 {
		return or, nil
	}

	bits, err := rlepluslazy.SliceFromRuns(or)
	if err != nil {
		return nil, err
	}
	// TODO: streaming impl
	out := make([]uint64, 0, len(bits))
	for _, bit := range bits {
		if _, un := bf.unset[bit]; !un {
			out = append(out, bit)
		}
	}

	res, err := rlepluslazy.RunsFromSlice(out)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Set ...s bit in the BitField
func (bf BitField) Set(bit uint64) {
	delete(bf.unset, bit)
	bf.set[bit] = struct{}{}
}

// Unset ...s bit in the BitField
func (bf BitField) Unset(bit uint64) {
	delete(bf.set, bit)
	bf.unset[bit] = struct{}{}
}

func (bf BitField) Count() (uint64, error) {
	s, err := bf.sum()
	if err != nil {
		return 0, err
	}
	return rlepluslazy.Count(s)
}

// All returns all set set
func (bf BitField) All(max uint64) ([]uint64, error) {
	c, err := bf.Count()
	if err != nil {
		return nil, xerrors.Errorf("count errror: %w", err)
	}
	if c > max {
		return nil, xerrors.Errorf("expected %d, got %d: %w", max, c, ErrBitFieldTooMany)
	}

	runs, err := bf.sum()
	if err != nil {
		return nil, err
	}

	res, err := rlepluslazy.SliceFromRuns(runs)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (bf BitField) AllMap(max uint64) (map[uint64]bool, error) {
	c, err := bf.Count()
	if err != nil {
		return nil, xerrors.Errorf("count errror: %w", err)
	}
	if c > max {
		return nil, xerrors.Errorf("expected %d, got %d: %w", max, c, ErrBitFieldTooMany)
	}

	runs, err := bf.sum()
	if err != nil {
		return nil, err
	}

	res, err := rlepluslazy.SliceFromRuns(runs)
	if err != nil {
		return nil, err
	}

	out := make(map[uint64]bool)
	for _, i := range res {
		out[i] = true
	}
	return out, nil
}

func (bf *BitField) MarshalCBOR(w io.Writer) error {
	if bf == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	s, err := bf.sum()
	if err != nil {
		return err
	}

	rle, err := rlepluslazy.EncodeRuns(s, []byte{})
	if err != nil {
		return err
	}

	if len(rle) > 8192 {
		return xerrors.Errorf("encoded bitfield was too large (%d)", len(rle))
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajByteString, uint64(len(rle)))); err != nil {
		return err
	}
	if _, err = w.Write(rle); err != nil {
		return xerrors.Errorf("writing rle: %w", err)
	}
	return nil
}

func (bf *BitField) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if extra > 8192 {
		return fmt.Errorf("array too large")
	}

	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}

	buf := make([]byte, extra)
	if _, err := io.ReadFull(br, buf); err != nil {
		return err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return xerrors.Errorf("could not decode rle+: %w", err)
	}
	bf.rle = rle
	bf.set = make(map[uint64]struct{})

	return nil
}

func (bf *BitField) ForEach(f func(uint64) error) error {
	iter, err := bf.sum()
	if err != nil {
		return err
	}

	var i uint64
	for iter.HasNext() {
		r, err := iter.NextRun()
		if err != nil {
			return err
		}

		if r.Val {
			for j := uint64(0); j < r.Len; j++ {
				if err := f(i); err != nil {
					return err
				}
				i++
			}
		} else {
			i += r.Len
		}
	}
	return nil
}

func (bf *BitField) First() (uint64, error) {
	iter, err := bf.sum()
	if err != nil {
		return 0, err
	}

	var i uint64
	for iter.HasNext() {
		r, err := iter.NextRun()
		if err != nil {
			return 0, err
		}

		if r.Val {
			return i, nil
		} else {
			i += r.Len
		}
	}
	return 0, fmt.Errorf("bitfield has no set bits")
}
