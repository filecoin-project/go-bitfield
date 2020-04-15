package bitfield

import (
	"encoding/json"
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

func newWithRle(rle rlepluslazy.RLE) *BitField {
	return &BitField{
		set:   make(map[uint64]struct{}),
		unset: make(map[uint64]struct{}),
		rle:   rle,
	}
}

func NewFromSet(setBits []uint64) *BitField {
	res := &BitField{
		set:   make(map[uint64]struct{}),
		unset: make(map[uint64]struct{}),
	}
	for _, b := range setBits {
		res.set[b] = struct{}{}
	}
	return res
}

func NewFromIter(r rlepluslazy.RunIterator) (*BitField, error) {
	buf, err := rlepluslazy.EncodeRuns(r, nil)
	if err != nil {
		return nil, err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return nil, err
	}

	return newWithRle(rle), nil
}

func MergeBitFields(a, b *BitField) (*BitField, error) {
	ra, err := a.sum()
	if err != nil {
		return nil, err
	}

	rb, err := b.sum()
	if err != nil {
		return nil, err
	}

	merge, err := rlepluslazy.Or(ra, rb)
	if err != nil {
		return nil, err
	}

	mergebytes, err := rlepluslazy.EncodeRuns(merge, nil)
	if err != nil {
		return nil, err
	}

	rle, err := rlepluslazy.FromBuf(mergebytes)
	if err != nil {
		return nil, err
	}

	return newWithRle(rle), nil
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

func (bf *BitField) MarshalJSON() ([]byte, error) {
	r, err := bf.sum()
	if err != nil {
		return nil, err
	}

	buf, err := rlepluslazy.EncodeRuns(r, nil)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buf)
}

func (bf *BitField) UnmarshalJSON(b []byte) error {
	var buf []byte
	if err := json.Unmarshal(b, &buf); err != nil {
		return err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return err
	}

	bf.rle = rle
	bf.set = make(map[uint64]struct{})
	bf.unset = make(map[uint64]struct{})
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

func (bf *BitField) IsSet(x uint64) (bool, error) {
	iter, err := bf.sum()
	if err != nil {
		return false, err
	}

	return rlepluslazy.IsSet(iter, x)
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

func (bf *BitField) IsEmpty() (bool, error) {
	c, err := bf.Count()
	if err != nil {
		return false, err
	}
	return c == 0, nil
}

func (bf *BitField) Slice(start, count uint64) (*BitField, error) {
	iter, err := bf.sum()
	if err != nil {
		return nil, err
	}

	valsUntilStart := start

	var sliceRuns []rlepluslazy.Run
	var i, outcount uint64
	for iter.HasNext() && valsUntilStart > 0 {
		r, err := iter.NextRun()
		if err != nil {
			return nil, err
		}

		if r.Val {
			if r.Len <= valsUntilStart {
				valsUntilStart -= r.Len
				i += r.Len
			} else {
				i += valsUntilStart

				rem := r.Len - valsUntilStart
				if rem > count {
					rem = count
				}

				sliceRuns = append(sliceRuns,
					rlepluslazy.Run{Val: false, Len: i},
					rlepluslazy.Run{Val: true, Len: rem},
				)
				outcount += rem
				valsUntilStart = 0
			}
		} else {
			i += r.Len
		}
	}

	for iter.HasNext() && outcount < count {
		r, err := iter.NextRun()
		if err != nil {
			return nil, err
		}

		if r.Val {
			if r.Len <= count-outcount {
				sliceRuns = append(sliceRuns, r)
				outcount += r.Len
			} else {
				sliceRuns = append(sliceRuns, rlepluslazy.Run{Val: true, Len: count - outcount})
				outcount = count
			}
		} else {
			if len(sliceRuns) == 0 {
				r.Len += i
			}
			sliceRuns = append(sliceRuns, r)
		}
	}
	if outcount < count {
		return nil, fmt.Errorf("not enough bits set in field to satisfy slice count")
	}

	buf, err := rlepluslazy.EncodeRuns(&rlepluslazy.RunSliceIterator{Runs: sliceRuns}, nil)
	if err != nil {
		return nil, err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return nil, err
	}

	return &BitField{rle: rle}, nil
}

func IntersectBitField(a, b *BitField) (*BitField, error) {
	ar, err := a.sum()
	if err != nil {
		return nil, err
	}

	br, err := b.sum()
	if err != nil {
		return nil, err
	}

	andIter, err := rlepluslazy.And(ar, br)
	if err != nil {
		return nil, err
	}

	buf, err := rlepluslazy.EncodeRuns(andIter, nil)
	if err != nil {
		return nil, err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return nil, err
	}

	return newWithRle(rle), nil
}

func SubtractBitField(a, b *BitField) (*BitField, error) {
	ar, err := a.sum()
	if err != nil {
		return nil, err
	}

	br, err := b.sum()
	if err != nil {
		return nil, err
	}

	andIter, err := rlepluslazy.Subtract(ar, br)
	if err != nil {
		return nil, err
	}

	buf, err := rlepluslazy.EncodeRuns(andIter, nil)
	if err != nil {
		return nil, err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return nil, err
	}

	return newWithRle(rle), nil
}
