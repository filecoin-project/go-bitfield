package rlepluslazy

import (
	"fmt"
	"math"

	"golang.org/x/xerrors"
)

func Or(a, b RunIterator) (RunIterator, error) {
	it := addIt{a: a, b: b}
	return &it, it.prep()
}

type addIt struct {
	a RunIterator
	b RunIterator

	next Run

	arun Run
	brun Run
}

func (it *addIt) prep() error {
	var err error

	fetch := func() error {
		if !it.arun.Valid() && it.a.HasNext() {
			it.arun, err = it.a.NextRun()
			if err != nil {
				return err
			}
		}

		if !it.brun.Valid() && it.b.HasNext() {
			it.brun, err = it.b.NextRun()
			if err != nil {
				return err
			}
		}
		return nil
	}

	if err := fetch(); err != nil {
		return err
	}

	// one is not valid
	if !it.arun.Valid() {
		it.next = it.brun
		it.brun.Len = 0
		return nil
	}

	if !it.brun.Valid() {
		it.next = it.arun
		it.arun.Len = 0
		return nil
	}

	if !(it.arun.Val || it.brun.Val) {
		min := it.arun.Len
		if it.brun.Len < min {
			min = it.brun.Len
		}
		it.next = Run{Val: it.arun.Val, Len: min}
		it.arun.Len -= it.next.Len
		it.brun.Len -= it.next.Len
		return nil
	}

	it.next = Run{Val: true}
	// different vals, 'true' wins
	for (it.arun.Val && it.arun.Valid()) || (it.brun.Val && it.brun.Valid()) {
		min := it.arun.Len
		if it.brun.Len < min && it.brun.Valid() || !it.arun.Valid() {
			min = it.brun.Len
		}
		it.next.Len += min
		if it.arun.Valid() {
			it.arun.Len -= min
		}
		if it.brun.Valid() {
			it.brun.Len -= min
		}
		if err := fetch(); err != nil {
			return err
		}
	}

	return nil
}

func (it *addIt) HasNext() bool {
	return it.next.Valid()
}

func (it *addIt) NextRun() (Run, error) {
	next := it.next
	return next, it.prep()
}

func Count(ri RunIterator) (uint64, error) {
	var count uint64

	for ri.HasNext() {
		r, err := ri.NextRun()
		if err != nil {
			return 0, err
		}
		if r.Val {
			if math.MaxUint64-r.Len < count {
				return 0, xerrors.New("RLE+ overflows")
			}
			count += r.Len
		}
	}
	return count, nil
}

func IsSet(ri RunIterator, x uint64) (bool, error) {
	var i uint64
	for ri.HasNext() {
		r, err := ri.NextRun()
		if err != nil {
			return false, err
		}

		if i+r.Len > x {
			return r.Val, nil
		}

		i += r.Len
	}
	return false, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func And(a, b RunIterator) (RunIterator, error) {
	var ar, br Run

	var out []Run
	for {
		if !ar.Valid() && a.HasNext() {
			nar, err := a.NextRun()
			if err != nil {
				return nil, err
			}
			ar = nar
		}
		if !br.Valid() && b.HasNext() {
			nbr, err := b.NextRun()
			if err != nil {
				return nil, err
			}
			br = nbr
		}

		// if either run is out of bits, we're done here
		if !ar.Valid() || !br.Valid() {
			break
		}

		r := Run{
			Val: ar.Val && br.Val,
			Len: min(ar.Len, br.Len),
		}

		ar.Len -= r.Len
		br.Len -= r.Len

		if len(out) > 0 && out[len(out)-1].Val == r.Val {
			out[len(out)-1].Len += r.Len
		} else {
			out = append(out, r)
		}
	}

	return &RunSliceIterator{out, 0}, nil
}

type RunSliceIterator struct {
	Runs []Run
	i    int
}

func (ri *RunSliceIterator) HasNext() bool {
	return ri.i < len(ri.Runs)
}

func (ri *RunSliceIterator) NextRun() (Run, error) {
	if ri.i >= len(ri.Runs) {
		return Run{}, fmt.Errorf("end of runs")
	}

	out := ri.Runs[ri.i]
	ri.i++
	return out, nil
}

type notIter struct {
	it RunIterator
}

func (ni *notIter) HasNext() bool {
	return true
}

func (ni *notIter) NextRun() (Run, error) {
	if !ni.it.HasNext() {
		return Run{
			Val: true,
			Len: 10000000000, // close enough to infinity
		}, nil
	}

	nr, err := ni.it.NextRun()
	if err != nil {
		return Run{}, err
	}

	nr.Val = !nr.Val
	return nr, nil
}

func Subtract(a, b RunIterator) (RunIterator, error) {
	return And(a, &notIter{it: b})
}
