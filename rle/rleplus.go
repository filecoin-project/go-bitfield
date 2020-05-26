package rlepluslazy

import (
	"errors"
	"fmt"

	"golang.org/x/xerrors"
)

const Version = 0

var (
	ErrWrongVersion = errors.New("invalid RLE+ version")
	ErrDecode       = fmt.Errorf("invalid encoding for RLE+ version %d", Version)
)

type RLE struct {
	buf  []byte
	runs []Run
}

func FromBuf(buf []byte) (RLE, error) {
	rle := RLE{buf: buf}

	if len(buf) > 0 && buf[0]&3 != Version {
		return RLE{}, xerrors.Errorf("could not create RLE+ for a buffer: %w", ErrWrongVersion)
	}

	_, err := rle.Count()
	if err != nil {
		return RLE{}, err
	}

	return rle, nil
}

func (rle *RLE) RunIterator() (RunIterator, error) {
	if rle.runs == nil {
		source, err := DecodeRLE(rle.buf)
		if err != nil {
			return nil, xerrors.Errorf("decoding RLE: %w", err)
		}
		for source.HasNext() {
			r, err := source.NextRun()
			if err != nil {
				return nil, xerrors.Errorf("reading run: %w", err)
			}
			rle.runs = append(rle.runs, r)
		}
	}

	return &RunSliceIterator{Runs: rle.runs}, nil
}

func (rle *RLE) Count() (uint64, error) {
	it, err := rle.RunIterator()
	if err != nil {
		return 0, err
	}
	return Count(it)
}
