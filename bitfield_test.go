package bitfield

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func slicesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func getRandIndexSet(n int) []uint64 {
	return getRandIndexSetSeed(n, 55)
}

func getRandIndexSetSeed(n int, seed int64) []uint64 {
	r := rand.New(rand.NewSource(seed))

	var items []uint64
	for i := 0; i < n; i++ {
		if r.Intn(3) != 0 {
			items = append(items, uint64(i))
		}
	}
	return items
}

func TestBitfieldSlice(t *testing.T) {
	vals := getRandIndexSet(10000)

	bf := NewFromSet(vals)

	sl, err := bf.Slice(600, 500)
	if err != nil {
		t.Fatal(err)
	}

	expslice := vals[600:1100]

	outvals, err := sl.All(10000)
	if err != nil {
		t.Fatal(err)
	}

	if !slicesEqual(expslice, outvals) {
		fmt.Println(expslice)
		fmt.Println(outvals)
		t.Fatal("output slice was not correct")
	}
}

func TestBitfieldSliceSmall(t *testing.T) {
	vals := []uint64{1, 5, 6, 7, 10, 11, 12, 15}

	testPerm := func(start, count uint64) func(*testing.T) {
		return func(t *testing.T) {

			bf := NewFromSet(vals)

			sl, err := bf.Slice(start, count)
			if err != nil {
				t.Fatal(err)
			}

			expslice := vals[start : start+count]

			outvals, err := sl.All(10000)
			if err != nil {
				t.Fatal(err)
			}

			if !slicesEqual(expslice, outvals) {
				fmt.Println(expslice)
				fmt.Println(outvals)
				t.Fatal("output slice was not correct")
			}
		}
	}

	/*
		t.Run("all", testPerm(0, 8))
		t.Run("not first", testPerm(1, 7))
		t.Run("last item", testPerm(7, 1))
		t.Run("start during gap", testPerm(1, 4))
		t.Run("start during run", testPerm(3, 4))
		t.Run("end during run", testPerm(1, 1))
	*/

	for i := 0; i < len(vals); i++ {
		for j := 0; j < len(vals)-i; j++ {
			t.Run(fmt.Sprintf("comb-%d-%d", i, j), testPerm(uint64(i), uint64(j)))
		}
	}
}

func unionArrs(a, b []uint64) []uint64 {
	m := make(map[uint64]bool)
	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		m[v] = true
	}

	out := make([]uint64, 0, len(m))
	for v := range m {
		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})

	return out
}

func TestBitfieldUnion(t *testing.T) {
	a := getRandIndexSetSeed(100, 1)
	b := getRandIndexSetSeed(100, 2)

	bfa := NewFromSet(a)
	bfb := NewFromSet(b)

	bfu, err := MergeBitFields(bfa, bfb)
	if err != nil {
		t.Fatal(err)
	}

	out, err := bfu.All(100000)
	if err != nil {
		t.Fatal(err)
	}

	exp := unionArrs(a, b)

	if !slicesEqual(out, exp) {
		fmt.Println(out)
		fmt.Println(exp)
		t.Fatal("union was wrong")
	}
}

func TestBitfieldJson(t *testing.T) {
	vals := getRandIndexSet(100000)

	bf := NewFromSet(vals)

	b, err := bf.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	var out BitField
	if err := out.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}

	outv, err := out.All(100000)
	if err != nil {
		t.Fatal(err)
	}

	if !slicesEqual(vals, outv) {
		t.Fatal("round trip failed")
	}
}
