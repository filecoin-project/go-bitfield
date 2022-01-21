package bitfield

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func multiUnionArrs(arrs [][]uint64) []uint64 {
	base := arrs[0]
	for i := 1; i < len(arrs); i++ {
		base = unionArrs(base, arrs[i])
	}
	return base
}

func TestBitfieldMultiUnion(t *testing.T) {
	var sets [][]uint64
	var bfs []BitField
	for i := 0; i < 15; i++ {
		s := getRandIndexSetSeed(10000, 1)
		sets = append(sets, s)
		bfs = append(bfs, NewFromSet(s))
	}

	bfu, err := MultiMerge(bfs...)
	if err != nil {
		t.Fatal(err)
	}

	out, err := bfu.All(100000)
	if err != nil {
		t.Fatal(err)
	}

	exp := multiUnionArrs(sets)

	if !slicesEqual(out, exp) {
		fmt.Println(out)
		fmt.Println(exp)
		t.Fatal("union was wrong")
	}
}

func TestBitfieldJson(t *testing.T) {
	vals := []uint64{1, 5, 6, 7, 10, 11, 12, 15}

	bf := NewFromSet(vals)

	b, err := bf.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	var buf = struct {
		Count uint64
		RLE   []uint64
	}{}
	if err := json.Unmarshal(b, &buf); err != nil {
		t.Fatal(err)
	}

	bfCount, err := bf.Count()
	if err != nil {
		t.Fatal(err)
	}

	// assert the count is the same in the unmarshalled bitfield and original
	if bfCount != buf.Count {
		t.Fatal(err)
	}

	// (0) (1) (2, 3, 4), (5, 6, 7), (8, 9), (10, 11, 12), (13, 14), 15
	runs := []uint64{1, 1, 3, 3, 2, 3, 2, 1}
	if !slicesEqual(runs, buf.RLE) {
		t.Fatal("runs not encoded correctly")
	}
}

func TestEmptyBitfieldJson(t *testing.T) {
	type ct struct {
		B BitField
	}

	ebf := New()
	s := &ct{
		B: ebf,
	}

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}

	var u ct
	if err := json.Unmarshal(b, &u); err != nil {
		t.Fatal(err)
	}

	set, err := u.B.Count()
	if err != nil {
		t.Fatal(err)
	}

	if set > 0 {
		t.Errorf("expected 0 bits to be set")
	}
}

func TestBitfieldJsonRoundTrip(t *testing.T) {
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

	bfCount, err := bf.Count()
	if err != nil {
		t.Fatal(err)
	}

	outCount, err := out.Count()
	if err != nil {
		t.Fatal(err)
	}

	if bfCount != outCount {
		t.Fatalf("round trip failed")
	}

	outv, err := out.All(100000)
	if err != nil {
		t.Fatal(err)
	}

	if !slicesEqual(vals, outv) {
		t.Fatal("round trip failed")
	}
}

func setIntersect(a, b []uint64) []uint64 {
	m := make(map[uint64]bool)
	for _, v := range a {
		m[v] = true
	}

	var out []uint64
	for _, v := range b {
		if m[v] {
			out = append(out, v)
		}
	}
	return out
}

func TestUninitializedBitField(t *testing.T) {
	var (
		bf  BitField
		buf bytes.Buffer
	)
	err := bf.MarshalCBOR(&buf)
	require.NoError(t, err)
	assert.Equal(t, 1, buf.Len())
	buf.Reset()

	fromSet := NewFromSet(nil)
	err = fromSet.MarshalCBOR(&buf)
	require.NoError(t, err)
	assert.Equal(t, 1, buf.Len())
	buf.Reset()
}

func TestMinimallyEncoded(t *testing.T) {
	for i := int64(0); i < 100; i++ {
		bits := getRandIndexSetSeed(100, i)
		bf := NewFromSet(bits)
		var buf bytes.Buffer
		err := bf.MarshalCBOR(&buf)
		require.NoError(t, err)
		assert.NotZero(t, buf.Bytes()[buf.Len()-1])
	}
}

func TestBitfieldIntersect(t *testing.T) {
	a := getRandIndexSetSeed(100, 1)
	b := getRandIndexSetSeed(100, 2)

	bfa := NewFromSet(a)
	bfb := NewFromSet(b)

	inter, err := IntersectBitField(bfa, bfb)
	if err != nil {
		t.Fatal(err)
	}

	out, err := inter.All(10000)
	if err != nil {
		t.Fatal(err)
	}

	exp := setIntersect(a, b)

	if !slicesEqual(out, exp) {
		fmt.Println(a)
		fmt.Println(b)
		fmt.Println(out)
		fmt.Println(exp)
		t.Fatal("intersection is wrong")
	}
}

func setSubtract(a, b []uint64) []uint64 {
	m := make(map[uint64]bool)
	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		delete(m, v)
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

func TestBitfieldOrDifferentLenZeroSuffix(t *testing.T) {
	ra := &rlepluslazy.RunSliceIterator{
		Runs: []rlepluslazy.Run{
			{Val: false, Len: 5},
		},
	}

	rb := &rlepluslazy.RunSliceIterator{
		Runs: []rlepluslazy.Run{
			{Val: false, Len: 8},
		},
	}

	merge, err := rlepluslazy.Or(ra, rb)
	require.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}

	mergebytes, err := rlepluslazy.EncodeRuns(merge, nil)
	require.NoError(t, err)
	assert.Empty(t, mergebytes)

	b, err := NewFromBytes(mergebytes)
	require.NoError(t, err)

	c, err := b.Count()
	require.NoError(t, err)

	assert.Zero(t, c, "expected 0 set bits")
}

func TestBitfieldSubDifferentLenZeroSuffix(t *testing.T) {
	ra := &rlepluslazy.RunSliceIterator{
		Runs: []rlepluslazy.Run{
			{Val: true, Len: 5},
			{Val: false, Len: 5},
		},
	}

	rb := &rlepluslazy.RunSliceIterator{
		Runs: []rlepluslazy.Run{
			{Val: true, Len: 5},
			{Val: false, Len: 8},
		},
	}

	merge, err := rlepluslazy.Subtract(ra, rb)
	if err != nil {
		t.Fatal(err)
	}

	mergebytes, err := rlepluslazy.EncodeRuns(merge, nil)
	require.NoError(t, err)
	assert.Empty(t, mergebytes)

	b, err := NewFromBytes(mergebytes)
	require.NoError(t, err)

	c, err := b.Count()
	require.NoError(t, err)

	assert.Zero(t, c, "expected 0 set bits")
}

func TestBitfieldSubtract(t *testing.T) {
	a := getRandIndexSetSeed(100, 1)
	b := getRandIndexSetSeed(100, 2)

	bfa := NewFromSet(a)
	bfb := NewFromSet(b)

	inter, err := SubtractBitField(bfa, bfb)
	if err != nil {
		t.Fatal(err)
	}

	out, err := inter.All(10000)
	if err != nil {
		t.Fatal(err)
	}

	exp := setSubtract(a, b)

	if !slicesEqual(out, exp) {
		fmt.Println(a)
		fmt.Println(b)
		fmt.Println(out)
		fmt.Println(exp)
		t.Fatal("subtraction is wrong")
	}
}
func TestBitfieldSubtractLargeElement(t *testing.T) {
	bfa := NewFromSet([]uint64{1, 2, math.MaxUint64 - 1})
	bfb := NewFromSet([]uint64{1})

	inter, err := SubtractBitField(bfa, bfb)
	if err != nil {
		t.Fatal(err)
	}

	out, err := inter.All(10000)
	if err != nil {
		t.Fatal(err)
	}

	if !slicesEqual(out, []uint64{2, math.MaxUint64 - 1}) {
		t.Fatalf("subtraction is wrong: %v", out)
	}
}

// <specs-actors>
func BitFieldUnion(bfs ...BitField) (BitField, error) {
	// TODO: optimize me
	for len(bfs) > 1 {
		var next []BitField
		for i := 0; i < len(bfs); i += 2 {
			if i+1 >= len(bfs) {
				next = append(next, bfs[i])
				break
			}
			merged, err := MergeBitFields(bfs[i], bfs[i+1])
			if err != nil {
				return BitField{}, err
			}

			next = append(next, merged)
		}
		bfs = next
	}
	return bfs[0], nil
}

// </specs-actors>
func TestBitfieldSubtractMore(t *testing.T) {
	have := NewFromSet([]uint64{5, 6, 8, 10, 11, 13, 14, 17})
	s1, err := SubtractBitField(NewFromSet([]uint64{5, 6}), have)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := SubtractBitField(NewFromSet([]uint64{8, 10}), have)
	if err != nil {
		t.Fatal(err)
	}
	s3, err := SubtractBitField(NewFromSet([]uint64{11, 13}), have)
	if err != nil {
		t.Fatal(err)
	}
	s4, err := SubtractBitField(NewFromSet([]uint64{14, 17}), have)
	if err != nil {
		t.Fatal(err)
	}

	u, err := BitFieldUnion(s1, s2, s3, s4)
	if err != nil {
		t.Fatal(err)
	}

	c, err := u.Count()
	if err != nil {
		t.Fatal(err)
	}
	if c != 0 {
		ua, err := u.All(500)
		fmt.Printf("%s %+v", err, ua)
		t.Error("expected 0", c)
	}
}

func TestBitfieldCopy(t *testing.T) {
	start := []uint64{5, 6, 8, 10, 11, 13, 14, 17}

	orig := NewFromSet(start)

	cp, err := orig.Copy()
	if err != nil {
		t.Fatal(err)
	}

	cp.Unset(10)

	s, err := orig.IsSet(10)
	if err != nil {
		t.Fatal(err)
	}
	if !s {
		t.Fatal("mutation affected original bitfield")
	}

}

func TestBitfieldSetUnset(t *testing.T) {
	expected := []uint64{4, 5, 8, 10, 11, 13, 14, 17}

	bits := []uint64{5, 6, 8, 10, 11, 13, 14, 17}
	iter, err := rlepluslazy.RunsFromSlice(bits)
	require.NoError(t, err)

	bf1, err := NewFromIter(iter)
	require.NoError(t, err)

	bf1.Set(3)
	bf1.Set(4)
	bf1.Set(5)

	bf1.Unset(3)
	bf1.Unset(6)
	bf1.Unset(7)

	iter, err = bf1.RunIterator()
	require.NoError(t, err)
	bf2, err := NewFromIter(iter)
	require.NoError(t, err)

	actual, err := bf2.All(100)
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestBitfieldAllMap(t *testing.T) {
	for i := int64(0); i < 100; i++ {
		set := getRandIndexSetSeed(100, i)
		bf := NewFromSet(set)

		asMap, err := bf.AllMap(1000)
		require.NoError(t, err)

		for _, b := range set {
			assert.True(t, asMap[b])
			delete(asMap, b)
		}
		assert.Empty(t, asMap)

		_, err = bf.AllMap(3)
		assert.Error(t, err)
		_, err = bf.AllMap(0)
		assert.Error(t, err)

	}
}

func TestBitfieldForEach(t *testing.T) {
	for i := int64(0); i < 100; i++ {
		set := getRandIndexSetSeed(100, i)
		bf := NewFromSet(set)

		var actual []uint64
		err := bf.ForEach(func(i uint64) error {
			actual = append(actual, i)
			return nil
		})
		require.NoError(t, err)

		assert.Equal(t, set, actual)

		testErr := fmt.Errorf("test")
		err = bf.ForEach(func(i uint64) error {
			return testErr
		})
		assert.Equal(t, err, testErr)
	}
}

func TestBitfieldIsSet(t *testing.T) {
	for i := int64(0); i < 100; i++ {
		set := getRandIndexSetSeed(100, i)
		bf := NewFromSet(set)

		for i := uint64(0); i < 100; i++ {
			expected := false
			isSet, err := bf.IsSet(i)
			require.NoError(t, err)
			if len(set) > 0 {
				expected = (set[0] == i)
				if set[0] <= i {
					set = set[1:]
				}
			}
			assert.Equal(t, expected, isSet)
		}
	}
}

func TestBitfieldFirst(t *testing.T) {
	set := getRandIndexSet(100)
	bf := NewFromSet(set)

	first, err := bf.First()
	require.NoError(t, err)
	require.Equal(t, set[0], first)
}

func TestBitfieldIsEmpty(t *testing.T) {
	set := getRandIndexSet(100)
	bf := NewFromSet(set)

	isEmpty, err := bf.IsEmpty()
	require.NoError(t, err)
	require.False(t, isEmpty)

	empty := NewFromSet(nil)

	isEmpty, err = empty.IsEmpty()
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func TestBitfieldBitIter(t *testing.T) {
	for i := int64(0); i < 100; i++ {
		set := getRandIndexSetSeed(100, i)
		bf := NewFromSet(set)

		bitIter, err := bf.BitIterator()
		require.NoError(t, err)

		err = bf.ForEach(func(i uint64) error {
			require.True(t, bitIter.HasNext())
			next, err := bitIter.Next()
			require.NoError(t, err)
			assert.Equal(t, i, next)
			return nil
		})
		require.NoError(t, err)
	}
}

func TestBitfieldCutRandom(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			testBitFieldCut(t, int64(i))
		})
	}
}

func testBitFieldCut(t *testing.T, seed int64) {
	a := getRandIndexSetSeed(100, seed)
	b := getRandIndexSetSeed(100, seed+1)

	bfa := NewFromSet(a)
	bfb := NewFromSet(b)

	cut, err := CutBitField(bfa, bfb)
	if err != nil {
		t.Fatal(err)
	}

	aBits, err := bfa.All(1000)
	require.NoError(t, err)
	bBits, err := bfb.All(1000)
	require.NoError(t, err)

	var (
		expected []uint64
		offset   uint64
	)
outer:
	for _, bit := range aBits {
		for len(bBits) > 0 {
			skipBit := bBits[0]
			if skipBit > bit {
				// haven't reached this bit yet.
				break
			} else if skipBit <= bit {
				// ok, skip this bit
				offset++
			}

			bBits = bBits[1:]

			// If the bits were equal, skip the outer bit.
			if skipBit == bit {
				continue outer
			}
		}
		expected = append(expected, bit-offset)
	}

	actual, err := cut.All(10000)
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestBitfieldCutSame(t *testing.T) {
	a := getRandIndexSetSeed(100, 1)

	bfa := NewFromSet(a)

	cut, err := CutBitField(bfa, bfa)
	if err != nil {
		t.Fatal(err)
	}

	count, err := cut.Count()
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestLast(t *testing.T) {
	bits := getRandIndexSetSeed(100, 1)
	bf := NewFromSet(bits)
	last, err := bf.Last()
	require.NoError(t, err)

	assert.Equal(t, last, bits[len(bits)-1])
}

func TestLastEmpty(t *testing.T) {
	bf := NewFromSet(nil)
	_, err := bf.Last()
	require.EqualError(t, err, ErrNoBitsSet.Error())
}
