// Copyright 2014 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package data

import (
	"fmt"
	"math"
	"reflect"
)

/*
pandas:

series: 1d labeled array of arbitrary data
  Series(data, index) - index is a label for each row
  the result of an operation between two unaligned series will be the union of labels in the two series

  is TimeSeries if index is DateTime objects

dataframe: 2d labeled data structure
  dict of series objects
  accepts multiple inputs:
    dict of 1d arrays, ndlists, etc
    2d array
    series
    another dataframe
  dataframe index refers to rows; series index refers to cols

helps you do the right thing for memory + CPU efficiency

DataFrame
TSDataFrame
TSDataFrame

*/

type LookupTypeError struct {
	Key interface{}
}

func (e LookupTypeError) Error() string {
	return fmt.Sprintf("Wrong lookup type for: %s (%T)", e.Key, e.Key)
}

type LookupError struct {
	Key interface{}
}

func (e LookupError) Error() string {
	return fmt.Sprintf("Lookup failed for: %s", e.Key)
}

type RLookupError struct {
	Offset int
}

func (e RLookupError) Error() string {
	return fmt.Sprintf("RLookup offset out of range for: %s", e.Offset)
}

type Index interface {
	Lookup(key interface{}) (int, error)
	RLookup(i int) (interface{}, error)
	Len() int
}

type IndexS struct {
	index  map[string]int
	rindex []string
}

func NewIndexS(index interface{}, rindex interface{}) (Index, error) {
	i, ok := index.(map[string]int)
	if !ok {
		return nil, fmt.Errorf("bad index type: %#T", index)
	}
	ri, ok := rindex.([]string)
	if !ok {
		return nil, fmt.Errorf("bad rindex type: %#T", rindex)
	}
	return &IndexS{i, ri}, nil
}

func (i *IndexS) Lookup(key interface{}) (int, error) {
	k, ok := key.(string)
	if !ok {
		return -1, LookupTypeError{key}
	}
	idx, ok := i.index[k]
	if !ok {
		return -1, LookupError{key}
	}
	return idx, nil
}

func (i *IndexS) RLookup(off int) (interface{}, error) {
	if off < len(i.rindex) {
		return i.rindex[off], nil
	}
	return nil, RLookupError{off}
}

func (i *IndexS) Len() int {
	return len(i.index)
}

type Series interface {
	Name() string
	Index() Index
	Data() interface{} // a slice of float64, int64, or time.Time
	Append(v interface{})
	AppendEmpty()
	Len() int
}

type NewSeriesFn func(name string, index Index, len, cap int64) Series

// A series for double-precision floating-point values, indexed by
// strings.
type SeriesF struct {
	name  string
	index Index
	data  []float64
}

func NewSeriesF(name string, index Index, len, cap int64) Series {
	s := &SeriesF{
		name:  name,
		index: index,
		data:  make([]float64, len, cap),
	}
	return s
}

func (s *SeriesF) Name() string {
	return s.name
}

func (s *SeriesF) Index() Index {
	return s.index
}

func (s *SeriesF) Data() interface{} {
	return s.data
}

func (s *SeriesF) Append(v interface{}) {
	vv, ok := v.(float64)
	if !ok {
		panic("append non-float to SeriesF")
	}
	s.data = append(s.data, vv)
}

func (s *SeriesF) AppendEmpty() {
	s.data = append(s.data, math.NaN())
}

func (s *SeriesF) Len() int {
	return len(s.data)
}

// Set updates the value associated with a given index.  If the index
// does not exist it returns an error, and the function call has no
// effect.  This is to prevent implicit poor performance.
func (s *SeriesF) Set(key string, val float64) error {
	i, err := s.index.Lookup(key)
	if err != nil {
		return err
	}
	s.data[i] = val
	return nil
}

// A series for signed 64-bit integer values, indexed by strings.
type SeriesI struct {
	name  string
	index Index
	data  []int64
}

func NewSeriesI(name string, index Index, len, cap int64) Series {
	s := &SeriesI{
		name:  name,
		index: index,
		data:  make([]int64, len, cap),
	}
	return s
}

func (s *SeriesI) Name() string {
	return s.name
}

func (s *SeriesI) Index() Index {
	return s.index
}

func (s *SeriesI) Data() interface{} {
	return s.data
}

func (s *SeriesI) Append(v interface{}) {
	vv, ok := v.(int64)
	if !ok {
		panic("append non-int to SeriesI")
	}
	s.data = append(s.data, vv)
}

func (s *SeriesI) AppendEmpty() {
	s.data = append(s.data, 0)
}

func (s *SeriesI) Len() int {
	return len(s.data)
}

// A series for string values, indexed by strings.
type SeriesS struct {
	name  string
	index Index
	data  []string
}

func NewSeriesS(name string, index Index, len, cap int64) Series {
	s := &SeriesS{
		name:  name,
		index: index,
		data:  make([]string, len, cap),
	}
	return s
}

func (s *SeriesS) Name() string {
	return s.name
}

func (s *SeriesS) Index() Index {
	return s.index
}

func (s *SeriesS) Data() interface{} {
	return s.data
}

func (s *SeriesS) Append(v interface{}) {
	vv, ok := v.(string)
	if !ok {
		panic("append non-string to SeriesS")
	}
	s.data = append(s.data, vv)
}

func (s *SeriesS) AppendEmpty() {
	s.data = append(s.data, "")
}

func (s *SeriesS) Len() int {
	return len(s.data)
}

type Frame struct {
	ColIndex *IndexS // index for column-names into Series member
	RowIndex Index   // shared index for all series
	Series   []Series
}

func (df *Frame) Append(records ...interface{}) error {
	return nil
}

var createSeries = map[reflect.Kind]NewSeriesFn{
	reflect.String:  NewSeriesS,
	reflect.Float64: NewSeriesF,
	reflect.Int64:   NewSeriesI,
}

// NewFrameFromRecords returns a *Frame from a slice []T where T
// is a struct, pointer to struct, or map[string]interface{}.  The
// type of each Series created in the Frame is inferred from the
// type of the struct field or dict value on a record - if different
// records have different types for the same field an error is
// returned. Key is the field name or map key that is used, and cap
// can be used to size the Frame's Series to avoid allocations in
// subsequent Appends
func NewFrameFromRecords(records interface{}, key string, cap int64) (*Frame, error) {
	rv := reflect.ValueOf(records)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("records arg must be slice, not %s", rv.Kind())
	}
	if key == "" {
		return nil, fmt.Errorf("key must be non-empty")
	}

	nr := int64(rv.Len())
	if cap < nr {
		cap = nr
	}
	cols := make(map[string]reflect.Kind)
	rows := make([]string, nr)

	// iterate through records to find union of field names for DF
	// index, along with keys for shared Series index
	for i := 0; int64(i) < nr; i++ {
		v := rv.Index(i)
		// FIXME(bp) handle nil
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Map:
			if v.Type().Key().Kind() != reflect.String {
				return nil, fmt.Errorf("map key type must be str, not %s", v.Type().Key().Kind())
			}
			// XXX: iterate over keys
		case reflect.Struct:
			fields := cachedTypeFields(v.Type())
			for j := range fields {
				n := fields[j].name
				fv := v.FieldByName(n)
				switch fv.Kind() {
				case reflect.String:
					if n == key {
						s := fv.String()
						rows[i] = s
					}
					fallthrough
				case reflect.Int64, reflect.Float64:
					k := fv.Kind()
					if prevK, ok := cols[n]; ok {
						if k != prevK {
							return nil, fmt.Errorf("field %s has different types %s vs %s", n, k, prevK)
						}
					}
					cols[n] = fv.Kind()
				default:
					// XXX: log?
					continue
				}
			}
		default:
			return nil, fmt.Errorf("unsupported record type %s", v.Kind())
		}
	}

	rowMap := make(map[string]int)
	for i, n := range rows {
		// if we don't have a unique mapping, don't overwrite
		// the first row.
		if _, ok := rowMap[n]; ok {
			continue
		}
		rowMap[n] = i
	}

	// create shared index
	index, err := NewIndexS(rowMap, rows)
	if err != nil {
		return nil, fmt.Errorf("NewIndexS: %s", err)
	}

	colMap := make(map[string]int)
	colRIndex := make([]string, 0, len(cols))
	series := make([]Series, 0, len(cols))

	// create series
	for name, kind := range cols {
		colMap[name] = len(colRIndex)
		colRIndex = append(colRIndex, name)

		newFn, ok := createSeries[kind]
		if !ok {
			return nil, fmt.Errorf("unknown kind %s for Series col %s", kind, name)
		}
		s := newFn(name, index, nr, cap)
		series = append(series, s)
	}

	df := &Frame{
		ColIndex: &IndexS{colMap, colRIndex},
		RowIndex: index,
		Series:   series,
	}

	// iterate through records, filling in series with value from
	// record or math.NaN
	for i := 0; int64(i) < nr; i++ {
		v := rv.Index(i)
		// FIXME(bp) handle nil
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Map:
			// XXX: iterate over keys
		case reflect.Struct:
			// iterate over all fields in index, NOT just
			// the fields on this struct
			for j := 0; j < index.Len(); j++ {
				ni, _ := index.RLookup(j)
				n := ni.(string)
				si, _ := df.RowIndex.Lookup(n)
				s := df.Series[si]
				if _, ok := v.Type().FieldByName(n); !ok {
					s.AppendEmpty()
					continue
				}
				fv := v.FieldByName(n)
				switch fv.Kind() {
				case reflect.String:
					s.Append(fv.String())
				case reflect.Int64:
					s.Append(fv.Int())
				case reflect.Float64:
					s.Append(fv.Float())
				}
			}
		}

	}

	return df, nil
}
