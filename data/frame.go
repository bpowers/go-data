// Copyright 2014 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package frame

import (
	"fmt"
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

type LookupTypeError struct{
	Key interface{}
}

func (e LookupTypeError) Error() string {
	return fmt.Sprintf("Wrong lookup type for: %s (%T)", e.Key, e.Key)
}

type LookupError struct{
	Key interface{}
}

func (e LookupError) Error() string {
	return fmt.Sprintf("Lookup failed for: %s", e.Key)
}

type Index interface{
	Lookup(key interface{}) (int, error)
	RLookup(i int) (interface{}, error)
	Len() int
}

type IndexS struct {
	index  map[string]int
	rindex []string
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

func (i *IndexS) Len() int {
	return len(i.index)
}

type Series interface {
	Name() string
	Index() Index
	SetIndex(i Index)
	Data() interface{} // a slice of float64, int64, or time.Time
}

// A series for double-precision floating-point values, indexed by
// strings.
type SeriesF struct {
	name  string
	index Index
	data  []float64
}

func (s *SeriesF) Name() string {
	return s.name
}

func (s *SeriesF) Index() Index {
	return s.index
}

func (s *SeriesF) SetIndex(i Index) {
	s.index = i
}

func (s *SeriesF) Data() interface{} {
	return s.data
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

type DataFrame struct {
	ColIndex *IndexS // index for column-names into Series member
	RowIndex Index // shared index for all series
	Series   []Series
}

func (df *DataFrame) Append(records ...interface{}) error {
	return nil
}

// NewDataFrameFromRecords returns a *DataFrame from a slice []T where
// T is a struct, pointer to struct, or map[string]interface{}.  The
// type of each Series created in the DataFrame is inferred from the
// type of the struct field or dict value on a record - if different
// records have different types for the same field an error is
// returned. Key is the field name or map key that is used, and cap
// can be used to size the DataFrame's Series to avoid allocations in
// subsequent Appends
func NewDataFrameFromRecords(records interface{}, key string, cap int) (*DataFrame, error) {
	rv := reflect.ValueOf(records)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("records arg must be slice, not %s", rv.Kind())
	}
	if cap < rv.Len() {
		cap = rv.Len()
	}
	// iterate through records to find union of field names for DF
	// index + keys for shared Series index

	// create series with 0 len + specified cap

	// iterate through records, filling in series with value from
	// record or math.NaN

	return nil, nil
}

