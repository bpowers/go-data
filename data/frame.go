// Copyright 2014 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package frame

import (
	"fmt"
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
}

type SIndex struct {
	index  map[string]int
	rindex []string
}

func (i *SIndex) Lookup(key interface{}) (int, error) {
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

// A series for double-precision floating-point values, indexed by
// strings.
type FloatSeries struct {
	Name  string
	Index Index
	Data  []float64
}

// Set updates the value associated with a given index.  If the index
// does not exist it returns an error, and the function call has no
// effect.  This is to prevent implicit poor performance.
func (s *FloatSeries) Set(key string, val float64) error {
	i, err := s.Index.Lookup(key)
	if err != nil {
		return err
	}
	s.Data[i] = val
	return nil
}

type FloatDataFrame struct {
	Index  *SIndex
	Series []FloatSeries
}
