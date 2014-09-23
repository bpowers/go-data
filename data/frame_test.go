// Copyright 2014 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package data

import (
	"testing"
	"time"
)

var testRecords1 = []struct {
	Name string `json:"name"`
	Stamp time.Time
	ValF float64
	ValI int64
}{
	{
		Name: "a",
		Stamp: time.Unix(1411430400, 0),
		ValF: 3.14,
		ValI: 42,
	},
	{
		Name: "b",
		Stamp: time.Unix(1411430400, 0),
		ValF: 2.0,
		ValI: 10,
	},
}

func TestNewDataFrame(t *testing.T) {
	df, err := NewDataFrameFromRecords("lulz", "", -1)
	if err == nil {
		t.Errorf("NewDataFrameFromRecords should fail for string rows")
		return
	}

	df, err = NewDataFrameFromRecords(testRecords1, "Name", -1)
	if err != nil {
		t.Errorf("NewDataFrameFromRecords: %s", err)
		return
	}
	if df == nil {
		t.Errorf("nil df")
		return
	}
	if df.ColIndex.Len() != 4 {
		t.Errorf("Bad ncols: %d != %d", df.ColIndex.Len(), 4)
		return
	}
}
