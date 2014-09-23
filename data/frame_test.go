// Copyright 2014 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package frame

import (
	"testing"
	"time"
)

var testRecords1 = []struct{
	Name  string
	Stamp time.Time
	ValF  float64
	ValI  int64
}{
	{
		Name: "a",
		Stamp: time.Unix(1411430400, 0),
		ValF: 3.14,
		ValI: 42,
	},
}

func TestNewDataFrame(t *testing.T) {
	df, err := NewDataFrameFromRecords(testRecords1, "", -1)
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
