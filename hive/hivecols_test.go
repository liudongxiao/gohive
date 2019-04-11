package hive

import (
	"fmt"
	"testing"
)

func TestHiveCols(t *testing.T) {
	cols := NewCols([]string{"a", "b", "c", "d", "dt"})
	cols.Set("b", "2")
	cols.Set("d", "4")
	cols.Set("dt", "2016")
	if cols.CsvLineEx(1) != ",2,,4" {
		t.Fatalf("no right ")
	}
	cols.CleanupValues()
	cols.Set("c", "3")
	if cols.CsvLineEx(1) != ",,3," {
		t.Fatalf("no right ")
	}
	if false {
		cols := HiveCols(TableInfo{
			HiveHost: "192.168.1.56",
			HivePort: 10007,
			Db:       "dsp",
			Table:    "dw_whisky_ana_logs",
			User:     "dc",
			Password: "",
		})
		cols.Set("requestid", "3")
		cols.Set("planid", "6666")
		fmt.Println(cols.CsvLineEx(1))
		cols.CleanupValues()
	}
}
