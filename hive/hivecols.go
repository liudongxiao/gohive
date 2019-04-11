package hive

// 用于获取 hive 中表的列字段，方便数据解析生成csv

import (
	"fmt"
	"log"
	"strings"
)

type Cols struct {
	Names   []string
	nameMap map[string]int
	values  []string
}

func NewCols(names []string) *Cols {
	// 复制一个，避免外部修改
	colNames := make([]string, 0, len(names))
	nameMap := make(map[string]int, len(names))
	for idx, name := range names {
		colNames = append(colNames, name)
		nameMap[name] = idx
	}
	return &Cols{
		Names:   colNames,
		nameMap: nameMap,
		values:  make([]string, len(colNames), len(colNames)),
	}
}
func (cols *Cols) Len() int {
	return len(cols.Names)
}
func (cols *Cols) IndexOf(colName string) (index int) {
	if idx, ok := cols.nameMap[colName]; ok {
		return idx
	}
	return -1
}

func (cols *Cols) Set(colName string, value string) {
	if idx, ok := cols.nameMap[colName]; ok {
		cols.values[idx] = value
	}
}

func (cols *Cols) CleanupValues() {
	cols.values = make([]string, len(cols.Names), len(cols.Names))
}
func (cols *Cols) CsvLine() string {
	return strings.Join(cols.values, ",")
}

// 可以用来忽略分区字段信息
func (cols *Cols) CsvLineEx(withoutLast int) string {
	return strings.Join(cols.values[:len(cols.values)-1], ",")
}

type TableInfo struct {
	HiveHost string
	HivePort int
	Db       string
	Table    string
	User     string
	Password string
}

func HiveCols(tableInfo TableInfo) *Cols {
	cfg := &Config{
		Host:      fmt.Sprintf("%s:%d", tableInfo.HiveHost, tableInfo.HivePort),
		BenchSize: 3000,
		UserName:  tableInfo.User,
		Password:  tableInfo.Password,
	}
	client, err := NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.Execute(fmt.Sprintf("desc %s.%s", tableInfo.Db, tableInfo.Table))
	if err != nil {
		log.Fatal(err)
	}

	colNames := []string{}
NP:
	for result.NextPage() {
		for result.NextInPage() {
			var name string
			result.Scan(&name)
			if name == "" {
				// 后面是partition信息，忽略
				break NP
			}
			colNames = append(colNames, name)
		}
	}
	if err := result.Err(); err != nil {
		log.Fatal(err)
	}
	return NewCols(colNames)
}
