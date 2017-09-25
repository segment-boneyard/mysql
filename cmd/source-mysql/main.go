package main

import (
	"github.com/segment-sources/mysql"
	"github.com/segment-sources/sqlsource"
)

func main() {
	sqlsource.Run(&mysql.MySQL{})
}
