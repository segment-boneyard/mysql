package mysql

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segment-sources/sqlsource/driver"
)

const chunkSize = 1000000

type tableDescriptionRow struct {
	Catalog    string `db:"table_catalog"`
	SchemaName string `db:"table_schema"`
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	IsPrimary  bool   `db:"is_primary_key"`
}

type MySQL struct {
	Connection *sqlx.DB
}

func (m *MySQL) Init(c *domain.Config) error {
	config := mysql.Config{
		User:   c.Username,
		Passwd: c.Password,
		DBName: c.Database,
		Net:    "tcp",
		Addr:   c.Hostname + ":" + c.Port,
		Params: map[string]string{},
	}

	for _, option := range c.ExtraOptions {
		splitEq := strings.Split(option, "=")
		if len(splitEq) != 2 {
			continue
		}
		config.Params[splitEq[0]] = splitEq[1]
	}

	db, err := sqlx.Connect("mysql", config.FormatDSN())
	if err != nil {
		return err
	}

	m.Connection = db

	return nil
}

func (m *MySQL) Scan(t *domain.Table, lastPkValues []interface{}) (driver.SqlRows, error) {
	// in most cases whereClause will simply look like "id" > 114, but since the source supports compound PKs
	// we must be able to include all PK columns in the query. For example, for a table with 3-column PK:
	//	a | b | c
	//	---+---+---
	//	1 | 1 | 1
	//	1 | 1 | 2
	//	1 | 2 | 1
	//	1 | 2 | 2
	//	2 | 1 | 1
	//
	// whereClause selecting records after (1, 1, 1) should look like:
	// a > 1 OR a = 1 AND b > 1 OR a = 1 AND b = 1 AND c > 1
	whereClause := "true"
	bindVars := []interface{}{}
	if len(lastPkValues) > 0 {
		// {"a > 1", "a = 1 AND b > 1", "a = 1 AND b = 1 AND c > 1"}
		whereOrList := []string{}

		for i, pk := range t.PrimaryKeys {
			// {"a = 1", "b = 1", "c > 1"}
			choiceAndList := []string{}
			for j := 0; j < i; j++ {
				choiceAndList = append(choiceAndList, fmt.Sprintf("`%s` = ?", t.PrimaryKeys[j]))
				bindVars = append(bindVars, lastPkValues[j])
			}
			choiceAndList = append(choiceAndList, fmt.Sprintf("`%s` > ?", pk))
			bindVars = append(bindVars, lastPkValues[i])
			whereOrList = append(whereOrList, strings.Join(choiceAndList, " AND "))
		}
		whereClause = strings.Join(whereOrList, " OR ")
	}

	orderByList := make([]string, 0, len(t.PrimaryKeys))
	for _, column := range t.PrimaryKeys {
		orderByList = append(orderByList, fmt.Sprintf("`%s`", column))
	}
	orderByClause := strings.Join(orderByList, ", ")

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s LIMIT %d", mysqlColumnsToSQL(t), t.SchemaName,
		t.TableName, whereClause, orderByClause, chunkSize)

	logger := logrus.WithFields(logrus.Fields{
		"sql":  query,
		"args": bindVars,
	})
	logger.Debugf("Executing query")

	stmt, err := m.Connection.Preparex(query)
	if err != nil {
		return nil, err
	}

	return stmt.Queryx(bindVars...)
}

func (m *MySQL) Transform(row map[string]interface{}) map[string]interface{} {
	// The MySQL driver returns text and date columns as []byte instead of string.
	for k, v := range row {
		switch val := v.(type) {
		case []byte:
			row[k] = string(val)
		}
	}

	return row
}

func mysqlColumnsToSQL(t *domain.Table) string {
	var c []string
	for _, column := range t.Columns {
		c = append(c, fmt.Sprintf("`%s`", column))
	}

	return strings.Join(c, ", ")
}

func (m *MySQL) Describe() (*domain.Description, error) {
	describeQuery := `
        SELECT table_schema, table_name, column_name, CASE column_key WHEN 'PRI' THEN true ELSE false END as is_primary_key
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
    `

	res := domain.NewDescription()

	rows, err := m.Connection.Queryx(describeQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		row := &tableDescriptionRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		res.AddColumn(&domain.Column{Name: row.ColumnName, Schema: row.SchemaName, Table: row.TableName, IsPrimaryKey: row.IsPrimary})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
