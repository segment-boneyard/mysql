package main

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/segment-sources/sqlsource/domain"
)

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

func (m *MySQL) Scan(t *domain.Table) (*sqlx.Rows, error) {
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", t.ColumnToSQL(), t.SchemaName, t.TableName)
	logrus.Debugf("Executing query: %v", query)

	return m.Connection.Queryx(query)
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
