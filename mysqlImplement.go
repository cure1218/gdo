package gdo

import (
	"database/sql"
)

type MysqlImplement struct {
	DB *sql.DB
}

func (mi *MysqlImplement) Ping() error {
	return mi.DB.Ping()
}

func (mi *MysqlImplement) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := mi.DB.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

func (mi *MysqlImplement) QueryRow(sql string, args ...interface{}) (*sql.Row, error) {
	stmt, err := mi.DB.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	row := stmt.QueryRow(args...)
	return row, nil
}

func (mi *MysqlImplement) Exec(sql string, args ...interface{}) (sql.Result, error) {
	stmt, err := mi.DB.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	return stmt.Exec(args...)
}

func (mi *MysqlImplement) Begin() (*sql.Tx, error) {
	return mi.DB.Begin()
}

func (mi *MysqlImplement) Close() error {
	return mi.DB.Close()
}
