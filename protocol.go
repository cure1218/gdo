package gdo

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

//================================================================
//
//================================================================
type Protocol struct {
	DBType   string
	User     string
	Password string
	Host     string
	Port     string
	DBName   string
	Params   string
}

func (p *Protocol) GetProtocol() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s",
		p.User,
		p.Password,
		p.Host,
		p.Port,
		p.DBName,
		p.Params,
	)
}

func (p *Protocol) Connect() (Gdo, error) {
	var err error

	switch p.DBType {
	case "mysql":
		mi := &MysqlImplement{}
		if mi.DB, err = sql.Open("mysql", p.GetProtocol()); err != nil {
			return nil, err
		}
		if err = mi.Ping(); err != nil {
			return nil, err
		}

		return mi, nil
	}

	return nil, errors.New("Invalid DBType.")
}
