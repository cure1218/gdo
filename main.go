package gdo

import (
	"crypto/sha512"
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

//================================================================
// Var: Err
//================================================================
var ErrNoRows = sql.ErrNoRows
var ErrConnDone = sql.ErrConnDone
var ErrTxDone = sql.ErrTxDone

//================================================================
// Type: Bool
//================================================================
type NullBool sql.NullBool

func (nb *NullBool) Scan(value interface{}) error {
	var b sql.NullBool

	if err := b.Scan(value); err != nil {
		return err
	}

	if reflect.TypeOf(value) == nil {
		*nb = NullBool{b.Bool, false}
	} else {
		*nb = NullBool{b.Bool, true}
	}

	return nil
}

func (nb *NullBool) MarshalJSON() ([]byte, error) {
	if !nb.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(nb.Bool)
}

func (nb *NullBool) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &nb.Bool)
	nb.Valid = (err == nil)
	return err
}

//================================================================
// Type: Float64
//================================================================
type NullFloat64 sql.NullFloat64

func (nf *NullFloat64) Scan(value interface{}) error {
	var f sql.NullFloat64

	if err := f.Scan(value); err != nil {
		return err
	}

	if reflect.TypeOf(value) == nil {
		*nf = NullFloat64{f.Float64, false}
	} else {
		*nf = NullFloat64{f.Float64, true}
	}

	return nil
}

func (nf *NullFloat64) MarshalJSON() ([]byte, error) {
	if !nf.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(nf.Float64)
}

func (nf *NullFloat64) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &nf.Float64)
	nf.Valid = (err == nil)
	return err
}

//================================================================
// Type: Int64
//================================================================
type NullInt64 sql.NullInt64

func (ni *NullInt64) Scan(value interface{}) error {
	var i sql.NullInt64

	if err := i.Scan(value); err != nil {
		return err
	}

	if reflect.TypeOf(value) == nil {
		*ni = NullInt64{i.Int64, false}
	} else {
		*ni = NullInt64{i.Int64, true}
	}

	return nil
}

func (ni *NullInt64) MarshalJSON() ([]byte, error) {
	if !ni.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ni.Int64)
}

func (ni *NullInt64) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &ni.Int64)
	ni.Valid = (err == nil)
	return err
}

//================================================================
// Type: String
//================================================================
type NullString sql.NullString

func (ns *NullString) Scan(value interface{}) error {
	var s sql.NullString

	if err := s.Scan(value); err != nil {
		return err
	}

	if reflect.TypeOf(value) == nil {
		*ns = NullString{s.String, false}
	} else {
		*ns = NullString{s.String, true}
	}

	return nil
}

func (ns *NullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ns.String)
}

func (ns *NullString) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &ns.String)
	ns.Valid = (err == nil)
	return err
}

//================================================================
// GDO
//================================================================
type Gdo interface {
	Connect() error
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) (*sql.Row, error)
	Exec(string, ...interface{}) (sql.Result, error)
	Ping() error
	Close() error
	Begin() (*sql.Tx, error)
}

type properties struct {
	Db       *sql.DB
	DBType   string
	User     string
	Password string
	Hostname string
	Port     string
	DBName   string
	Protocol string
}

type DBProtocol interface {
	GetDBType() string
	GetUser() string
	GetPassword() string
	GetHost() string
	GetPort() string
	GetDBName() string
	GetProtocol() string
}

func NewGdo(dbType string, user string, password string, hostname string, port string, dbName string) Gdo {
	return &properties{
		DBType:   dbType,
		User:     user,
		Password: password,
		Hostname: hostname,
		Port:     port,
		DBName:   dbName,
	}
}

func NewGdoWithDBProtocol(dbp DBProtocol) Gdo {
	return &properties{
		DBType:   dbp.GetDBType(),
		User:     dbp.GetUser(),
		Password: dbp.GetPassword(),
		Hostname: dbp.GetHost(),
		Port:     dbp.GetPort(),
		DBName:   dbp.GetDBName(),
		Protocol: dbp.GetProtocol(),
	}
}

func (p *properties) Connect() error {
	var err error

	switch p.DBType {
	case "mysql":
		p.Db, err = sql.Open(p.DBType, p.Protocol)
	default:
		err = errors.New("Invalid database type {" + p.DBType + "}")
	}

	return err
}

func (p *properties) Begin() (*sql.Tx, error) {
	return p.Db.Begin()
}

func (p *properties) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := p.Db.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

func (p *properties) QueryRow(sql string, args ...interface{}) (*sql.Row, error) {
	stmt, err := p.Db.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	row := stmt.QueryRow(args...)
	return row, nil
}

func (p *properties) Exec(sql string, args ...interface{}) (sql.Result, error) {
	stmt, err := p.Db.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	return stmt.Exec(args...)
}

func (p *properties) Ping() error {
	return p.Db.Ping()
}

func (p *properties) Close() error {
	return p.Db.Close()
}

func GenUpdateSubsqlAndArgs(data interface{}, fieldMap *map[string]string, args *[]interface{}) string {
	cols := []string{}
	v := reflect.ValueOf(data)
	vt := reflect.Indirect(v).Type()
	numField := v.NumField()

	for i := 0; i < numField; i++ {
		vf := v.Field(i)
		if !vf.IsZero() {
			cols = append(cols, (*fieldMap)[vt.Field(i).Name]+" = ?")
			*args = append(*args, vf.Interface())
		}
	}

	return strings.Join(cols, ",")
}

func GenInsertSubsqlAndArgs(data interface{}, fieldMap *map[string]string) (string, string, *[]interface{}) {
	cols := []string{}
	placeholder := []string{}
	values := []interface{}{}
	v := reflect.ValueOf(data)
	vt := reflect.Indirect(v).Type()
	numField := v.NumField()

	for i := 0; i < numField; i++ {
		vf := v.Field(i)
		if !vf.IsZero() {
			cols = append(cols, (*fieldMap)[vt.Field(i).Name])
			placeholder = append(placeholder, "?")
			values = append(values, vf.Interface())
		}
	}

	return strings.Join(cols, ","), strings.Join(placeholder, ","), &values
}

func Sha512BinaryStr(s string) string {
	hash := sha512.Sum512([]byte(s))
	checksum := make([]byte, sha512.Size)
	for i := range hash {
		checksum[i] = hash[i]
	}

	return string(checksum)
}

func Placeholder(num int) string {
	ph := make([]string, num)
	for i := range ph {
		ph[i] = "?"
	}

	return strings.Join(ph, ",")
}
