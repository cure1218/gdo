package gdo

import (
	"reflect"
	"strings"
)

func Placeholder(num int) string {
	ph := make([]string, num)
	for i := range ph {
		ph[i] = "?"
	}

	return strings.Join(ph, ",")
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
