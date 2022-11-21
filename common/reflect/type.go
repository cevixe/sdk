package reflect

import (
	"reflect"
)

func GetTypeName(object interface{}) string {
	rv := reflect.ValueOf(object)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	return rv.Type().Name()
}
