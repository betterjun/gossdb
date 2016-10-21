package ssdb

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

// formatData format data before sending to server.
func formatData(args []interface{}) ([]byte, error) {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case []int:
			for _, d := range arg {
				s = fmt.Sprintf("%d", d)
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case []interface{}:
			for _, d := range arg {
				v, err := formatInterface(d)
				if err != nil {
					return nil, err
				}
				s = v
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		default:
			v, err := formatInterface(arg)
			if err != nil {
				return nil, err
			}
			s = v
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// formatInterface formats any supported value as a string,
// but returns an error for unsupported.
func formatInterface(value interface{}) (string, error) {
	return formatAtom(reflect.ValueOf(value))
}

// formatAtom formats a built-in value without inspecting its internal structure.
func formatAtom(v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.Invalid:
		return "", nil
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'f', 10, 32), nil
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', 10, 64), nil
	case reflect.Bool:
		if v.Bool() {
			return "1", nil
		} else {
			return "0", nil
		}
	case reflect.String:
		return v.String(), nil
	default:
		return "", fmt.Errorf("unsupported data type %v", v.Kind())
	}
}
