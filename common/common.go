package common

import (
	"fmt"
	"strings"
)

func FormatValue(s interface{}) (string, string) {
	switch s.(type) {
	case string:
		return "string", strings.ReplaceAll(strings.ReplaceAll(fmt.Sprintf("%v", s), "'", `\'`), `"`, `\"`)
	case int32, int, int64, int16, int8:
		return "int", strings.ReplaceAll(strings.ReplaceAll(fmt.Sprintf("%v", s), "'", `\'`), `"`, `\"`)
	default:
		return "error", fmt.Sprintf("err:%v", s)
	}

}
