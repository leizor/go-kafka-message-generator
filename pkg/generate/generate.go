package generate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/leizor/go-kafka-message-generator/pkg/model"
	"github.com/leizor/go-kafka-message-generator/pkg/util"
)

func Run(packageName, in, out *string) error {
	err := os.MkdirAll(*out, os.ModePerm)
	if err != nil {
		return err
	}

	dirEntries, err := os.ReadDir(*in)
	if err != nil {
		return err
	}
	for _, entry := range dirEntries {
		if !entry.IsDir() {
			data, err := os.ReadFile(filepath.Join(*in, entry.Name()))
			if err != nil {
				return err
			}

			spec := model.Message{}
			err = json.Unmarshal(data, &spec)
			if err != nil {
				return err
			}

			filename, cb, err := generateFile(*packageName, spec)
			if err != nil {
				return err
			}
			err = writeFile(filepath.Join(*out, filename), cb)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func generateFile(packageName string, spec model.Message) (string, util.CodeBuffer, error) {
	cb := util.NewCodeBuffer()

	cb.AddLine("package %s", packageName)

	cb.AddLine("import \"encoding/binary\"")

	cb.AddLine("type %s struct{", spec.Name)
	cb.IncrementIndent()
	for _, field := range spec.Fields {
		cb.AddLine("%s %s", capitalize(field.Name), field.FieldType)
	}
	cb.DecrementIndent()
	cb.AddLine("}")

	cb.AddLine("func Read%s(data []bytes, version int) (%s, error) {", spec.Name, spec.Name)
	cb.IncrementIndent()
	cb.AddLine("var res %s", spec.Name)

	for _, field := range spec.Fields {
		versions := addVersionIfClause(cb, field.Versions)
		if versions {
			cb.IncrementIndent()
		}
		switch field.FieldType {
		case "string":
			addReadString(cb, field.Name)
		case "int32":
			cb.AddLine("res.%s = int32(binary.BigEndian().Uint32(data))", capitalize(field.Name))
		case "int64":
			cb.AddLine("res.%s = int64(binary.BigEndian().Uint64(data))", capitalize(field.Name))
		default:
			return "", nil, fmt.Errorf("unrecognized field type: %s", field.FieldType)
		}
		if versions {
			cb.DecrementIndent()
			if field.DefaultVal != nil {
				cb.AddLine("} else {")
				cb.IncrementIndent()
				switch field.FieldType {
				case "string":
					defaultString, ok := (*field.DefaultVal).(string)
					if !ok {
						return "", nil, fmt.Errorf("unexpected value type for string default: %v (%T)", *field.DefaultVal, *field.DefaultVal)
					}
					cb.AddLine("res.%s = %s", capitalize(field.Name), defaultString)
				case "int32", "int64":
					defaultInt, ok := (*field.DefaultVal).(int)
					if !ok {
						return "", nil, fmt.Errorf("unexpected value type for int default: %v (%T)", *field.DefaultVal, *field.DefaultVal)
					}
					cb.AddLine("res.%s = %d", capitalize(field.Name), defaultInt)
				default:
					return "", nil, fmt.Errorf("unrecognized field type: %s", field.FieldType)
				}
				cb.DecrementIndent()
			}
			cb.AddLine("}")
		}
	}
	cb.DecrementIndent()
	cb.AddLine("}")

	// TODO: Support tagged fields
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields

	return spec.Name, cb, nil
}

func capitalize(s string) string {
	return strings.ToUpper(s[0:1]) + s[1:]
}

var (
	versionRangeRegexp = regexp.MustCompile("(\\d+)-(\\d+)")
	versionMinRegexp   = regexp.MustCompile("(\\d+)\\+")
	versionRegexp      = regexp.MustCompile("(\\d+)")
)

func addVersionIfClause(cb util.CodeBuffer, versions string) bool {
	if m := versionRangeRegexp.FindStringSubmatch(versions); len(m) == 2 {
		cb.AddLine("if version >= %d || version <= %d", m[0], m[1])
		return true
	} else if m := versionMinRegexp.FindStringSubmatch(versions); len(m) == 1 {
		cb.AddLine("if version >= %d", m[0])
		return true
	} else if m := versionRegexp.FindStringSubmatch(versions); len(m) == 1 {
		cb.AddLine("if version == %d", m[0])
		return true
	}
	return false
}

func addReadString(cb util.CodeBuffer, fieldName string) {
	cb.AddLine("{")
	cb.IncrementIndent()

	cb.AddLine("length := binary.BigEndian().Uint16(data)")
	cb.AddLine("if length < 0 {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"non-nullable field group was serialized as null\")")
	cb.DecrementIndent()
	cb.AddLine("} else if length > 0x7fff {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"string field group had invalid length \" + length)")
	cb.DecrementIndent()
	cb.AddLine("} else {")
	cb.IncrementIndent()
	cb.AddLine("res.%s = string(data[0:length])", capitalize(fieldName))
	cb.AddLine("data = data[length:]")
	cb.DecrementIndent()
	cb.AddLine("}")

	cb.DecrementIndent()
	cb.AddLine("}")
}

func writeFile(fullPath string, cb util.CodeBuffer) error {
	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	return cb.Write(bufio.NewWriter(f))
}
