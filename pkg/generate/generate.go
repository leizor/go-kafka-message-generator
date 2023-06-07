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
				return fmt.Errorf("problem reading file '%s': %w", entry.Name(), err)
			}

			spec := model.Message{}
			err = json.Unmarshal(skipCommentLines(data), &spec)
			if err != nil {
				return fmt.Errorf("problem unmarshalling json in '%s': %w", entry.Name(), err)
			}

			filename, cb, err := generateFile(*packageName, spec)
			if err != nil {
				return fmt.Errorf("problem generating file for '%s': %w", entry.Name(), err)
			}
			err = writeFile(filepath.Join(*out, filename), cb)
			if err != nil {
				return fmt.Errorf("problem writing file '%s': %w", filename, err)
			}
		}
	}

	return nil
}

func skipCommentLines(data []byte) []byte {
	dataStr := string(data)
	var sb strings.Builder

	for _, s := range strings.Split(dataStr, "\n") {
		if !strings.HasPrefix(s, "//") {
			sb.WriteString(s)
			sb.WriteString("\n")
		}
	}

	return []byte(sb.String())
}

var arrayRegex = regexp.MustCompile("^(\\[])?(.+)$")

func generateFile(packageName string, spec model.Message) (string, util.CodeBuffer, error) {
	cb := util.NewCodeBuffer()

	cb.AddLine("package %s", packageName)

	cb.AddLine("import \"encoding/binary\"")

	cb.AddLine("type %s struct {", spec.Name)
	cb.IncrementIndent()
	for _, field := range spec.Fields {
		cb.AddLine("%s %s", capitalize(field.Name), field.Type)
	}
	cb.DecrementIndent()
	cb.AddLine("}")

	for _, cs := range spec.CommonStructs {
		cb.AddLine("type %s struct {", cs.Name)
		cb.IncrementIndent()
		for _, field := range cs.Fields {
			cb.AddLine("%s %s", capitalize(field.Name), field.Type)
		}
		cb.DecrementIndent()
		cb.AddLine("}")
	}

	cb.AddLine("func Read%s(data []bytes, version int) (%s, error) {", spec.Name, spec.Name)
	cb.IncrementIndent()
	cb.AddLine("var res %s", spec.Name)

	for _, field := range spec.Fields {
		versions := addVersionIfClause(cb, field.Versions)
		if versions {
			cb.IncrementIndent()
		}
		// TODO: Handle using common structs
		m := arrayRegex.FindStringSubmatch(field.Type)
		if m[1] == "[]" {
			cb.AddLine("{")
			cb.IncrementIndent()
			cb.AddLine("arrLen, err := binary.ReadUvarint(data)")
			cb.AddLine("if err != nil {")
			cb.IncrementIndent()
			cb.AddLine("return res, err")
			cb.DecrementIndent()
			cb.AddLine("}")
			cb.AddLine("arrLen--")
			cb.AddLine("for i := 0; i < arrLen; i++ {")
			cb.IncrementIndent()
			switch m[2] {
			case "string":
				addReadString(cb, field.Name, true)
			case "int32":
				cb.AddLine("res.%s = append(res.%s, int32(binary.BigEndian().Uint32(data))", capitalize(field.Name), capitalize(field.Name))
			case "int64":
				cb.AddLine("res.%s = append(res.%s, int32(binary.BigEndian().Uint64(data))", capitalize(field.Name), capitalize(field.Name))
			default:
				return "", nil, fmt.Errorf("unrecognized field type: %s", m[2])
			}
			cb.DecrementIndent()
			cb.AddLine("}")
			cb.DecrementIndent()
			cb.AddLine("}")
		} else {
			switch m[2] {
			case "string":
				addReadString(cb, field.Name, false)
			case "int32":
				cb.AddLine("res.%s = int32(binary.BigEndian().Uint32(data))", capitalize(field.Name))
			case "int64":
				cb.AddLine("res.%s = int64(binary.BigEndian().Uint64(data))", capitalize(field.Name))
			default:
				return "", nil, fmt.Errorf("unrecognized field type: %s", m[2])
			}
		}
		if versions {
			cb.DecrementIndent()
			if field.Default != nil {
				cb.AddLine("} else {")
				cb.IncrementIndent()
				switch field.Type {
				case "string":
					defaultString, ok := (*field.Default).(string)
					if !ok {
						return "", nil, fmt.Errorf("unexpected value type for string default: %v (%T)", *field.Default, *field.Default)
					}
					cb.AddLine("res.%s = %s", capitalize(field.Name), defaultString)
				case "int32", "int64":
					defaultInt, ok := (*field.Default).(int)
					if !ok {
						return "", nil, fmt.Errorf("unexpected value type for int default: %v (%T)", *field.Default, *field.Default)
					}
					cb.AddLine("res.%s = %d", capitalize(field.Name), defaultInt)
				default:
					return "", nil, fmt.Errorf("unrecognized field type: %s", field.Type)
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

	return spec.Name + ".go", cb, nil
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

func addReadString(cb util.CodeBuffer, fieldName string, appendToArray bool) {
	cb.AddLine("{")
	cb.IncrementIndent()

	cb.AddLine("stringLen := binary.BigEndian().Uint16(data)")
	cb.AddLine("if stringLen < 0 {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"non-nullable field group was serialized as null\")")
	cb.DecrementIndent()
	cb.AddLine("} else if stringLen > 0x7fff {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"string field group had invalid length \" + stringLen)")
	cb.DecrementIndent()
	cb.AddLine("} else {")
	cb.IncrementIndent()
	if appendToArray {
		cb.AddLine("res.%s = append(res.%s, string(data[0:stringLen]))", capitalize(fieldName), capitalize(fieldName))
	} else {
		cb.AddLine("res.%s = string(data[0:stringLen])", capitalize(fieldName))
	}
	cb.AddLine("data = data[stringLen:]")
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
