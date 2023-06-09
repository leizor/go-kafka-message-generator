package generate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/leizor/go-kafka-message-generator/pkg/model"
	"github.com/leizor/go-kafka-message-generator/pkg/util"
)

func Run(packageName *string, in *[]string, out *string) error {
	err := os.MkdirAll(*out, os.ModePerm)
	if err != nil {
		return err
	}

	for _, dir := range *in {
		dirEntries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range dirEntries {
			if !entry.IsDir() {
				data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
				if err != nil {
					return fmt.Errorf("problem reading file '%s': %w", entry.Name(), err)
				}

				spec := model.Message{}
				err = json.Unmarshal(skipCommentLines(data), &spec)
				if err != nil {
					return fmt.Errorf("problem unmarshalling json in '%s': %w", entry.Name(), err)
				}

				cb := util.NewCodeBuffer()
				filename, err := generateFile(*packageName, spec, cb)
				if err != nil {
					return fmt.Errorf("problem generating file for '%s': %w", entry.Name(), err)
				}
				err = writeFile(filepath.Join(*out, filename), cb)
				if err != nil {
					return fmt.Errorf("problem writing file '%s': %w", filename, err)
				}
			}
		}
	}

	return nil
}

var (
	isCommentedLineRegex = regexp.MustCompile("^\\s*//.*$")
	arrayRegex           = regexp.MustCompile("^(\\[])?(.+)$")
	versionRangeRegexp   = regexp.MustCompile("(\\d+)-(\\d+)")
	versionMinRegexp     = regexp.MustCompile("(\\d+)\\+")
	versionRegexp        = regexp.MustCompile("(\\d+)")
)

func skipCommentLines(data []byte) []byte {
	dataStr := string(data)
	var sb strings.Builder

	for _, s := range strings.Split(dataStr, "\n") {
		if !isCommentedLineRegex.MatchString(s) {
			sb.WriteString(s)
			sb.WriteString("\n")
		}
	}

	return []byte(sb.String())
}

func generateFile(packageName string, spec model.Message, cb util.CodeBuffer) (string, error) {
	cb.AddLine("package %s", packageName)

	cb.AddLine("import (")
	cb.IncrementIndent()
	for _, dep := range collectImports(spec) {
		cb.AddLine("\"%s\"", dep)
	}
	cb.DecrementIndent()
	cb.AddLine(")")

	cb.AddLine("type %s struct {", spec.Name)
	cb.IncrementIndent()
	addStructFields(cb, spec.Name, spec.Fields)
	cb.DecrementIndent()
	cb.AddLine("}")

	for _, cs := range spec.CommonStructs {
		err := addCommonStruct(cb, spec.Name, cs)
		if err != nil {
			return "", fmt.Errorf("problem adding common struct: %w", err)
		}
	}

	for _, inlineStruct := range collectInlineStructs(spec.Fields) {
		err := addInlineStruct(cb, spec.Name, inlineStruct)
		if err != nil {
			return "", fmt.Errorf("problem adding inline struct: %w", err)
		}
	}

	cb.AddLine("func Read%s(data []byte, version int) (%s, error) {", spec.Name, spec.Name)
	cb.IncrementIndent()
	cb.AddLine("var res %s", spec.Name)

	for _, field := range spec.Fields {
		err := addReadField(cb, spec.Name, field)
		if err != nil {
			return "", err
		}
	}
	cb.AddLine("return res, nil")
	cb.DecrementIndent()
	cb.AddLine("}")

	// TODO: Support tagged fields
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields

	return spec.Name + ".go", nil
}

// We depend on the following built-in packages, but only selectively depending on the data types used in each file:
//   - bytes
//   - encoding/binary
//   - fmt
func collectImports(spec model.Message) []string {
	dependsOn := map[string]bool{
		"bytes":           false,
		"encoding/binary": true,
		"fmt":             false,
	}

	fields := make([]model.MessageField, 0, len(spec.Fields)+len(spec.CommonStructs))
	for _, f := range spec.Fields {
		fields = append(fields, f)
	}
	for _, cs := range spec.CommonStructs {
		for _, f := range cs.Fields {
			fields = append(fields, f)
		}
	}

	for _, field := range fields {
		isArray, fieldType := deconstructFieldType(field.Type)
		if isArray {
			dependsOn["bytes"] = true
			dependsOn["fmt"] = true
		}
		switch fieldType {
		case "bytes":
			dependsOn["bytes"] = true
			dependsOn["fmt"] = true
		case "string":
			dependsOn["fmt"] = true
		case "int8", "int16", "int32", "int64", "uuid":
			// The encoding/binary package is always a dependency so we already set it to true above.
		default:
			dependsOn["fmt"] = true
		}
	}

	imports := make([]string, 0, len(dependsOn))
	for dep, ok := range dependsOn {
		if ok {
			imports = append(imports, dep)
		}
	}
	sort.Strings(imports)
	return imports
}

func collectInlineStructs(fields []model.MessageField) (res []model.MessageField) {
	for _, field := range fields {
		if len(field.Fields) > 0 {
			res = append(res, field)
			res = append(res, collectInlineStructs(field.Fields)...)
		}
	}
	return res
}

func addStructFields(cb util.CodeBuffer, name string, fields []model.MessageField) {
	for _, field := range fields {
		fieldType := field.Type
		switch fieldType {
		case "uuid":
			fieldType = "uint16"
		case "bytes":
			fieldType = "[]byte"
		}
		if len(field.Fields) > 0 {
			// This is an inline struct.
			isArray, ft := deconstructFieldType(fieldType)
			if isArray {
				cb.AddLine("%s []%s%s", capitalize(field.Name), name, ft)
			} else {
				cb.AddLine("%s %s%s", capitalize(field.Name), name, fieldType)
			}
		} else {
			cb.AddLine("%s %s", capitalize(field.Name), fieldType)
		}
	}
}

func addCommonStruct(cb util.CodeBuffer, name string, cs model.CommonStruct) error {
	versions := addVersionIfClause(cb, cs.Versions)
	if versions {
		cb.IncrementIndent()
	}

	cb.AddLine("type %s struct {", cs.Name)
	cb.IncrementIndent()
	addStructFields(cb, name, cs.Fields)
	cb.DecrementIndent()
	cb.AddLine("}")

	cb.AddLine("func New%s(data []byte) (%s, error) {", capitalize(cs.Name), capitalize(cs.Name))
	cb.IncrementIndent()
	cb.AddLine("var res %s", capitalize(cs.Name))
	for _, field := range cs.Fields {
		err := addReadField(cb, name, field)
		if err != nil {
			return err
		}
	}
	cb.AddLine("return res, nil")
	cb.DecrementIndent()
	cb.AddLine("}")

	if versions {
		cb.DecrementIndent()
		cb.AddLine("}")
	}

	return nil
}

// addInlineStruct adds inline structs to the code buffer. An inline struct is like a common struct but is not accessed
// by other readers in the package. In practice, the only difference is that we prepend the message name to the struct
// name.
func addInlineStruct(cb util.CodeBuffer, name string, field model.MessageField) error {
	versions := addVersionIfClause(cb, field.Versions)
	if versions {
		cb.IncrementIndent()
	}

	_, fieldType := deconstructFieldType(field.Type)
	cb.AddLine("type %s%s struct {", name, fieldType)
	cb.IncrementIndent()
	addStructFields(cb, name, field.Fields)
	cb.DecrementIndent()
	cb.AddLine("}")

	cb.AddLine("func New%s%s(data []byte) (%s%s, error) {", name, capitalize(fieldType), name, capitalize(fieldType))
	cb.IncrementIndent()
	cb.AddLine("var res %s%s", name, capitalize(fieldType))
	for _, f := range field.Fields {
		err := addReadField(cb, name, f)
		if err != nil {
			return err
		}
	}
	cb.AddLine("return res, nil")
	cb.DecrementIndent()
	cb.AddLine("}")

	if versions {
		cb.DecrementIndent()
		cb.AddLine("}")
	}

	return nil
}

func addReadField(cb util.CodeBuffer, name string, field model.MessageField) error {
	versions := addVersionIfClause(cb, field.Versions)
	if versions {
		cb.IncrementIndent()
	}

	isArray, fieldType := deconstructFieldType(field.Type)
	if isArray {
		cb.AddLine("{")
		cb.IncrementIndent()
		cb.AddLine("arrLen, err := binary.ReadUvarint(bytes.NewReader(data))")
		cb.AddLine("if err != nil {")
		cb.IncrementIndent()
		cb.AddLine("return res, fmt.Errorf(\"problem reading uvarint: %%w\", err)")
		cb.DecrementIndent()
		cb.AddLine("}")
		cb.AddLine("arrLen--")
		cb.AddLine("for i := uint64(0); i < arrLen; i++ {")
		cb.IncrementIndent()
		switch fieldType {
		case "int8":
			cb.AddLine("res.%s = append(res.%s, int8(data[0]))", capitalize(field.Name), capitalize(field.Name))
			cb.AddLine("data = data[1:]")
		case "int16":
			cb.AddLine("res.%s = append(res.%s, int16(binary.BigEndian.Uint16(data)))", capitalize(field.Name), capitalize(field.Name))
		case "int32":
			cb.AddLine("res.%s = append(res.%s, int32(binary.BigEndian.Uint32(data)))", capitalize(field.Name), capitalize(field.Name))
		case "int64":
			cb.AddLine("res.%s = append(res.%s, int32(binary.BigEndian.Uint64(data)))", capitalize(field.Name), capitalize(field.Name))
		case "string":
			addReadString(cb, field.Name, true)
		case "uuid":
			cb.AddLine("res.%s = append(res.%s, binary.BigEndian.Uint16(data))", capitalize(field.Name), capitalize(field.Name))
		case "bytes":
			addReadBytes(cb, field.Name, true)
		default:
			cb.AddLine("{")
			cb.IncrementIndent()
			if len(field.Fields) > 0 {
				addAllocateInlineStruct(cb, name, field.Type)
			} else {
				addAllocateCommonStruct(cb, field.Type)
			}
			cb.AddLine("res.%s = append(res.%s, v)", capitalize(field.Name), capitalize(field.Name))
			cb.DecrementIndent()
			cb.AddLine("}")
		}
		cb.DecrementIndent()
		cb.AddLine("}")
		cb.DecrementIndent()
		cb.AddLine("}")
	} else {
		switch fieldType {
		case "int8":
			cb.AddLine("res.%s = int8(data[0])", capitalize(field.Name))
			cb.AddLine("data = data[1:]")
		case "int16":
			cb.AddLine("res.%s = int16(binary.BigEndian.Uint16(data))", capitalize(field.Name))
		case "int32":
			cb.AddLine("res.%s = int32(binary.BigEndian.Uint32(data))", capitalize(field.Name))
		case "int64":
			cb.AddLine("res.%s = int64(binary.BigEndian.Uint64(data))", capitalize(field.Name))
		case "string":
			addReadString(cb, field.Name, false)
		case "uuid":
			cb.AddLine("res.%s = binary.BigEndian.Uint16(data)", capitalize(field.Name))
		case "bytes":
			addReadBytes(cb, field.Name, false)
		default:
			cb.AddLine("{")
			cb.IncrementIndent()
			if len(field.Fields) > 0 {
				addAllocateInlineStruct(cb, name, field.Type)
			} else {
				addAllocateCommonStruct(cb, field.Type)
			}
			cb.AddLine("res.%s = v", capitalize(field.Name))
			cb.DecrementIndent()
			cb.AddLine("}")
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
					return fmt.Errorf("unexpected value type for string default: %v (%T)", *field.Default, *field.Default)
				}
				cb.AddLine("res.%s = %s", capitalize(field.Name), defaultString)
			case "int32", "int64":
				defaultInt, ok := (*field.Default).(int)
				if !ok {
					return fmt.Errorf("unexpected value type for int default: %v (%T)", *field.Default, *field.Default)
				}
				cb.AddLine("res.%s = %d", capitalize(field.Name), defaultInt)
			default:
				return fmt.Errorf("unrecognized field type: %s", field.Type)
			}
			cb.DecrementIndent()
		}
		cb.AddLine("}")
	}

	return nil
}

func deconstructFieldType(fieldType string) (bool, string) {
	m := arrayRegex.FindStringSubmatch(fieldType)
	return m[1] == "[]", m[2]
}

func capitalize(s string) string {
	return strings.ToUpper(s[0:1]) + s[1:]
}

func addAllocateCommonStruct(cb util.CodeBuffer, fieldType string) {
	_, ft := deconstructFieldType(fieldType)
	constructName := fmt.Sprintf("New%s", capitalize(ft))

	cb.AddLine("v, err := %s(data)", constructName)
	cb.AddLine("if err != nil {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"problem building %s: %%w\", err)", capitalize(ft))
	cb.DecrementIndent()
	cb.AddLine("}")
}

func addAllocateInlineStruct(cb util.CodeBuffer, name string, fieldType string) {
	_, ft := deconstructFieldType(fieldType)
	constructName := fmt.Sprintf("New%s%s", name, capitalize(ft))

	cb.AddLine("v, err := %s(data)", constructName)
	cb.AddLine("if err != nil {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"problem building %s%s: %%w\", err)", name, capitalize(ft))
	cb.DecrementIndent()
	cb.AddLine("}")
}

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

	cb.AddLine("stringLen := binary.BigEndian.Uint16(data)")
	cb.AddLine("if stringLen < 0 {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"non-nullable field %s was serialized as null\")", fieldName)
	cb.DecrementIndent()
	cb.AddLine("} else if stringLen > 0x7fff {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"string field %s had invalid length %%d\", stringLen)", fieldName)
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

func addReadBytes(cb util.CodeBuffer, fieldName string, appendToArray bool) {
	cb.AddLine("{")
	cb.IncrementIndent()

	cb.AddLine("bytesLen, err := binary.ReadUvarint(bytes.NewReader(data))")
	cb.AddLine("if err != nil {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"problem reading uvarint: %%w\", err)")
	cb.DecrementIndent()
	cb.AddLine("}")
	cb.AddLine("if bytesLen < 0 {")
	cb.IncrementIndent()
	cb.AddLine("return res, fmt.Errorf(\"non-nullable field group was serialized as null\")")
	cb.DecrementIndent()
	cb.AddLine("}")

	if appendToArray {
		cb.AddLine("res.%s = append(res.%s, data[0:bytesLen])", capitalize(fieldName), capitalize(fieldName))
	} else {
		cb.AddLine("res.%s = data[0:bytesLen]", capitalize(fieldName))
	}
	cb.AddLine("data = data[bytesLen:]")

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
