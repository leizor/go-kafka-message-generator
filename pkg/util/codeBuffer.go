package util

import (
	"bufio"
	"fmt"
	"strings"
)

type CodeBuffer interface {
	IncrementIndent()
	DecrementIndent()
	AddLine(format string, a ...any)
	WriteTo(other CodeBuffer)
	Write(w *bufio.Writer) error
}

func NewCodeBuffer() CodeBuffer {
	return &codeBuffer{indent: "    "}
}

type codeBuffer struct {
	lines       []string
	indentLevel int
	indent      string
}

func (cb *codeBuffer) IncrementIndent() {
	cb.indentLevel++
}

func (cb *codeBuffer) DecrementIndent() {
	cb.indentLevel--
	if cb.indentLevel < 0 {
		panic("indentLevel < 0")
	}
}

func (cb *codeBuffer) AddLine(format string, a ...any) {
	line := fmt.Sprintf(cb.indentSpaces()+format, a...)
	cb.lines = append(cb.lines, line)
}

func (cb *codeBuffer) WriteTo(other CodeBuffer) {
	for _, line := range cb.lines {
		other.AddLine(line)
	}
}

func (cb *codeBuffer) Write(w *bufio.Writer) error {
	for _, line := range cb.lines {
		_, err := w.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	return w.Flush()
}

func (cb *codeBuffer) indentSpaces() string {
	var sb strings.Builder
	for i := 0; i < cb.indentLevel; i++ {
		sb.WriteString(cb.indent)
	}
	return sb.String()
}
