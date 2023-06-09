package model

type CommonStruct struct {
	Name     string         `json:"name"`
	Versions string         `json:"versions"`
	Fields   []MessageField `json:"fields"`
}
