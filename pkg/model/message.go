package model

type Message struct {
	MessageType      string         `json:"type"`
	Name             string         `json:"name"`
	ValidVersions    string         `json:"validVersions"`
	FlexibleVersions string         `json:"flexibleVersions"`
	Fields           []MessageField `json:"fields"`
}

type MessageField struct {
	Name       string `json:"name"`
	FieldType  string `json:"fieldType"`
	Versions   string `json:"versions"`
	DefaultVal *any   `json:"default,omitempty"`
	Ignorable  *bool  `json:"ignorable,omitempty"`
}
