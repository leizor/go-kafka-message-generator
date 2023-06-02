package model

type Message struct {
	MessageType      string         `json:"type"`
	Name             string         `json:"name"`
	ValidVersions    string         `json:"validVersions"`
	FlexibleVersions string         `json:"flexibleVersions"`
	Fields           []MessageField `json:"fields"`
	CommonStructs    []CommonStruct `json:"commonStructs"`
}

type MessageField struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Versions  string `json:"versions"`
	About     string `json:"about,omitempty"`
	Default   *any   `json:"default,omitempty"`
	Ignorable *bool  `json:"ignorable,omitempty"`
}

type CommonStruct struct {
	Name     string         `json:"name"`
	Versions string         `json:"versions"`
	Fields   []MessageField `json:"fields"`
}
