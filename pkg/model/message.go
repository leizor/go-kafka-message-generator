package model

type Message struct {
	MessageType      string         `json:"type"`
	Name             string         `json:"name"`
	ValidVersions    string         `json:"validVersions"`
	FlexibleVersions string         `json:"flexibleVersions"`
	Fields           []MessageField `json:"fields"`
	CommonStructs    []CommonStruct `json:"commonStructs"`
}
