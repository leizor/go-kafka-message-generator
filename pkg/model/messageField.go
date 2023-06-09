package model

type MessageField struct {
	Name      string         `json:"name"`
	Type      string         `json:"type"`
	Versions  string         `json:"versions"`
	About     string         `json:"about,omitempty"`
	Default   *any           `json:"default,omitempty"`
	Ignorable *bool          `json:"ignorable,omitempty"`
	Fields    []MessageField `json:"fields"`
}
