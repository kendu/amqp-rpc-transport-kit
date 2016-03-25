package rpc

import (
	"encoding/json"
)

type Encoder func(*json.Encoder) error
type Decoder func(*json.Decoder) error
type Callback func(*json.Decoder, *json.Encoder) error
