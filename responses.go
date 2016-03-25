package rpc

type ErrorResponse struct {
	Message string `json:"msg"`
	Code    string `json:"code"`
}
