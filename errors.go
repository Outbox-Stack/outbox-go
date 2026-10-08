package outbox

import (
	"fmt"
	"time"
)

// APIError: the API answered with a non-2xx status, or (Code "invalid_response") a 2xx body that
// couldn't be read. On a send, "invalid_response" usually means the message was accepted: retry
// with IdempotencyKey to find out. Branch on Code; new codes may appear.
type APIError struct {
	Status  int
	Code    string
	Message string
	// RequestID: quote this to support.
	RequestID string
	Body      []byte
	// RetryAfter is the server's hint, when it sent one.
	RetryAfter time.Duration
	// IdempotencyKey the failed request carried. Retry with it and the API won't send twice.
	IdempotencyKey string
}

func (e *APIError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("%s (HTTP %d, %s, request %s)", e.Message, e.Status, e.Code, e.RequestID)
	}
	return fmt.Sprintf("%s (HTTP %d, %s)", e.Message, e.Status, e.Code)
}

// ConnectionError: no answer from the API after every attempt. The request may or may not have
// reached it. Code is "timeout" or "connection_error".
type ConnectionError struct {
	Code           string
	Message        string
	IdempotencyKey string
	Err            error
}

func (e *ConnectionError) Error() string { return e.Message + ": " + e.Err.Error() }
func (e *ConnectionError) Unwrap() error { return e.Err }
