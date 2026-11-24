// Package outbox is the Outbox Stack email SDK for Go: send email, read message history, manage
// suppressions and verify webhooks. It has no dependencies outside the standard library.
//
//	client, err := outbox.New(outbox.Options{}) // reads OUTBOX_API_KEY
//	res, err := client.Send(ctx, outbox.Message{
//		From:    "hello@yourcompany.com",
//		To:      []string{"ada@example.com"},
//		Subject: "Your receipt",
//		HTML:    "<p>Thanks for your order.</p>",
//	}, nil)
//
// Requests that can't cause a duplicate send are retried with backoff. Every send carries an
// idempotency key, generated when you don't pass one, so a retry never sends twice.
package outbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Version is the SDK version sent in the User-Agent header.
const Version = "0.2.0"

const defaultBaseURL = "https://outboxstack.app"

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Options configure a Client. The zero value works when OUTBOX_API_KEY is set.
type Options struct {
	// APIKey defaults to the OUTBOX_API_KEY environment variable.
	APIKey string
	// BaseURL is the platform origin. Defaults to OUTBOX_BASE_URL, then https://outboxstack.app.
	BaseURL string
	// Timeout is per attempt. Default 10s.
	Timeout time.Duration
	// MaxRetries after timeouts, network errors, 408, 429 and 5xx. Default 2; set NoRetries to disable.
	MaxRetries int
	NoRetries  bool
	// HTTPClient is used for requests. Redirects are never followed.
	HTTPClient *http.Client
}

// Client talks to the Outbox Stack API. It is safe for concurrent use.
type Client struct {
	apiKey     string
	baseURL    string
	timeout    time.Duration
	maxRetries int
	http       *http.Client
	sleep      func(context.Context, time.Duration) error
}

// New returns a client, or an error when the settings can't work.
func New(o Options) (*Client, error) {
	if o.APIKey == "" {
		o.APIKey = os.Getenv("OUTBOX_API_KEY")
	}
	if o.APIKey == "" {
		return nil, errors.New("outbox: an API key is required: set Options.APIKey or OUTBOX_API_KEY")
	}
	if o.BaseURL == "" {
		o.BaseURL = os.Getenv("OUTBOX_BASE_URL")
	}
	if o.BaseURL == "" {
		o.BaseURL = defaultBaseURL
	}
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("outbox: BaseURL must be the platform origin")
	}
	host := u.Hostname()
	if u.Scheme != "https" && !(u.Scheme == "http" && (host == "localhost" || host == "127.0.0.1" || host == "::1")) {
		return nil, errors.New("outbox: use HTTPS outside localhost")
	}
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}
	if o.Timeout < 0 {
		return nil, errors.New("outbox: Timeout must be positive")
	}
	retries := 2
	if o.NoRetries {
		retries = 0
	} else if o.MaxRetries != 0 {
		retries = o.MaxRetries
	}
	if retries < 0 || retries > 10 {
		return nil, errors.New("outbox: MaxRetries must be from 0 to 10")
	}
	hc := &http.Client{}
	if o.HTTPClient != nil {
		copied := *o.HTTPClient
		hc = &copied
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{apiKey: o.APIKey, baseURL: u.Scheme + "://" + u.Host, timeout: o.Timeout, maxRetries: retries, http: hc, sleep: sleepContext}, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func checkKey(key string) error {
	if key != "" && (strings.TrimSpace(key) == "" || len(key) > 200) {
		return errors.New("outbox: idempotency key must contain 1-200 characters")
	}
	return nil
}

// SendOptions apply to one Send.
type SendOptions struct {
	// IdempotencyKey: the same key gives the same message, even across retries and restarts. Generated when empty.
	IdempotencyKey string
}

// Send accepts a message into the queue. Accepted is not delivered: track it with GetMessage or webhooks.
func (c *Client) Send(ctx context.Context, m Message, opts *SendOptions) (*SendResult, error) {
	key := ""
	if opts != nil {
		key = opts.IdempotencyKey
	}
	if err := checkKey(key); err != nil {
		return nil, err
	}
	if key == "" {
		key = newUUID()
	}
	var out SendResult
	return &out, c.do(ctx, http.MethodPost, "/v1/messages", m, key, true, &out)
}

// SendBatch sends up to 100 messages in one request. Items fail independently; check each result.
// It's retried only when every item carries an IdempotencyKey.
func (c *Client) SendBatch(ctx context.Context, messages []BatchMessage) (*BatchResult, error) {
	if len(messages) < 1 || len(messages) > 100 {
		return nil, errors.New("outbox: SendBatch takes 1 to 100 messages")
	}
	retryable := true
	for _, m := range messages {
		if err := checkKey(m.IdempotencyKey); err != nil {
			return nil, err
		}
		retryable = retryable && m.IdempotencyKey != ""
	}
	var out BatchResult
	return &out, c.do(ctx, http.MethodPost, "/v1/messages/batch", map[string]any{"messages": messages}, "", retryable, &out)
}

// GetMessage returns one message with its delivery history.
func (c *Client) GetMessage(ctx context.Context, id string) (*MessageDetail, error) {
	if !uuidPattern.MatchString(id) {
		return nil, errors.New("outbox: a message UUID is required")
	}
	var out MessageDetail
	return &out, c.do(ctx, http.MethodGet, "/v1/messages/"+id, nil, "", true, &out)
}

// ListMessagesOptions filter and page through messages.
type ListMessagesOptions struct {
	Status string
	// Stream is "transactional" or "marketing".
	Stream string
	// To matches recipients whose address contains this text (case-insensitive).
	To string
	// Limit is 1-200. Default 50.
	Limit int
	// After is the Next value from a previous page.
	After string
}

// ListMessagesPage returns one page, most recent first. Pass Next back as After until it's empty.
func (c *Client) ListMessagesPage(ctx context.Context, o ListMessagesOptions) (*MessagePage, error) {
	q := url.Values{}
	set(q, "status", o.Status)
	set(q, "stream", o.Stream)
	set(q, "to", o.To)
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	set(q, "after", o.After)
	var out MessagePage
	return &out, c.do(ctx, http.MethodGet, "/v1/messages"+encode(q), nil, "", true, &out)
}

// IterateMessages walks every matching message, most recent first, a page at a time:
//
//	it := client.IterateMessages(ctx, outbox.ListMessagesOptions{Status: "bounced"})
//	for it.Next() { m := it.Message() }
//	if err := it.Err(); err != nil { ... }
func (c *Client) IterateMessages(ctx context.Context, o ListMessagesOptions) *MessageIterator {
	return &MessageIterator{c: c, ctx: ctx, opts: o}
}

// CancelMessage stops a message that hasn't started sending. Once it has, the error is an *APIError
// with Code "not_cancellable". Safe to retry.
func (c *Client) CancelMessage(ctx context.Context, id string) (*CancelResult, error) {
	if !uuidPattern.MatchString(id) {
		return nil, errors.New("outbox: a message UUID is required")
	}
	var out CancelResult
	return &out, c.do(ctx, http.MethodPost, "/v1/messages/"+id+"/cancel", nil, "", true, &out)
}

// ListSuppressionsOptions page through suppressed addresses.
type ListSuppressionsOptions struct {
	// Limit is 1-1000. Default 1000.
	Limit int
	After string
}

// ListSuppressionsPage returns one page, newest first. Suppression calls need a full-access API key.
func (c *Client) ListSuppressionsPage(ctx context.Context, o ListSuppressionsOptions) (*SuppressionPage, error) {
	q := url.Values{}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	set(q, "after", o.After)
	var out SuppressionPage
	return &out, c.do(ctx, http.MethodGet, "/v1/suppressions"+encode(q), nil, "", true, &out)
}

// IterateSuppressions walks every suppressed address, newest first.
func (c *Client) IterateSuppressions(ctx context.Context, o ListSuppressionsOptions) *SuppressionIterator {
	return &SuppressionIterator{c: c, ctx: ctx, opts: o}
}

func checkEmail(email string) error {
	if !strings.Contains(email, "@") || len(email) > 254 {
		return errors.New("outbox: a valid email address is required")
	}
	return nil
}

// AddSuppression stops all mail to an address. Adding one that's already suppressed succeeds and keeps its reason.
func (c *Client) AddSuppression(ctx context.Context, email string) error {
	if err := checkEmail(email); err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/v1/suppressions", map[string]string{"email": email}, "", true, nil)
}

// RemoveSuppression removes a "manual" or "unverified" suppression; removing an "unverified" one also
// skips Amazon's address check for that address from then on. Bounces, complaints and unsubscribes
// can't be removed (Code "conflict"). Not retried automatically: a retry after a lost response would report
// "not_found" for an address that was removed.
func (c *Client) RemoveSuppression(ctx context.Context, email string) error {
	if err := checkEmail(email); err != nil {
		return err
	}
	return c.do(ctx, http.MethodDelete, "/v1/suppressions/"+url.PathEscape(email), nil, "", false, nil)
}

func set(q url.Values, k, v string) {
	if v != "" {
		q.Set(k, v)
	}
}

func encode(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	return "?" + q.Encode()
}

func (c *Client) do(ctx context.Context, method, path string, body any, idempotencyKey string, retryable bool, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("outbox: encode request: %w", err)
		}
	}
	for attempt := 0; ; attempt++ {
		canRetry := retryable && attempt < c.maxRetries
		status, header, raw, err := c.attempt(ctx, method, path, payload, idempotencyKey)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !canRetry {
				code, msg := "connection_error", "outbox: could not reach Outbox Stack"
				var timeout interface{ Timeout() bool }
				if errors.As(err, &timeout) && timeout.Timeout() {
					code, msg = "timeout", fmt.Sprintf("outbox: no response within %s", c.timeout)
				}
				return &ConnectionError{Code: code, Message: msg, IdempotencyKey: idempotencyKey, Err: err}
			}
			if err := c.sleep(ctx, delay(attempt, -1)); err != nil {
				return err
			}
			continue
		}
		retryAfter := parseRetryAfter(header.Get("Retry-After"), time.Now())
		ok := status >= 200 && status < 300
		var parsed map[string]any
		decodable := json.Unmarshal(raw, &parsed) == nil && parsed != nil
		if ok && decodable {
			if out == nil {
				return nil
			}
			if err := json.Unmarshal(raw, out); err != nil {
				return &APIError{Status: status, Code: "invalid_response", Message: "outbox: unexpected response body", IdempotencyKey: idempotencyKey, RequestID: header.Get("X-Request-Id")}
			}
			return nil
		}
		if canRetry && (ok || status == 408 || status == 429 || status >= 500) {
			if err := c.sleep(ctx, delay(attempt, retryAfter)); err != nil {
				return err
			}
			continue
		}
		e := &APIError{Status: status, Code: "error", RequestID: header.Get("X-Request-Id"), Body: raw, IdempotencyKey: idempotencyKey}
		if retryAfter >= 0 {
			e.RetryAfter = retryAfter
		}
		if ok {
			e.Code, e.Message = "invalid_response", fmt.Sprintf("Outbox API returned HTTP %d with an unreadable body", status)
			return e
		}
		e.Message = fmt.Sprintf("Outbox API returned HTTP %d", status)
		if decodable {
			if s, _ := parsed["error"].(string); s != "" {
				e.Message = s
			}
			if s, _ := parsed["code"].(string); s != "" {
				e.Code = s
			}
			if s, _ := parsed["request_id"].(string); s != "" {
				e.RequestID = s
			}
		}
		return e
	}
}

func (c *Client) attempt(ctx context.Context, method, path string, payload []byte, idempotencyKey string) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "outbox-go/"+Version)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 10<<20))
	if err != nil {
		return 0, nil, nil, err
	}
	return res.StatusCode, res.Header, raw, nil
}

// delay is Retry-After when the server gives one (capped at 30s), else exponential backoff with full jitter.
func delay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter >= 0 {
		return min(retryAfter, 30*time.Second)
	}
	return time.Duration(mrand.Float64() * float64(min(time.Duration(float64(500*time.Millisecond)*math.Pow(2, float64(attempt))), 8*time.Second)))
}

// parseRetryAfter reads delay-seconds or an HTTP date; -1 when absent or unreadable.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return -1
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0)
	}
	return -1
}
