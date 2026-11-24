package outbox

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const msgID = "550e8400-e29b-41d4-a716-446655440000"

type seen struct {
	method, path, query, auth, key, agent string
	body                                  map[string]any
}

// server replays responses in order and records every request.
func server(t *testing.T, responses ...func(w http.ResponseWriter)) (*Client, *[]seen) {
	t.Helper()
	var mu sync.Mutex
	calls := []seen{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		calls = append(calls, seen{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Get("Idempotency-Key"), r.Header.Get("User-Agent"), body})
		if len(calls) > len(responses) {
			t.Fatalf("unexpected request %d", len(calls))
		}
		responses[len(calls)-1](w)
	}))
	t.Cleanup(srv.Close)
	c, err := New(Options{APIKey: "mk_test", BaseURL: strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)})
	if err != nil {
		t.Fatal(err)
	}
	c.sleep = func(context.Context, time.Duration) error { return nil }
	return c, &calls
}

func reply(status int, body string, headers ...string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

var queued = reply(202, `{"id":"`+msgID+`","status":"queued","duplicate":false}`)

func TestSendHeadersAndGeneratedKey(t *testing.T) {
	c, calls := server(t, queued)
	res, err := c.Send(context.Background(), Message{From: "a@acme.com", To: []string{"b@example.com"}, Subject: "Hi", HTML: "<p>x</p>", Attachments: []Attachment{{Filename: "a.txt", Content: []byte("hello")}}}, nil)
	if err != nil || res.ID != msgID {
		t.Fatalf("send: %v %+v", err, res)
	}
	got := (*calls)[0]
	if got.method != "POST" || got.path != "/v1/messages" || got.auth != "Bearer mk_test" || !strings.HasPrefix(got.agent, "outbox-go/") || !uuidPattern.MatchString(got.key) {
		t.Fatalf("request: %+v", got)
	}
	att := got.body["attachments"].([]any)[0].(map[string]any)
	if att["content"] != base64.StdEncoding.EncodeToString([]byte("hello")) || got.body["to"].([]any)[0] != "b@example.com" {
		t.Fatalf("body: %v", got.body)
	}
}

func TestRetriesWithSameKey(t *testing.T) {
	c, calls := server(t, reply(503, `{"error":"busy"}`, "Retry-After", "1"), reply(429, `{}`), queued)
	if _, err := c.Send(context.Background(), Message{From: "a@acme.com", To: []string{"b@example.com"}, Subject: "Hi", Text: "x"}, &SendOptions{IdempotencyKey: "order-1"}); err != nil {
		t.Fatal(err)
	}
	for _, call := range *calls {
		if call.key != "order-1" {
			t.Fatalf("retry changed the key: %+v", *calls)
		}
	}
	if len(*calls) != 3 {
		t.Fatalf("attempts: %d", len(*calls))
	}
}

func TestAPIErrorNotRetried(t *testing.T) {
	c, calls := server(t, reply(422, `{"error":"domain is not verified","code":"domain_not_verified","request_id":"req_1"}`))
	_, err := c.Send(context.Background(), Message{From: "a@acme.com", To: []string{"b@example.com"}, Subject: "Hi", Text: "x"}, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 422 || apiErr.Code != "domain_not_verified" || apiErr.RequestID != "req_1" || apiErr.IdempotencyKey == "" || len(*calls) != 1 {
		t.Fatalf("error: %#v calls=%d", err, len(*calls))
	}
}

func TestBatchRetriedOnlyWithKeys(t *testing.T) {
	c, calls := server(t, reply(503, `{}`))
	if _, err := c.SendBatch(context.Background(), []BatchMessage{{Message: Message{From: "a@acme.com", To: []string{"b@example.com"}, Subject: "Hi", Text: "x"}}}); err == nil || len(*calls) != 1 {
		t.Fatalf("batch without keys retried: %v %d", err, len(*calls))
	}
	c, calls = server(t, reply(503, `{}`), reply(202, `{"accepted":1,"failed":0,"results":[{"index":0,"id":"`+msgID+`","status":"queued"}]}`))
	res, err := c.SendBatch(context.Background(), []BatchMessage{{Message: Message{From: "a@acme.com", To: []string{"b@example.com"}, Subject: "Hi", Text: "x"}, IdempotencyKey: "k1"}})
	if err != nil || res.Accepted != 1 || len(*calls) != 2 || (*calls)[1].body["messages"].([]any)[0].(map[string]any)["idempotencyKey"] != "k1" {
		t.Fatalf("batch: %v %+v", err, res)
	}
}

func TestIterateMessagesAndSuppressions(t *testing.T) {
	c, calls := server(t, reply(200, `{"messages":[{"id":"1"},{"id":"2"}],"next":"c1"}`), reply(200, `{"messages":[{"id":"3"}],"next":null}`))
	it := c.IterateMessages(context.Background(), ListMessagesOptions{Status: "bounced", Stream: "marketing"})
	ids := []string{}
	for it.Next() {
		ids = append(ids, it.Message().ID)
	}
	if it.Err() != nil || strings.Join(ids, ",") != "1,2,3" || (*calls)[1].query != "after=c1&status=bounced&stream=marketing" {
		t.Fatalf("iterate: %v %v %+v", it.Err(), ids, *calls)
	}
	c, calls = server(t, reply(503, `{"error":"busy"}`))
	if err := c.RemoveSuppression(context.Background(), "a+b@example.com"); err == nil || len(*calls) != 1 || (*calls)[0].path != "/v1/suppressions/a+b@example.com" {
		t.Fatalf("remove suppression: %v %+v", err, *calls)
	}
}

func TestGetMessageProblem(t *testing.T) {
	c, _ := server(t, reply(200, `{"id":"`+msgID+`","status":"suppressed","campaign_id":null,"events":[{"type":"skipped","at":"2026-11-24T10:00:00Z","subtype":"EmailValidationSuppressed"}],"problem":{"code":"unverified_address","title":"Not sent","detail":"d","action":"a"}}`))
	m, err := c.GetMessage(context.Background(), msgID)
	if err != nil || m.Problem == nil || m.Problem.Code != "unverified_address" || m.Problem.Pending || len(m.Events) != 1 || m.Events[0].Subtype != "EmailValidationSuppressed" || m.CampaignID != "" {
		t.Fatalf("message detail: %v %+v", err, m)
	}
}

func TestConnectionErrorAndSettings(t *testing.T) {
	c, err := New(Options{APIKey: "k", BaseURL: "http://localhost:1", Timeout: 200 * time.Millisecond, NoRetries: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetMessage(context.Background(), msgID)
	var conn *ConnectionError
	if !errors.As(err, &conn) || conn.Code != "connection_error" {
		t.Fatalf("connection error: %#v", err)
	}
	for _, bad := range []string{"http://outboxstack.app", "https://outboxstack.app/v1", "https://u:p@outboxstack.app"} {
		if _, err := New(Options{APIKey: "k", BaseURL: bad}); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if _, err := c.GetMessage(context.Background(), "nope"); err == nil {
		t.Error("bad id accepted")
	}
}

func sign(secret, id, ts string, body []byte) string {
	key, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestVerifyWebhook(t *testing.T) {
	secret := "whsec_" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	body := []byte(`{"type":"message.deferred","created_at":"2026-10-08T10:00:00Z","data":{"to":"a@b.co","reason":"soft_bounce","soft_bounce_count":2}}`)
	h := http.Header{}
	h.Set("webhook-id", "msg_1")
	h.Set("webhook-timestamp", "1700000000")
	h.Set("webhook-signature", "v1,bad v1,"+sign(secret, "msg_1", "1700000000", body))
	event, err := VerifyWebhook(body, h, secret, &VerifyOptions{Now: time.Unix(1700000100, 0)})
	if err != nil || event.Type != "message.deferred" {
		t.Fatalf("verify: %v %+v", err, event)
	}
	data, err := event.MessageData()
	if err != nil || data.Reason != "soft_bounce" || data.SoftBounceCount != 2 {
		t.Fatalf("data: %v %+v", err, data)
	}
	if _, err := VerifyWebhook(append(body, ' '), h, secret, &VerifyOptions{Now: time.Unix(1700000000, 0)}); !errors.Is(err, ErrWebhookVerification) {
		t.Error("tampered body accepted")
	}
	if _, err := VerifyWebhook(body, h, secret, &VerifyOptions{Now: time.Unix(1700000000+301, 0)}); !errors.Is(err, ErrWebhookVerification) {
		t.Error("stale timestamp accepted")
	}
	suppressed := WebhookEvent{Type: "message.suppressed", Data: []byte(`{"to":"a@b.co","reason":"known_dead_address","description":"Not sent: this address doesn't exist."}`)}
	if data, err := suppressed.MessageData(); err != nil || data.Reason != "known_dead_address" || data.Description != "Not sent: this address doesn't exist." {
		t.Fatalf("suppressed data: %v %+v", err, data)
	}
}
