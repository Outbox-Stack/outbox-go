package outbox

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrWebhookVerification is wrapped by every VerifyWebhook failure. Answer the request with 400.
var ErrWebhookVerification = errors.New("outbox: webhook verification failed")

// WebhookEvent is a verified webhook. Decode Data with MessageData or ContactData depending on Type.
type WebhookEvent struct {
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// MessageEventData is the data of message.* events.
type MessageEventData struct {
	MessageID         string            `json:"message_id"`
	To                string            `json:"to"`
	Subject           string            `json:"subject"`
	Stream            string            `json:"stream"`
	CampaignID        string            `json:"campaign_id"`
	Metadata          map[string]string `json:"metadata"`
	ProviderMessageID string            `json:"provider_message_id"`
	Recipients        []string          `json:"recipients"`
	OccurredAt        *time.Time        `json:"occurred_at"`
	// BounceType: message.bounced is always "hard"; message.deferred is "soft", or empty for a delivery delay.
	BounceType    string `json:"bounce_type"`
	BounceSubtype string `json:"bounce_subtype"`
	// Diagnostic is the receiving server's SMTP reply, when the provider reports one.
	Diagnostic string `json:"diagnostic"`
	// Reason: message.deferred gives "soft_bounce" or "delivery_delay"; message.failed says why it won't be sent.
	Reason          string `json:"reason"`
	SoftBounceCount int    `json:"soft_bounce_count"`
	// Suppressed reports whether this event added the address to your suppressions.
	Suppressed bool `json:"suppressed"`
	// Test is set on events from test API keys.
	Test bool `json:"test"`
}

// ContactEventData is the data of contact.subscribed and contact.unsubscribed.
type ContactEventData struct {
	ContactID string `json:"contact_id"`
	Email     string `json:"email"`
	ListID    string `json:"list_id"`
}

func (e *WebhookEvent) MessageData() (*MessageEventData, error) {
	var d MessageEventData
	return &d, json.Unmarshal(e.Data, &d)
}

func (e *WebhookEvent) ContactData() (*ContactEventData, error) {
	var d ContactEventData
	return &d, json.Unmarshal(e.Data, &d)
}

// VerifyOptions: Tolerance defaults to 5 minutes; Now to time.Now.
type VerifyOptions struct {
	Tolerance time.Duration
	Now       time.Time
}

func verifyError(msg string) error { return errors.Join(ErrWebhookVerification, errors.New(msg)) }

// VerifyWebhook checks a signed webhook (Standard Webhooks) and returns the event. Pass the raw
// request body exactly as received, not re-encoded JSON.
func VerifyWebhook(payload []byte, headers http.Header, secret string, opts *VerifyOptions) (*WebhookEvent, error) {
	tolerance, now := 5*time.Minute, time.Now()
	if opts != nil {
		if opts.Tolerance > 0 {
			tolerance = opts.Tolerance
		}
		if !opts.Now.IsZero() {
			now = opts.Now
		}
	}
	id, timestamp, signatures := headers.Get("webhook-id"), headers.Get("webhook-timestamp"), headers.Get("webhook-signature")
	if id == "" || timestamp == "" || signatures == "" {
		return nil, verifyError("missing webhook-id, webhook-timestamp or webhook-signature header")
	}
	sentAt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || math.Abs(float64(now.Unix()-sentAt)) > tolerance.Seconds() {
		return nil, verifyError("webhook timestamp is outside the allowed tolerance")
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		return nil, verifyError("webhook secret is not valid base64")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + timestamp + "."))
	mac.Write(payload)
	expected := mac.Sum(nil)
	for _, candidate := range strings.Split(signatures, " ") {
		version, encoded, found := strings.Cut(candidate, ",")
		if !found || version != "v1" {
			continue
		}
		sig, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}
		if hmac.Equal(sig, expected) {
			var event WebhookEvent
			if err := json.Unmarshal(payload, &event); err != nil {
				return nil, verifyError("webhook body is not valid JSON")
			}
			return &event, nil
		}
	}
	return nil, verifyError("no matching webhook signature")
}
