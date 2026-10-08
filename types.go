package outbox

import (
	"encoding/json"
	"time"
)

// Message is one email. Set Subject with HTML and/or Text, or Template with Data. Every address in
// To, Cc and Bcc becomes its own message with its own id; at most 50 in total. Addresses are
// "ada@example.com" or "Ada Obi <ada@example.com>".
type Message struct {
	From     string   `json:"from"`
	FromName string   `json:"fromName,omitempty"`
	To       []string `json:"to"`
	// ToName is the display name when To is a single bare address.
	ToName string   `json:"toName,omitempty"`
	Cc     []string `json:"cc,omitempty"`
	// Bcc is never shown in any recipient's headers.
	Bcc     []string `json:"bcc,omitempty"`
	ReplyTo string   `json:"replyTo,omitempty"`
	Subject string   `json:"subject,omitempty"`
	HTML    string   `json:"html,omitempty"`
	// Text is generated from HTML when empty.
	Text string `json:"text,omitempty"`
	// Template is a stored template's id or slug; it supplies the subject and body.
	Template string         `json:"template,omitempty"`
	Data     map[string]any `json:"data,omitempty"`
	// Headers: up to 20 custom headers. Routing, authentication and provider-control headers are refused.
	Headers map[string]string `json:"headers,omitempty"`
	// Attachments: up to 10 files, 10 MB in total.
	Attachments []Attachment `json:"attachments,omitempty"`
	// Metadata: up to 10 values of your own, returned by GetMessage and in webhooks.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Attachment is a file. Content is the raw bytes; the SDK base64-encodes it.
type Attachment struct {
	Filename string `json:"filename"`
	Content  []byte `json:"content"`
	// ContentType is inferred from the filename when empty.
	ContentType string `json:"contentType,omitempty"`
	// ContentID makes the file inline; reference it in HTML as cid:<ContentID>.
	ContentID string `json:"contentId,omitempty"`
}

// BatchMessage is one item of SendBatch.
type BatchMessage struct {
	Message
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

// RecipientResult is one recipient's message in a multi-recipient send.
type RecipientResult struct {
	Email     string `json:"email"`
	ID        string `json:"id"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate"`
}

// SendResult: ID is the first recipient's message id. Duplicate is true when the idempotency key
// matched an earlier send. Recipients has one entry per recipient when there was more than one.
type SendResult struct {
	ID         string            `json:"id"`
	Status     string            `json:"status"`
	Duplicate  bool              `json:"duplicate"`
	Recipients []RecipientResult `json:"recipients,omitempty"`
}

// BatchItemResult is either accepted (ID set) or refused (Error and Code set).
type BatchItemResult struct {
	Index      int               `json:"index"`
	ID         string            `json:"id,omitempty"`
	Status     string            `json:"status,omitempty"`
	Duplicate  bool              `json:"duplicate,omitempty"`
	Recipients []RecipientResult `json:"recipients,omitempty"`
	Error      string            `json:"error,omitempty"`
	Code       string            `json:"code,omitempty"`
}

type BatchResult struct {
	Accepted int               `json:"accepted"`
	Failed   int               `json:"failed"`
	Results  []BatchItemResult `json:"results"`
}

type MessageSummary struct {
	ID        string    `json:"id"`
	Stream    string    `json:"stream"`
	ToEmail   string    `json:"to_email"`
	Subject   string    `json:"subject"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// MessageEvent is one step in a message's delivery history.
type MessageEvent struct {
	Type string    `json:"type"`
	At   time.Time `json:"at"`
}

type MessageDetail struct {
	MessageSummary
	FromEmail         string            `json:"from_email"`
	FromName          string            `json:"from_name"`
	ToName            string            `json:"to_name"`
	ReplyTo           string            `json:"reply_to"`
	HTML              string            `json:"html"`
	Text              string            `json:"text"`
	Attempts          int               `json:"attempts"`
	LastError         string            `json:"last_error"`
	IdempotencyKey    string            `json:"idempotency_key"`
	ProviderMessageID string            `json:"provider_message_id"`
	TemplateID        string            `json:"template_id"`
	Extras            json.RawMessage   `json:"extras"`
	Metadata          map[string]string `json:"metadata"`
	Events            []MessageEvent    `json:"events"`
	UpdatedAt         time.Time         `json:"updated_at"`
	SentAt            *time.Time        `json:"sent_at"`
	// Test is true for messages sent with a test API key.
	Test bool `json:"test"`
}

// MessagePage: pass Next back as After for the next page; empty on the last page.
type MessagePage struct {
	Messages []MessageSummary `json:"messages"`
	Next     string           `json:"next"`
}

type CancelResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type Suppression struct {
	Email     string    `json:"email"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}

type SuppressionPage struct {
	Suppressions []Suppression `json:"suppressions"`
	Next         string        `json:"next"`
}
