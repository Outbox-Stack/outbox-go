# outbox-go

Go client for [Outbox Stack](https://outboxstack.app) email. Go 1.21+, no dependencies.

```sh
go get github.com/Outbox-Stack/outbox-go
```

Create a **Sending only** API key under Developers in the dashboard and set it as `OUTBOX_API_KEY`. For tests and staging, create a **Test** key instead: it checks every message but delivers nothing.

## Send

```go
import outbox "github.com/Outbox-Stack/outbox-go"

client, err := outbox.New(outbox.Options{}) // reads OUTBOX_API_KEY; base URL defaults to https://outboxstack.app

res, err := client.Send(ctx, outbox.Message{
	From:        "billing@your-verified-domain.com",
	FromName:    "Your app",
	To:          []string{"Ada Obi <ada@example.com>"},
	ReplyTo:     "support@your-verified-domain.com",
	Subject:     "Your invoice",
	HTML:        "<p>Invoice attached.</p>",
	Attachments: []outbox.Attachment{{Filename: "invoice.pdf", Content: pdfBytes}},
	Metadata:    map[string]string{"invoice_id": "inv_123"},
}, &outbox.SendOptions{IdempotencyKey: "invoice:" + invoice.ID})

res.ID         // the first recipient's message id
res.Recipients // one entry per recipient when there's more than one
```

`Message` mirrors the [API](https://outboxstack.app/openapi.json): `To`, `Cc`, `Bcc` (50 recipients in total), `FromName`, `ToName`, `ReplyTo`, `Subject`, `HTML`, `Text`, `Template` with `Data`, `Headers` (20), `Metadata` (10) and `Attachments` (10 files, 10 MB, raw bytes).

`Send` returns once the message is queued, not delivered. Every send carries an idempotency key, generated when you don't pass one, so an automatic retry never sends twice.

Also: `SendBatch` (1–100 messages, each may carry `IdempotencyKey`) and `GetMessage`.

## Find messages

```go
it := client.IterateMessages(ctx, outbox.ListMessagesOptions{Status: "bounced", To: "@example.com"})
for it.Next() {
	m := it.Message()
	fmt.Println(m.ID, m.ToEmail, m.CreatedAt)
}
if err := it.Err(); err != nil { ... }
```

To page by hand, call `ListMessagesPage` and pass `Next` back as `After` until it's empty.

## Cancel, suppress

```go
_, err := client.CancelMessage(ctx, id)
var apiErr *outbox.APIError
if errors.As(err, &apiErr) && apiErr.Code == "not_cancellable" {
	// it already started sending
}

client.AddSuppression(ctx, "ada@example.com")    // needs a full-access key
client.RemoveSuppression(ctx, "ada@example.com") // manual suppressions only
```

## Errors

| Error | When |
| --- | --- |
| `*outbox.APIError` | The API refused the request. Branch on `Code` (`domain_not_verified`, `rate_limited`, `quota_exceeded`, …); quote `RequestID` to support. |
| `*outbox.ConnectionError` | No answer after every retry (`Code` is `timeout` or `connection_error`). The request may have arrived: retry with `IdempotencyKey`. |

Timeouts, connection errors, 408, 429 and 5xx are retried twice with backoff, honouring `Retry-After` (`Options.MaxRetries`, `Options.NoRetries`, `Options.Timeout` per attempt). Every call takes a `context.Context`.

## Webhooks

```go
http.HandleFunc("/hooks/outbox", func(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body) // the raw body, not re-encoded JSON
	event, err := outbox.VerifyWebhook(body, r.Header, os.Getenv("OUTBOX_WEBHOOK_SECRET"), nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if event.Type == "message.bounced" { // a hard bounce: final
		data, _ := event.MessageData()
		markUndeliverable(data.To)
	}
	// message.deferred is a soft bounce or delay: not final, so don't give up on the address.
	w.WriteHeader(http.StatusOK)
})
```

Events can arrive more than once: deduplicate on the `webhook-id` header. See [the event reference](https://outboxstack.app/docs/#events).

## Licence

MIT
