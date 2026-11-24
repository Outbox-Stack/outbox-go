# Changelog

## 0.2.0

Requires the matching API release for `message.suppressed` webhooks.

- `MessageEventData.Description` carries the plain-language reason on `message.suppressed`, the new event for a message that was held back and never sent. `Reason` is a suppression reason or `known_dead_address`.
- `Suppression.Reason` can be `unverified` (Amazon's pre-send address check). `RemoveSuppression` removes `manual` and `unverified` suppressions.
- `ListMessagesOptions.Stream` filters by `transactional` or `marketing`.
- `MessageDetail` gains `CampaignID` and `Problem` (a `DeliveryProblem`); `MessageEvent` gains `Subtype` and `Diagnostic`.

## 0.1.0

First release: `Send`, `SendBatch`, `GetMessage`, message paging and iterators, `CancelMessage`, suppressions, `VerifyWebhook`, automatic retries with idempotency keys. Same behaviour as the Node.js SDK 0.4.
