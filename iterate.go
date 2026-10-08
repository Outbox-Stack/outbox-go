package outbox

import "context"

// MessageIterator walks message pages. Call Next until it returns false, then check Err.
type MessageIterator struct {
	c    *Client
	ctx  context.Context
	opts ListMessagesOptions
	page []MessageSummary
	cur  MessageSummary
	done bool
	err  error
}

func (it *MessageIterator) Next() bool {
	for len(it.page) == 0 {
		if it.done || it.err != nil {
			return false
		}
		p, err := it.c.ListMessagesPage(it.ctx, it.opts)
		if err != nil {
			it.err = err
			return false
		}
		it.page, it.opts.After, it.done = p.Messages, p.Next, p.Next == ""
	}
	it.cur, it.page = it.page[0], it.page[1:]
	return true
}

func (it *MessageIterator) Message() MessageSummary { return it.cur }
func (it *MessageIterator) Err() error              { return it.err }

// SuppressionIterator walks suppression pages. Call Next until it returns false, then check Err.
type SuppressionIterator struct {
	c    *Client
	ctx  context.Context
	opts ListSuppressionsOptions
	page []Suppression
	cur  Suppression
	done bool
	err  error
}

func (it *SuppressionIterator) Next() bool {
	for len(it.page) == 0 {
		if it.done || it.err != nil {
			return false
		}
		p, err := it.c.ListSuppressionsPage(it.ctx, it.opts)
		if err != nil {
			it.err = err
			return false
		}
		it.page, it.opts.After, it.done = p.Suppressions, p.Next, p.Next == ""
	}
	it.cur, it.page = it.page[0], it.page[1:]
	return true
}

func (it *SuppressionIterator) Suppression() Suppression { return it.cur }
func (it *SuppressionIterator) Err() error               { return it.err }
