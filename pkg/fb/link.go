package fb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.rtnl.ai/x/randstr"
)

const (
	linkIdLen     = 16
	linkIdCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	ErrSenderAlreadyConnected   = errors.New("sender already connected")
	ErrReceiverAlreadyConnected = errors.New("receiver already connected")
	ErrLinkExpired              = errors.New("link expired")
)

type Link struct {
	id                string
	client            string
	ctx               context.Context
	senderRequest     *http.Request
	senderResponse    http.ResponseWriter
	receiverRequest   *http.Request
	receiverResponse  http.ResponseWriter
	senderConnected   chan bool
	receiverConnected chan bool
	done              chan struct{}
}

func NewLink(client, linkPrefix string, timeout time.Duration) *Link {

	ctxExpired, _ := context.WithTimeoutCause(context.Background(), timeout, ErrLinkExpired)

	return &Link{
		id:                generateLinkId(linkPrefix),
		client:            client,
		ctx:               ctxExpired,
		senderConnected:   make(chan bool, 1),
		receiverConnected: make(chan bool, 1),
		done:              make(chan struct{}),
	}
}

func generateLinkId(linkPrefix string) string {
	return fmt.Sprintf("%s%s", linkPrefix, randstr.Generate(linkIdLen, linkIdCharset))
}

func (l *Link) Id() string {
	return l.id
}

func (l *Link) IsExpired() bool {
	if l.ctx.Err() != nil {
		if errors.Is(context.Cause(l.ctx), ErrLinkExpired) {
			return true
		}
	}
	return false
}

func (l *Link) SetSender(req *http.Request, res http.ResponseWriter) error {
	if l.senderRequest != nil {
		return ErrSenderAlreadyConnected
	}
	l.senderRequest = req
	l.senderResponse = res
	l.senderConnected <- true
	return nil
}

func (l *Link) SetReceiver(req *http.Request, res http.ResponseWriter) error {
	if l.receiverRequest != nil {
		return ErrReceiverAlreadyConnected
	}
	l.receiverRequest = req
	l.receiverResponse = res
	l.receiverConnected <- true
	return nil
}

func (l *Link) Copy(ctx context.Context) (int64, error) {
	var n int64
	var err error
	select {
	case <-l.receiverConnected:
		<-l.senderConnected
		n, err = io.Copy(l.receiverResponse, l.senderRequest.Body)
	case <-ctx.Done():
		n, err = 0, ctx.Err()
	case <-l.ctx.Done():
		n, err = 0, l.ctx.Err()
	}
	close(l.done)
	return n, err
}

func (l *Link) Wait(ctx context.Context) error {
	select {
	case <-l.done:
		return nil
	case <-l.ctx.Done():
		return l.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
