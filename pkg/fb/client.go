package fb

import (
	"errors"
	"sync"
)

var (
	mu             sync.Mutex
	links          = make(map[string]*Link)
	linksPerClient = make(map[string]int)

	ErrTooManyLinksForClient = errors.New("too many links for client")
	ErrBadLink               = errors.New("link is nil")
	ErrLinkNotFound          = errors.New("link not found")
)

func AddClientLink(link *Link, maxLinksPerClient int) error {
	mu.Lock()
	defer mu.Unlock()
	if link == nil {
		return ErrBadLink
	}
	if linksPerClient[link.client] >= maxLinksPerClient {
		return ErrTooManyLinksForClient
	}
	linksPerClient[link.client]++
	links[link.id] = link
	return nil
}

func GetClientLink(id string) (*Link, error) {
	mu.Lock()
	link, exists := links[id]
	mu.Unlock()
	if !exists {
		return nil, ErrLinkNotFound
	}
	return link, nil
}

func RemoveClientLink(link *Link) error {
	mu.Lock()
	defer mu.Unlock()
	if link == nil {
		return ErrBadLink
	}
	if _, exists := linksPerClient[link.client]; !exists {
		return nil
	}
	linksPerClient[link.client]--
	if linksPerClient[link.client] == 0 {
		delete(linksPerClient, link.client)
	}
	delete(links, link.id)
	return nil
}
