package fb

import (
	"errors"
	"log/slog"
	"sync"
	"time"
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

// call while holding mu.Lock()
func removeClientLink(link *Link) {
	linksPerClient[link.client]--
	if linksPerClient[link.client] == 0 {
		delete(linksPerClient, link.client)
	}
	delete(links, link.id)
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
	removeClientLink(link)
	return nil
}

func StartLinkGarbageCollector(gcInterval time.Duration) {
	ticker := time.NewTicker(gcInterval)
	go func() {
		for range ticker.C {
			mu.Lock()
			for _, link := range links {
				if link.IsExpired() {
					slog.Info("Removing expired link", "linkId", link.id, "client", link.client)
					removeClientLink(link)
				}
			}
			mu.Unlock()
		}
	}()
}
