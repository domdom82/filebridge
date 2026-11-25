package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/domdom82/filebridge/pkg/fb"
)

var linkTimeout = 5 * time.Minute // TODO: make this configurable
var maxLinksPerClient = 10        // TODO: make this configurable

// shardPrefix will later be used to identify the shard of the link server. Can be zoned by a load balancer
// sharding allows for multiple link servers without having to share state
var shardPrefix = "a" //TODO: make this configurable

var externalHost = "localhost:8080" //TODO: make this configurable
var scheme = "http"                 //TODO: make this configurable

func main() {

	mux := http.NewServeMux()
	mux.HandleFunc("/link", genLink)
	mux.HandleFunc("/link/", genLink)
	mux.HandleFunc("/link/{linkId}", handleLink)

	http.ListenAndServe(":8080", mux)
}

func genLink(w http.ResponseWriter, r *http.Request) {
	client, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		slog.Error("could not get client IP", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	link := fb.NewLink(client, shardPrefix, linkTimeout)
	if err = fb.AddClientLink(link, maxLinksPerClient); err != nil {
		if errors.Is(err, fb.ErrTooManyLinksForClient) {
			slog.Info("Too many links for client", "client", client, "maxLinksPerClient", maxLinksPerClient)
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		slog.Error("Error during link generation", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	externalLink := fmt.Sprintf("%s://%s/link/%s", scheme, externalHost, link.Id())

	//TODO: this should be a proper HTML page with instructions and curl commands for sender and receiver
	sb := strings.Builder{}
	sb.WriteString("Link is valid for " + linkTimeout.String() + "\n\n")
	sb.WriteString("Sender:\n\t")
	sb.WriteString("curl -T <file> " + externalLink + "\n")
	sb.WriteString("Receiver:\n\t")
	sb.WriteString("curl -o <file> " + externalLink + "\n")

	w.Write([]byte(sb.String()))
}

func handleLink(w http.ResponseWriter, r *http.Request) {
	linkId := r.PathValue("linkId")
	link, err := fb.GetClientLink(linkId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		if err := link.SetSender(r, w); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		slog.Info("Sender connected", "linkId", linkId, "client", r.RemoteAddr)
	} else if r.Method == http.MethodGet {
		if err := link.SetReceiver(r, w); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		slog.Info("Receiver connected", "linkId", linkId, "client", r.RemoteAddr)
	} else {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if r.Method == http.MethodGet {
		slog.Info("Forwarding data from sender to receiver", "linkId", linkId)
		n, err := link.Copy(r.Context())
		if err != nil {
			slog.Error("Error forwarding data from sender to receiver", "linkId", linkId, "error", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
		} else {
			slog.Info("Forwarded data from sender to receiver", "linkId", linkId, "bytes", n)
		}
	}
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		if err := link.Wait(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
		} else {
			w.Write([]byte("OK\n"))
		}
	}

	err = fb.RemoveClientLink(link)
	if err != nil {
		slog.Error("Error during link removal", "err", err)
	}
}
