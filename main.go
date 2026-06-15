package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed static
var staticFS embed.FS

const (
	ringSize          = 1024
	heartbeatInterval = 25 * time.Second
	clientBuffer      = 64
	maxIngestBody     = 5 << 20
	readTimeout       = 10 * time.Second
	shutdownGrace     = 10 * time.Second
)

type Event struct {
	ID        uint64
	Type      string
	Registry  string
	Ecosystem string
	Data      json.RawMessage
}

type Filter struct {
	Event     string
	Registry  string
	Ecosystem string
}

func (f Filter) Match(e Event) bool {
	if f.Event != "" && f.Event != e.Type {
		return false
	}
	if f.Registry != "" && f.Registry != e.Registry {
		return false
	}
	if f.Ecosystem != "" && f.Ecosystem != e.Ecosystem {
		return false
	}
	return true
}

type subscriber struct {
	ch     chan Event
	filter Filter
}

type Broker struct {
	mu     sync.RWMutex
	subs   map[*subscriber]struct{}
	ring   []Event
	head   int
	nextID uint64
}

func NewBroker() *Broker {
	return &Broker{
		subs:   make(map[*subscriber]struct{}),
		ring:   make([]Event, ringSize),
		nextID: 1,
	}
}

func (b *Broker) Publish(typ, registry, ecosystem string, data json.RawMessage) Event {
	b.mu.Lock()
	ev := Event{
		ID:        b.nextID,
		Type:      typ,
		Registry:  registry,
		Ecosystem: ecosystem,
		Data:      data,
	}
	b.nextID++
	b.ring[b.head] = ev
	b.head = (b.head + 1) % len(b.ring)

	subs := make([]*subscriber, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()

	for _, s := range subs {
		if !s.filter.Match(ev) {
			continue
		}
		select {
		case s.ch <- ev:
		default:
		}
	}
	return ev
}

func (b *Broker) Subscribe(f Filter) (*subscriber, func()) {
	s := &subscriber{ch: make(chan Event, clientBuffer), filter: f}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s, func() {
		b.mu.Lock()
		delete(b.subs, s)
		b.mu.Unlock()
		close(s.ch)
	}
}

func (b *Broker) Since(id uint64, f Filter) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Event, 0)
	for i := 0; i < len(b.ring); i++ {
		ev := b.ring[(b.head+i)%len(b.ring)]
		if ev.ID == 0 || ev.ID <= id {
			continue
		}
		if f.Match(ev) {
			out = append(out, ev)
		}
	}
	return out
}

func (b *Broker) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

type Server struct {
	broker    *Broker
	token     string
	indexHTML []byte
}

func NewServer(token string) *Server {
	return &Server{broker: NewBroker(), token: token, indexHTML: renderIndex()}
}

func assetDigest(path string) string {
	b, err := staticFS.ReadFile("static/" + path)
	if err != nil {
		return "0"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:4])
}

func renderIndex() []byte {
	b, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		log.Fatalf("read index.html: %v", err)
	}
	html := string(b)
	for _, p := range []string{"css/application.css", "app.css", "app.js"} {
		html = strings.Replace(html, "/static/"+p, "/static/"+p+"?v="+assetDigest(p), 1)
	}
	return []byte(html)
}

type ingestPayload struct {
	Events []json.RawMessage `json:"events"`
}

type eventHeader struct {
	Event     string `json:"event"`
	Registry  string `json:"registry"`
	Ecosystem string `json:"ecosystem"`
}

func (s *Server) authorized(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	want := "Bearer " + s.token
	if len(auth) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(auth), []byte(want)) == 1
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxIngestBody)
	var payload ingestPayload
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	for _, raw := range payload.Events {
		var hdr eventHeader
		_ = json.Unmarshal(raw, &hdr)
		s.broker.Publish(hdr.Event, hdr.Registry, hdr.Ecosystem, raw)
	}
	w.WriteHeader(http.StatusAccepted)
}

func writeEvent(w io.Writer, ev Event) {
	_, _ = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.ID, ev.Data)
}

func parseFilter(r *http.Request) Filter {
	q := r.URL.Query()
	return Filter{
		Event:     q.Get("event"),
		Registry:  q.Get("registry"),
		Ecosystem: q.Get("ecosystem"),
	}
}

func parseLastID(r *http.Request) uint64 {
	if h := r.Header.Get("Last-Event-ID"); h != "" {
		if id, err := strconv.ParseUint(strings.TrimSpace(h), 10, 64); err == nil {
			return id
		}
	}
	if q := r.URL.Query().Get("last_id"); q != "" {
		if id, err := strconv.ParseUint(q, 10, 64); err == nil {
			return id
		}
	}
	return 0
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	filter := parseFilter(r)
	lastID := parseLastID(r)

	sub, unsub := s.broker.Subscribe(filter)
	defer unsub()

	_, _ = fmt.Fprint(w, "retry: 3000\n\n")
	for _, ev := range s.broker.Since(lastID, filter) {
		writeEvent(w, ev)
	}
	flusher.Flush()

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case ev := <-sub.ch:
			writeEvent(w, ev)
			flusher.Flush()
		}
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"service":     "live.ecosyste.ms",
		"subscribers": s.broker.SubscriberCount(),
		"events_seen": s.broker.nextID - 1,
		"endpoints": map[string]string{
			"ingest": "POST /ingest",
			"events": "GET /events?event=&registry=&ecosystem=",
		},
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(s.indexHTML)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/ingest", s.handleIngest)
	mux.HandleFunc("/events", s.handleEvents)
	return mux
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	srv := NewServer(os.Getenv("LIVE_WEBHOOK_TOKEN"))
	addr := ":" + env("PORT", "3000")

	httpServer := &http.Server{
		Addr:        addr,
		Handler:     srv.Handler(),
		ReadTimeout: readTimeout,
	}

	go func() {
		log.Printf("live: listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("live: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}
