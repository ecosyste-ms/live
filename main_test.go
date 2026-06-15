package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFilterMatch(t *testing.T) {
	ev := Event{Type: "version.created", Registry: "npmjs.org", Ecosystem: "npm"}

	cases := []struct {
		name string
		f    Filter
		want bool
	}{
		{"empty matches all", Filter{}, true},
		{"event match", Filter{Event: "version.created"}, true},
		{"event miss", Filter{Event: "package.created"}, false},
		{"registry match", Filter{Registry: "npmjs.org"}, true},
		{"registry miss", Filter{Registry: "rubygems.org"}, false},
		{"ecosystem match", Filter{Ecosystem: "npm"}, true},
		{"all match", Filter{Event: "version.created", Registry: "npmjs.org", Ecosystem: "npm"}, true},
		{"partial miss", Filter{Event: "version.created", Registry: "rubygems.org"}, false},
	}
	for _, tc := range cases {
		if got := tc.f.Match(ev); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestBrokerPublishSubscribe(t *testing.T) {
	b := NewBroker()
	sub, unsub := b.Subscribe(Filter{})
	defer unsub()

	ev := b.Publish("version.created", "npmjs.org", "npm", json.RawMessage(`{"name":"foo"}`))
	if ev.ID != 1 {
		t.Fatalf("expected ID 1, got %d", ev.ID)
	}

	select {
	case got := <-sub.ch:
		if got.ID != 1 || got.Type != "version.created" {
			t.Fatalf("unexpected event %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestBrokerFilterDelivery(t *testing.T) {
	b := NewBroker()
	sub, unsub := b.Subscribe(Filter{Registry: "rubygems.org"})
	defer unsub()

	b.Publish("version.created", "npmjs.org", "npm", json.RawMessage(`{}`))
	b.Publish("version.created", "rubygems.org", "rubygems", json.RawMessage(`{}`))

	select {
	case got := <-sub.ch:
		if got.Registry != "rubygems.org" {
			t.Fatalf("expected rubygems.org, got %s", got.Registry)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case got := <-sub.ch:
		t.Fatalf("unexpected second event %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBrokerSince(t *testing.T) {
	b := NewBroker()
	for i := 0; i < 5; i++ {
		b.Publish("e", "", "", json.RawMessage(`{}`))
	}

	got := b.Since(2, Filter{})
	if len(got) != 3 {
		t.Fatalf("expected 3 events after id 2, got %d", len(got))
	}
	if got[0].ID != 3 || got[2].ID != 5 {
		t.Fatalf("expected ids 3..5, got %d..%d", got[0].ID, got[len(got)-1].ID)
	}
}

func TestBrokerSinceWraps(t *testing.T) {
	b := NewBroker()
	for i := 0; i < ringSize+10; i++ {
		b.Publish("e", "", "", json.RawMessage(`{}`))
	}
	got := b.Since(0, Filter{})
	if len(got) != ringSize {
		t.Fatalf("expected %d events, got %d", ringSize, len(got))
	}
	if got[0].ID != 11 {
		t.Fatalf("expected oldest id 11 after wrap, got %d", got[0].ID)
	}
}

func TestBrokerDropsOnFullBuffer(t *testing.T) {
	b := NewBroker()
	sub, unsub := b.Subscribe(Filter{})
	defer unsub()

	for i := 0; i < clientBuffer+10; i++ {
		b.Publish("e", "", "", json.RawMessage(`{}`))
	}
	if len(sub.ch) != clientBuffer {
		t.Fatalf("expected channel at capacity %d, got %d", clientBuffer, len(sub.ch))
	}
}

func TestIngestUnauthorized(t *testing.T) {
	s := NewServer("secret")
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"events":[]}`))
	w := httptest.NewRecorder()
	s.handleIngest(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestIngestAuthorized(t *testing.T) {
	s := NewServer("secret")
	body := `{"events":[{"event":"version.created","registry":"npmjs.org","ecosystem":"npm","name":"foo"}]}`
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	s.handleIngest(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	got := s.broker.Since(0, Filter{})
	if len(got) != 1 {
		t.Fatalf("expected 1 event in ring, got %d", len(got))
	}
	if got[0].Type != "version.created" || got[0].Registry != "npmjs.org" {
		t.Fatalf("unexpected event %+v", got[0])
	}
	if !strings.Contains(string(got[0].Data), `"name":"foo"`) {
		t.Fatalf("data not preserved: %s", got[0].Data)
	}
}

func TestIngestNoTokenAllowsAll(t *testing.T) {
	s := NewServer("")
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"events":[{"event":"x"}]}`))
	w := httptest.NewRecorder()
	s.handleIngest(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
}

func TestIngestBadJSON(t *testing.T) {
	s := NewServer("")
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`not json`))
	w := httptest.NewRecorder()
	s.handleIngest(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestIngestMethodNotAllowed(t *testing.T) {
	s := NewServer("")
	req := httptest.NewRequest(http.MethodGet, "/ingest", nil)
	w := httptest.NewRecorder()
	s.handleIngest(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestParseLastID(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/events", nil)
	r.Header.Set("Last-Event-ID", "42")
	if got := parseLastID(r); got != 42 {
		t.Fatalf("header: expected 42, got %d", got)
	}

	r = httptest.NewRequest(http.MethodGet, "/events?last_id=7", nil)
	if got := parseLastID(r); got != 7 {
		t.Fatalf("query: expected 7, got %d", got)
	}

	r = httptest.NewRequest(http.MethodGet, "/events", nil)
	if got := parseLastID(r); got != 0 {
		t.Fatalf("none: expected 0, got %d", got)
	}
}

func TestEventsSSEReplayAndStream(t *testing.T) {
	s := NewServer("")
	s.broker.Publish("a", "", "", json.RawMessage(`{"n":1}`))
	s.broker.Publish("b", "", "", json.RawMessage(`{"n":2}`))

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/events", nil)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", ct)
	}
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatal("expected X-Accel-Buffering: no")
	}

	reader := bufio.NewReader(resp.Body)
	deadline := time.After(2 * time.Second)
	var lines []string
	for len(lines) < 5 {
		lineCh := make(chan string, 1)
		go func() {
			line, _ := reader.ReadString('\n')
			lineCh <- line
		}()
		select {
		case l := <-lineCh:
			lines = append(lines, l)
		case <-deadline:
			t.Fatalf("timeout reading SSE, got %d lines: %q", len(lines), lines)
		}
	}

	joined := strings.Join(lines, "")
	if !strings.Contains(joined, "retry: 3000") {
		t.Errorf("missing retry directive: %q", joined)
	}
	if !strings.Contains(joined, "id: 2") || !strings.Contains(joined, `data: {"n":2}`) {
		t.Errorf("missing replayed event id 2: %q", joined)
	}
	if strings.Contains(joined, "id: 1") {
		t.Errorf("should not replay id 1 (Last-Event-ID was 1): %q", joined)
	}

	s.broker.Publish("c", "", "", json.RawMessage(`{"n":3}`))

	deadline = time.After(2 * time.Second)
	var streamed string
	for !strings.Contains(streamed, `data: {"n":3}`) {
		lineCh := make(chan string, 1)
		go func() {
			line, _ := reader.ReadString('\n')
			lineCh <- line
		}()
		select {
		case l := <-lineCh:
			streamed += l
		case <-deadline:
			t.Fatalf("timeout waiting for streamed event: %q", streamed)
		}
	}
	if !strings.Contains(streamed, "id: 3") {
		t.Errorf("missing streamed event id: %q", streamed)
	}
}

func TestStatusJSON(t *testing.T) {
	s := NewServer("")
	s.broker.Publish("x", "", "", json.RawMessage(`{}`))
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	w := httptest.NewRecorder()
	s.handleStatus(w, req)

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["events_seen"].(float64) != 1 {
		t.Fatalf("expected events_seen 1, got %v", body["events_seen"])
	}
}

func TestIndexHTML(t *testing.T) {
	s := NewServer("")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	s.handleIndex(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("expected text/html, got %s", ct)
	}
	if !strings.Contains(w.Body.String(), "Ecosyste.ms: Live") {
		t.Fatal("expected page title in body")
	}
}

func TestIndexNotFound(t *testing.T) {
	s := NewServer("")
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	w := httptest.NewRecorder()
	s.handleIndex(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestStaticServed(t *testing.T) {
	s := NewServer("")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/static/app.js")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
