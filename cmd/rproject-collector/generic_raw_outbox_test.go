package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type genericRawOutboxFixture struct {
	mu           sync.Mutex
	acked        map[string]int
	queued       map[string]int
	rawAttempts  []int
	outboxWrites int
	failOutboxAt int
	rawStatus    func(int, int) int
}

func newGenericRawOutboxFixture(t *testing.T, rawStatus func(int, int) int) (*genericRawOutboxFixture, clickHouseQueryConfig) {
	t.Helper()
	t.Setenv("RPROJECT_CLICKHOUSE_ATTEMPTS", "1")
	t.Setenv("RPROJECT_CLICKHOUSE_QUERY_ATTEMPTS", "1")
	t.Setenv("RPROJECT_CLICKHOUSE_SPLIT_ON_TIMEOUT", "true")
	t.Setenv("R_YOUTUBE_PUBLISH_TRANSIENT_FAIL_OPEN", "true")
	f := &genericRawOutboxFixture{acked: map[string]int{}, queued: map[string]int{}, rawStatus: rawStatus}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read fixture request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		query := string(body)
		if strings.HasPrefix(strings.TrimSpace(query), "SELECT count()") {
			_, _ = io.WriteString(w, "{\"count\":0}\n")
			return
		}
		rows := fixtureGenericRawRows(t, directRowsJSONFromInsertBody(query))
		if strings.HasPrefix(query, "INSERT INTO "+rProjectDirectOutboxTable+" ") {
			f.outboxWrites++
			if f.outboxWrites == f.failOutboxAt {
				http.Error(w, "temporary upstream failure", http.StatusServiceUnavailable)
				return
			}
			if len(rows) != 1 {
				t.Errorf("outbox insert has %d envelope rows, want 1", len(rows))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			pending, _ := rows[0]["rows_json"].(string)
			for _, row := range fixtureGenericRawRows(t, pending) {
				f.queued[row["uuid"].(string)]++
			}
			return
		}
		if !strings.HasPrefix(query, "INSERT INTO Data_R_Community_Raw.r_youtube_event_raw ") {
			t.Error("unexpected fixture query target")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.rawAttempts = append(f.rawAttempts, len(rows))
		status := f.rawStatus(len(f.rawAttempts), len(rows))
		if status != http.StatusOK {
			message := "timeout"
			if status == http.StatusForbidden {
				message = "clickhouse-permission"
			}
			http.Error(w, message, status)
			return
		}
		for _, row := range rows {
			f.acked[row["uuid"].(string)]++
		}
	}))
	t.Cleanup(server.Close)
	cfg := clickHouseQueryConfig{Host: server.URL, User: "fixture", Password: "fixture", Timeout: time.Second, InsertDistributedSync: true}
	t.Setenv("CH_HOST", server.URL)
	t.Setenv("CH_USER", "fixture")
	t.Setenv("CH_PASSWORD", "fixture")
	return f, cfg
}

func fixtureGenericRawRows(t *testing.T, rowsJSON string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(rowsJSON), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Errorf("decode fixture row: %v", err)
			return nil
		}
		rows = append(rows, row)
	}
	return rows
}

func fixtureGenericRawEvents(count int) []genericEvent {
	events := make([]genericEvent, count)
	for i := range events {
		events[i] = genericEvent{EventID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), EventType: "r.youtube.video.upserted", Payload: fmt.Sprintf(`{"video_id":"video-%d"}`, i)}
	}
	return events
}

func assertGenericRawEventsDurable(t *testing.T, fixture *genericRawOutboxFixture, events []genericEvent) {
	t.Helper()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	for _, event := range events {
		if got := fixture.acked[event.EventID] + fixture.queued[event.EventID]; got != 1 {
			t.Errorf("event %s persisted %d times, want one ACK or outbox row", event.EventID, got)
		}
	}
}

func TestGenericRawSplitDeferralPreservesUnattemptedSiblings(t *testing.T) {
	f, cfg := newGenericRawOutboxFixture(t, func(int, int) int { return http.StatusRequestTimeout })
	events := fixtureGenericRawEvents(5)
	target, err := genericEventsDirectTarget(events)
	if err != nil {
		t.Fatal(err)
	}
	err = insertGenericRawEventChunkWithSplit(context.Background(), cfg, target, events)
	if err == nil || !shouldDeferYouTubePublishFailure(err) {
		t.Fatalf("durably queued transient publish should remain deferred: %v", err)
	}
	assertGenericRawEventsDurable(t, f, events)
	if fmt.Sprint(f.rawAttempts) != "[5 2 1]" {
		t.Fatalf("unattempted siblings must be queued without raw retry: %v", f.rawAttempts)
	}
}

func TestGenericRawDeferralPreservesLaterChunksAfterAcknowledgedPrefix(t *testing.T) {
	t.Setenv("RPROJECT_CLICKHOUSE_CHUNK_SIZE", "3")
	f, _ := newGenericRawOutboxFixture(t, func(attempt, _ int) int {
		if attempt == 1 {
			return http.StatusOK
		}
		return http.StatusRequestTimeout
	})
	events := fixtureGenericRawEvents(8)
	_, err := insertGenericRawEventsDirect(context.Background(), events)
	if err == nil || !shouldDeferYouTubePublishFailure(err) {
		t.Fatalf("durably queued transient publish should remain deferred: %v", err)
	}
	assertGenericRawEventsDurable(t, f, events)
	if len(f.acked) != 3 || len(f.queued) != 5 {
		t.Fatalf("want 3 ACKed and 5 queued events, got %d and %d", len(f.acked), len(f.queued))
	}
}

func TestGenericRawSplitFatalChildDoesNotQueueSiblings(t *testing.T) {
	f, cfg := newGenericRawOutboxFixture(t, func(attempt, _ int) int {
		if attempt == 1 {
			return http.StatusRequestTimeout
		}
		return http.StatusForbidden
	})
	events := fixtureGenericRawEvents(5)
	target, _ := genericEventsDirectTarget(events)
	err := insertGenericRawEventChunkWithSplit(context.Background(), cfg, target, events)
	if err == nil || shouldDeferYouTubePublishFailure(err) {
		t.Fatalf("permission failure must remain fatal: %v", err)
	}
	if f.outboxWrites != 0 {
		t.Fatalf("fatal child must not queue unattempted siblings: %d outbox writes", f.outboxWrites)
	}
}

func TestGenericRawUnattemptedSiblingPersistenceFailureIsHardError(t *testing.T) {
	f, cfg := newGenericRawOutboxFixture(t, func(int, int) int { return http.StatusRequestTimeout })
	f.failOutboxAt = 2
	events := fixtureGenericRawEvents(5)
	target, _ := genericEventsDirectTarget(events)
	err := insertGenericRawEventChunkWithSplit(context.Background(), cfg, target, events)
	if !isDirectOutboxPersistenceError(err) || shouldDeferYouTubePublishFailure(err) {
		t.Fatalf("failed sibling outbox persistence must retain hard error identity: %v", err)
	}
	if f.outboxWrites != 2 || len(f.queued) != 1 {
		t.Fatalf("must stop at failed sibling persistence: writes=%d queued=%d", f.outboxWrites, len(f.queued))
	}
}
