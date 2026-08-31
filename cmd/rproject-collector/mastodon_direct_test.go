package main

import (
	"strings"
	"testing"
)

func TestMastodonRawDirectRowNormalizesEditedAtForClickHouse(t *testing.T) {
	event := webREvent{EventUUID: "event-1", CreatedAt: "2026-08-31 16:59:40.123"}
	payload := map[string]any{
		"uuid":             "018f2f22-593d-7aa1-8000-000000000001",
		"status_edited_at": "2026-08-31T07:59:40.123Z",
	}

	row, err := mastodonRawDirectRow(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := row["status_edited_at"], any("2026-08-31 16:59:40.123"); got != want {
		t.Fatalf("status_edited_at = %#v, want %#v", got, want)
	}
}

func TestMastodonRawDirectRowRejectsInvalidEditedAt(t *testing.T) {
	event := webREvent{EventUUID: "event-1", CreatedAt: "2026-08-31 16:59:40.123"}
	payload := map[string]any{
		"uuid":             "018f2f22-593d-7aa1-8000-000000000001",
		"status_edited_at": "not-a-date",
	}

	_, err := mastodonRawDirectRow(event, payload)
	if err == nil || !strings.Contains(err.Error(), "invalid payload.status_edited_at") {
		t.Fatalf("mastodonRawDirectRow error = %v, want invalid status_edited_at", err)
	}
}
