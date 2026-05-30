package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCommunityCanonicalDedupKey(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "drops tracking query and lowercases host",
			raw:  "https://Example.com/Article/?utm_source=newsletter&b=2#part",
			want: "https://example.com/article?b=2#part",
		},
		{
			name: "keeps meaningful fragment",
			raw:  "https://cran.r-project.org/doc/manuals/r-release/NEWS.html#changes-in-r-4.6.0",
			want: "https://cran.r-project.org/doc/manuals/r-release/news.html#changes-in-r-4.6.0",
		},
		{
			name: "trims trailing slash before fragment",
			raw:  "https://example.com/path/#section",
			want: "https://example.com/path#section",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := communityCanonicalDedupKey(tt.raw); got != tt.want {
				t.Fatalf("communityCanonicalDedupKey(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestValidateRequiredCommunityRows(t *testing.T) {
	rows := []communityJSONLRow{
		{lineNo: 1, row: map[string]any{"source_id": "community:stackoverflow:r"}},
		{lineNo: 2, row: map[string]any{"source_id": "community:posit:latest-r-filtered"}},
	}
	if err := validateRequiredCommunityRows(rows, []string{"community:stackoverflow:r"}); err != nil {
		t.Fatalf("validateRequiredCommunityRows returned unexpected error: %v", err)
	}
	if err := validateRequiredCommunityRows(rows, []string{"reddit:r/rstats"}); err == nil {
		t.Fatal("validateRequiredCommunityRows returned nil for missing required source")
	}
}

func TestReadCommunityJSONLEventsAllowsMissingPrioritySource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "latest.jsonl")
	body := `{"source_id":"community:stackoverflow:r","external_id":"so-1","canonical_url":"https://stackoverflow.com/questions/1","published_at":"2026-05-31T00:00:00Z"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	events, selected, skipped, err := readCommunityJSONLEvents(
		context.Background(),
		path,
		10,
		[]string{"community:stackoverflow:r"},
		[]string{"community:stackoverflow:r", "community:posit:events"},
		nil,
		false,
	)
	if err != nil {
		t.Fatalf("readCommunityJSONLEvents returned unexpected error: %v", err)
	}
	if selected != 1 || skipped != 0 || len(events) != 1 {
		t.Fatalf("selected=%d skipped=%d events=%d, want selected=1 skipped=0 events=1", selected, skipped, len(events))
	}
}
