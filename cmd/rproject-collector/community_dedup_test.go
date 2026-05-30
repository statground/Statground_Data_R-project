package main

import "testing"

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
