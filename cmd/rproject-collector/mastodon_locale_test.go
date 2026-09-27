package main

import (
	"strings"
	"testing"
)

func TestParseMastodonLocalesBounds(t *testing.T) {
	got, err := parseMastodonLocales("ZH-hans, en, zh-Hans")
	if err != nil || len(got) != 2 || got[0] != "zh-Hans" || got[1] != "en" {
		t.Fatalf("canonical locale selection: %v %v", got, err)
	}
	for _, raw := range []string{"", "ko", "en,ja,fr", "en' OR 1=1"} {
		if _, err := parseMastodonLocales(raw); err == nil {
			t.Errorf("accepted unsafe or unbounded locales %q", raw)
		}
	}
}

func TestMastodonLocaleQueriesFenceLatestSource(t *testing.T) {
	q := mastodonLocaleCandidatesSQL("ja", 2)
	for _, part := range []string{
		"row_number() OVER (PARTITION BY uuid ORDER BY fetched_at DESC, ingested_at DESC, event_uuid DESC)",
		"r.rn = 1 AND r.active = 1", "r.visibility IN ('public', 'unlisted')", "r.language_code = 'en'",
		"b.rn = 1", "source_sha256') != r.source_sha256", "LIMIT 2",
	} {
		if !strings.Contains(q, part) {
			t.Errorf("candidate query missing %q", part)
		}
	}
	if strings.Index(q, "row_number() OVER (PARTITION BY uuid ORDER BY fetched_at") > strings.Index(q, "r.rn = 1 AND r.active = 1") {
		t.Fatal("active state was checked before latest source version")
	}
	filter, err := mastodonLocaleUUIDFilter(map[string]string{"11111111-2222-3333-4444-555555555555": "sha"})
	if err != nil || !strings.Contains(filter, "toString(uuid) IN ('11111111-2222-3333-4444-555555555555')") {
		t.Fatalf("safe UUID predicate: %q %v", filter, err)
	}
	if _, err := mastodonLocaleUUIDFilter(map[string]string{"x') OR 1=1 --": "sha"}); err == nil {
		t.Fatal("accepted SQL injection in UUID")
	}
}

func TestMastodonLocaleSourceCopyAndTranslationGuard(t *testing.T) {
	row := mastodonLocaleCandidate{
		UUID: "11111111-2222-3333-4444-555555555555", StatusURL: "https://fosstodon.org/@R_Foundation/1",
		ContentText: "R 4.5 is available at https://r-project.org. <script>alert(1)</script>", SourceSHA256: strings.Repeat("a", 64),
	}
	title, content, err := translateMastodonLocale(nil, "", row, "en")
	if err != nil || title == "" || strings.Contains(content, "<script>") || !strings.Contains(content, "&lt;script&gt;") {
		t.Fatalf("English source copy was not escaped: %q %q %v", title, content, err)
	}
	if !strings.Contains(content, "R 4.5 is available") || strings.Contains(content, "https://") {
		t.Fatalf("English source copy lost its text or retained a URL: %q", content)
	}
	if _, _, err := translateMastodonLocale(nil, "", row, "ja"); err == nil {
		t.Fatal("accepted non-English locale without translation provider")
	}
	if _, err := mastodonLocaleCandidateFromRow(map[string]any{"uuid": "not-a-uuid", "source_sha256": row.SourceSHA256}); err == nil {
		t.Fatal("accepted incomplete source identity")
	}
}
