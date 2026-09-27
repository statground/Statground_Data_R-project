package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRbloggerAdditionalLocalesMatchWebRPublicMenu(t *testing.T) {
	locales, err := parseRbloggerAdditionalLocales("en,ja,zh-Hans,pt-BR,en")
	if err != nil || strings.Join(locales, ",") != "en,ja,zh-Hans,pt-BR" {
		t.Fatalf("unexpected locales=%v err=%v", locales, err)
	}
	for _, bad := range []string{"ko", "xx", "ja'; DROP TABLE board"} {
		if _, err := parseRbloggerAdditionalLocales(bad); err == nil {
			t.Fatalf("locale %q should be rejected", bad)
		}
	}
	if len(rbloggerLocaleNames) != 22 {
		t.Fatalf("supported additional locale count=%d, want 22", len(rbloggerLocaleNames))
	}
}

func TestRbloggerLocalePayloadKeepsSourceFingerprint(t *testing.T) {
	row := StaleRawArticle{UUID: "11111111-1111-4111-8111-111111111111", Title: "Title", Content: "Body", RawCreatedAt: "2026-09-27 12:00:00.000"}
	payload := additionalLocaleBoardPayload(row, "fr", "test-model", "Titre", "<p>Texte</p>", time.Date(2026, 9, 27, 13, 0, 0, 0, time.FixedZone("KST", 9*3600)))
	if payload["language_code"] != "fr" || payload["title"] != "Titre" || payload["active"] != 1 {
		t.Fatalf("unexpected locale payload: %v", payload)
	}
	log := payload["created_log"].(map[string]any)
	if log["source_sha256"] != "b93e9074bf0f60db9ff2f517fe8ce3ce2f29aec3e00989a2c1a8a727005bffc2" || log["target_language"] != "fr" || log["prompt_revision"] != 1 {
		t.Fatalf("missing source/version provenance: %v", log)
	}
}

func TestRbloggerEnglishLaneCopiesSourceWithoutProvider(t *testing.T) {
	title, content, err := translateArticleToLocale(nil, "", Article{
		ArticleHeadline: "R source", MetaDescription: "Read https://example.com now",
	}, "en")
	if err != nil || title != "R source" || strings.Contains(content, "https://") {
		t.Fatalf("English source copy should be sanitized, title=%q content=%q err=%v", title, content, err)
	}
	if _, _, err := translateArticleToLocale(nil, "", Article{ArticleHeadline: "R source"}, "fr"); err == nil {
		t.Fatal("non-English locale must not silently copy the English source")
	}
}

func TestRbloggerLocaleCandidateQueryPreservesTombstonesAndChecksHash(t *testing.T) {
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"uuid":  "11111111-1111-4111-8111-111111111111",
			"title": "Title", "content": "Body", "url": "https://example.org/a",
			"raw_created_at": "2026-09-27 12:00:00.000",
			"source_sha256":  rbloggerSourceSHA256("Title", "Body"),
		})
	}))
	defer server.Close()
	reader := NewClickHouseReader(ClickHouseConfig{Host: server.URL, Timeout: time.Second})
	rows, err := reader.MissingLocaleTranslations(context.Background(), "fr", 2)
	if err != nil || len(rows) != 1 {
		t.Fatalf("candidate query rows=%v err=%v", rows, err)
	}
	for _, required := range []string{
		"WHERE rn = 1 AND coalesce(active, 0) = 1",
		"coalesce(b.active, 0) = 1", "source_sha256", "unhex('0A')", "LIMIT 2",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("candidate query missing %q", required)
		}
	}
	if _, err := reader.MissingLocaleTranslations(context.Background(), "ko", 2); err == nil {
		t.Fatal("invalid locale must fail before querying")
	}
}
