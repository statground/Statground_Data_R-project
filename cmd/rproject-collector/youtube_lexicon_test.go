package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"statground_data_r_project/internal/lexicon"
)

func TestYouTubeDiscoveryGateKeepsExactTargetsAndCannotRaiseIOLimit(t *testing.T) {
	for _, test := range []struct{ raw, want string }{{"0.90", "0.50"}, {"0.25", "0.25"}} {
		environ, err := youtubeDiscoveryGateEnv([]string{
			"CLICKHOUSE_PRESSURE_GATE_TARGETS=local:unrelated.table",
			"CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED=" + test.raw,
		})
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]string{}
		for _, entry := range environ {
			name, value, _ := strings.Cut(entry, "=")
			values[name] = value
		}
		if values["CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED"] != test.want || values["CLICKHOUSE_PRESSURE_GATE_TARGETS"] != youtubeDiscoveryGateTargets {
			t.Fatalf("gate changed scope or weakened pressure limit: %#v", values)
		}
		if !strings.Contains(values["CLICKHOUSE_PRESSURE_GATE_TARGETS"], "replica:Data_Content_Lexicon.keyword_selection_log_local") {
			t.Fatal("selection ledger omitted from write pressure gate")
		}
	}
	for _, raw := range []string{"NaN", "Inf", "-1", "invalid"} {
		if _, err := youtubeDiscoveryGateEnv([]string{"CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED=" + raw}); err == nil {
			t.Fatalf("invalid pressure limit %s accepted", raw)
		}
	}
}

func TestYouTubeDiscoveryFailedGateStopsBeforeDictionaryOrProvider(t *testing.T) {
	script := filepath.Join(t.TempDir(), "gate.py")
	if err := os.WriteFile(script, []byte("raise SystemExit(1)\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RPROJECT_PRESSURE_GATE_SCRIPT", script)
	t.Setenv("LEXICON_CANDIDATES_FILE", filepath.Join(t.TempDir(), "must-not-read.json"))
	events, err := youtubeSearchEvents(nil, 50)
	if err == nil || !strings.Contains(err.Error(), "pressure gate blocked") || len(events) != 0 {
		t.Fatalf("gate did not block before reading dictionary: events=%d err=%v", len(events), err)
	}
}

func youtubeTestSearchPlan(count int) []youtubeSearchPlanItem {
	plan := make([]youtubeSearchPlanItem, 0, count)
	for i := 0; i < count; i++ {
		origin := "curated"
		query := fmt.Sprintf("R tutorial %d", i)
		keyword := query
		if i%2 == 1 {
			origin = "lexicon"
			keyword = fmt.Sprintf("주제%d", i)
			query = "R programming " + keyword
		}
		plan = append(plan, youtubeSearchPlanItem{Query: query, Selection: lexicon.Selection{
			Keyword: keyword, Language: "ko", Origin: origin, SnapshotID: "snapshot-test", RunID: "run-test",
			Scope: youtubeLexiconScope, Sources: []string{"r_package_document"},
		}})
	}
	return plan
}

func TestBuildYouTubeSearchPlanBalancedStableAndKeepsKeywordLanguageSeparate(t *testing.T) {
	for _, name := range []string{"LEXICON_CLICKHOUSE_HTTP_URL", "CLICKHOUSE_HOST", "CH_HOST", "CLICKHOUSE_DSN"} {
		t.Setenv(name, "")
	}
	t.Setenv("LEXICON_STATE_DIR", t.TempDir())
	snapshot := strings.Repeat("a", 64)
	pool := []lexicon.Candidate{
		{Keyword: "통계", NormalizedWord: "통계", Language: "ko", Confidence: .99, SnapshotID: snapshot, Scope: youtubeLexiconScope, Sources: []string{"r_package_document"}},
		{Keyword: "variance", NormalizedWord: "variance", Language: "en", Confidence: .99, SnapshotID: snapshot, Scope: youtubeLexiconScope, Sources: []string{"r_package_document"}},
		{Keyword: "regression", NormalizedWord: "regression", Language: "en", Confidence: .99, SnapshotID: snapshot, Scope: youtubeLexiconScope, Sources: []string{"r_package_document"}},
	}
	curated := []string{"R programming tutorial", "R statistical computing", "R package tutorial"}
	first, err := buildYouTubeSearchPlan(curated, pool, snapshot, "stable-run", 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildYouTubeSearchPlan(curated, pool, snapshot, "stable-run", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 4 || len(second) != 4 {
		t.Fatalf("plan lengths=%d,%d", len(first), len(second))
	}
	for i, item := range first {
		if second[i].Query != item.Query || second[i].Selection.SelectedAt != item.Selection.SelectedAt {
			t.Fatal("same run did not replay its exact saved selections")
		}
		origin := "curated"
		if i%2 == 1 {
			origin = "lexicon"
			if item.Query != "R programming "+item.Selection.Keyword || item.Selection.Language == "und" {
				t.Fatalf("invalid dictionary query or language: %#v", item)
			}
		}
		if item.Selection.Origin != origin || item.Selection.Scope != youtubeLexiconScope {
			t.Fatalf("invalid balanced plan: %#v", item)
		}
	}
	if _, err := buildYouTubeSearchPlan(curated, nil, snapshot, "no-pool-run", 4); err == nil {
		t.Fatal("empty dictionary must not turn into an all-curated plan")
	}
}

func TestYouTubeSearchFairBudgetCallsBothHalvesAndPreservesQuota(t *testing.T) {
	t.Setenv("R_YOUTUBE_SEARCH_VIDEO_ENRICH_LIMIT", "500")
	t.Setenv("R_YOUTUBE_SEARCH_RESULT_LIMIT", "50")
	plan := youtubeTestSearchPlan(20)
	fetchCount, enrichCount, outcomes := 0, 0, 0
	seenQueries := map[string]bool{}
	fetch := func(target string) ([]byte, error) {
		parsed, err := url.Parse(target)
		if err != nil {
			t.Fatal(err)
		}
		seenQueries[parsed.Query().Get("search_query")] = true
		fetchCount++
		var body strings.Builder
		for i := 0; i < 50; i++ {
			fmt.Fprintf(&body, "/watch?v=video%06d ", fetchCount*100+i)
		}
		return []byte(body.String()), nil
	}
	enrich := func(id, target string, seed map[string]any) (map[string]any, error) {
		enrichCount++
		if seed["language_hint"] != "und" {
			t.Fatal("keyword language must not become assumed video language")
		}
		return map[string]any{
			"youtube_video_id": id, "canonical_url": target, "video_title": "Verified R lecture",
			"source_method": "youtube_data_api_v3_videos_list", "active": "1", "collection_status": "collected",
		}, nil
	}
	events, err := youtubeSearchEventsWithPlan(plan, 50, fetch, enrich, func(selection lexicon.Selection, status string, count int) error {
		outcomes++
		if status != "completed" || count == 0 {
			t.Fatalf("unexpected outcome %s count=%d", status, count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if fetchCount != 20 || len(seenQueries) != 20 || outcomes != 20 || enrichCount != 5 || len(events) > 50 {
		t.Fatalf("fetch=%d unique=%d outcomes=%d enrich=%d events=%d", fetchCount, len(seenQueries), outcomes, enrichCount, len(events))
	}
	quotaCount := 0
	origins := map[string]int{}
	for _, event := range events {
		if event.EventType == "r.youtube.quota.usage.v1" {
			quotaCount++
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["query_snapshot_id"] != "snapshot-test" || payload["query_run_id"] != "run-test" {
			t.Fatalf("missing selection provenance: %#v", payload)
		}
		if event.EventType == "r.youtube.search.result.v1" {
			origins[stringAny(payload["query_origin"])]++
		}
		if event.EventType == "r.youtube.video.candidate.v1" && payload["active"] != "0" {
			t.Fatal("unenriched candidate became public")
		}
	}
	if quotaCount != enrichCount || origins["curated"] == 0 || origins["lexicon"] == 0 {
		t.Fatalf("quota=%d enrich=%d origins=%v", quotaCount, enrichCount, origins)
	}
}

func TestYouTubeSearchEnrichmentErrorsConsumeAttemptBudget(t *testing.T) {
	t.Setenv("R_YOUTUBE_SEARCH_VIDEO_ENRICH_LIMIT", "5")
	attempts := 0
	events, err := youtubeSearchEventsWithPlan(youtubeTestSearchPlan(20), 50,
		func(string) ([]byte, error) { return []byte("/watch?v=otherVideo1"), nil },
		func(string, string, map[string]any) (map[string]any, error) {
			attempts++
			return nil, errors.New("metadata unavailable")
		},
		func(lexicon.Selection, string, int) error { return nil },
	)
	if err != nil || attempts != 5 || len(events) > 50 {
		t.Fatalf("attempts=%d events=%d err=%v", attempts, len(events), err)
	}
	for _, event := range events {
		if event.EventType == "r.youtube.video.snapshot.v1" {
			t.Fatal("failed metadata must not produce an active snapshot")
		}
	}
}

func TestYouTubeSearchRecordsEmptyFailureAndStopsOnLedgerFailure(t *testing.T) {
	plan := youtubeTestSearchPlan(4)
	fetches := 0
	statuses := []string{}
	ledgerErr := errors.New("receipt persistence unavailable")
	events, err := youtubeSearchEventsWithPlan(plan, 50,
		func(string) ([]byte, error) {
			fetches++
			if fetches == 1 {
				return nil, errors.New("provider unavailable")
			}
			return []byte("no public results"), nil
		}, nil,
		func(_ lexicon.Selection, status string, count int) error {
			statuses = append(statuses, status)
			if count != 0 {
				t.Fatal("empty/error results counted as records")
			}
			if len(statuses) == 2 {
				return ledgerErr
			}
			return nil
		},
	)
	if !errors.Is(err, ledgerErr) || fetches != 2 || len(events) != 1 || strings.Join(statuses, ",") != "provider_error,empty" {
		t.Fatalf("err=%v fetches=%d events=%d statuses=%v", err, fetches, len(events), statuses)
	}
}

func TestInterleaveYouTubePlanDoesNotStarveDictionary(t *testing.T) {
	original := youtubeTestSearchPlan(20)
	grouped := make([]youtubeSearchPlanItem, 0, 20)
	for _, origin := range []string{"curated", "lexicon"} {
		for _, item := range original {
			if item.Selection.Origin == origin {
				grouped = append(grouped, item)
			}
		}
	}
	for i, item := range interleaveYouTubeSearchPlan(grouped) {
		want := "curated"
		if i%2 == 1 {
			want = "lexicon"
		}
		if item.Selection.Origin != want {
			t.Fatalf("index=%d origin=%s want=%s", i, item.Selection.Origin, want)
		}
	}
}
