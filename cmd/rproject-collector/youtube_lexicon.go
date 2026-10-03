package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"statground_data_r_project/internal/lexicon"
)

const youtubeLexiconScope = "r-youtube"

const youtubeDiscoveryGateTargets = "replica:Data_Content_Lexicon.keyword_selection_log_local," +
	"replica:Data_R_Community_Raw.r_youtube_event_raw_local," +
	"replica:Data_R_Community_Raw.r_youtube_video_snapshot_raw_local," +
	"replica:Data_R_Community_Service.r_youtube_video_current_local," +
	"replica:Data_R_Community_Raw.r_youtube_package_mention_raw_local," +
	"replica:Data_R_Community_Service.r_youtube_package_mention_current_local," +
	"replica:Data_R_Community_Log.api_quota_ledger_local," +
	"local:Data_R_Community_Log.r_project_direct_insert_outbox"

func youtubeDiscoveryGateEnv(environ []string) ([]string, error) {
	result := make([]string, 0, len(environ)+2)
	maxIO := "0.50"
	for _, entry := range environ {
		if strings.HasPrefix(entry, "CLICKHOUSE_PRESSURE_GATE_TARGETS=") {
			continue
		}
		if raw, ok := strings.CutPrefix(entry, "CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED="); ok {
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				return nil, errors.New("YouTube discovery pressure threshold is invalid")
			}
			if value < .5 {
				maxIO = raw
			}
			continue
		}
		result = append(result, entry)
	}
	return append(result, "CLICKHOUSE_PRESSURE_GATE_TARGETS="+youtubeDiscoveryGateTargets, "CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED="+maxIO), nil
}

func gateYouTubeDiscoveryWrites(ctx context.Context) error {
	script := envString("RPROJECT_PRESSURE_GATE_SCRIPT", "scripts/clickhouse_pressure_gate.py")
	path, err := filepath.Abs(script)
	if err != nil {
		return errors.New("YouTube discovery pressure gate path is invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("YouTube discovery pressure gate is unavailable")
	}
	environ, err := youtubeDiscoveryGateEnv(os.Environ())
	if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "python3", path)
	command.Env = environ
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if command.Run() != nil {
		return errors.New("YouTube discovery pressure gate blocked")
	}
	return nil
}

type youtubeSearchPlanItem struct {
	Selection lexicon.Selection
	Query     string
}

func buildYouTubeSearchPlan(curated []string, pool []lexicon.Candidate, snapshotID, runID string, count int) ([]youtubeSearchPlanItem, error) {
	if count < 2 || count%2 != 0 || count > 20 {
		return nil, errors.New("YouTube discovery requires an even query count between 2 and 20")
	}
	selected, err := lexicon.Select(curated, pool, count, runID, youtubeLexiconScope)
	if err != nil {
		return nil, err
	}
	plan := make([]youtubeSearchPlanItem, 0, len(selected))
	for _, selection := range selected {
		if selection.Origin == "lexicon" && selection.SnapshotID == "" {
			selection.SnapshotID = snapshotID
		}
		query := selection.Keyword
		if selection.Origin == "lexicon" {
			query = "R programming " + query
		}
		plan = append(plan, youtubeSearchPlanItem{Selection: selection, Query: query})
	}
	return interleaveYouTubeSearchPlan(plan), nil
}

func interleaveYouTubeSearchPlan(plan []youtubeSearchPlanItem) []youtubeSearchPlanItem {
	curated := make([]youtubeSearchPlanItem, 0, len(plan)/2)
	dictionary := make([]youtubeSearchPlanItem, 0, len(plan)/2)
	for _, item := range plan {
		if item.Selection.Origin == "lexicon" {
			dictionary = append(dictionary, item)
		} else {
			curated = append(curated, item)
		}
	}
	out := make([]youtubeSearchPlanItem, 0, len(plan))
	for i := 0; i < len(curated) || i < len(dictionary); i++ {
		if i < len(curated) {
			out = append(out, curated[i])
		}
		if i < len(dictionary) {
			out = append(out, dictionary[i])
		}
	}
	return out
}

func youtubeSearchEvents(seeds []map[string]any, limit int) ([]genericEvent, error) {
	ctx := context.Background()
	if err := gateYouTubeDiscoveryWrites(ctx); err != nil {
		return nil, err
	}
	pool, snapshotID, err := lexicon.FromEnv(ctx, youtubeLexiconScope)
	if err != nil {
		fmt.Printf("[youtube] discovery_skipped reason=dictionary_unavailable error_type=%T\n", err)
		return nil, nil
	}
	count := envInt("R_YOUTUBE_SEARCH_QUERY_LIMIT", 20)
	if limit > 0 && limit < count*2 {
		count = (limit / 2) &^ 1
	}
	plan, err := buildYouTubeSearchPlan(youtubeCuratedSearchQueries(seeds), pool, snapshotID, lexicon.RunID(), count)
	if err != nil {
		fmt.Printf("[youtube] discovery_skipped reason=balanced_plan_unavailable error_type=%T\n", err)
		return nil, nil
	}
	selections := make([]lexicon.Selection, 0, len(plan))
	for _, item := range plan {
		selections = append(selections, item.Selection)
	}
	if err := lexicon.Record(ctx, selections); err != nil {
		return nil, fmt.Errorf("YouTube discovery receipt could not be committed: %w", err)
	}
	fmt.Printf("[youtube] discovery_plan curated=%d dictionary=%d queries=%d event_limit=%d snapshot=%s\n", len(plan)/2, len(plan)/2, len(plan), limit, snapshotID)
	return youtubeSearchEventsWithPlan(plan, limit, fetchBytes, fetchYouTubeVideoSnapshotPayload, func(selection lexicon.Selection, outcome string, resultCount int) error {
		return lexicon.Outcome(ctx, []lexicon.Selection{selection}, outcome, resultCount)
	})
}

type youtubeSearchFetch func(string) ([]byte, error)
type youtubeSearchEnrich func(string, string, map[string]any) (map[string]any, error)
type youtubeSearchOutcome func(lexicon.Selection, string, int) error

func youtubeSearchEventsWithPlan(plan []youtubeSearchPlanItem, limit int, fetch youtubeSearchFetch, enrich youtubeSearchEnrich, outcome youtubeSearchOutcome) ([]genericEvent, error) {
	events := make([]genericEvent, 0)
	enrichAttempts := 0
	enrichLimit := minInt(5, maxInt(0, envInt("R_YOUTUBE_SEARCH_VIDEO_ENRICH_LIMIT", 5)))
	for i, item := range plan {
		if limit > 0 && len(events) >= limit {
			break
		}
		query := item.Query
		searchURL := "https://www.youtube.com/results?search_query=" + url.QueryEscape(query)
		body, err := fetch(searchURL)
		if err != nil {
			payload := map[string]any{"source_url": searchURL, "error_code": fmt.Sprintf("%T", err), "source_method": "youtube_public_search_html", "collection_status": "failed"}
			addYouTubeSelectionMetadata(payload, item)
			events = append(events, newGenericEvent("r.youtube.collection.failure.v1", "youtube_public_search_html", searchURL, "R-YouTube", "", "", "", payload))
			if err := outcome(item.Selection, "provider_error", 0); err != nil {
				return events, err
			}
			continue
		}
		remainingQueries := len(plan) - i
		queryCap := 0
		if limit > 0 {
			// Reserve the possible API quota events separately. A large first
			// result page must not consume the dictionary half of this plan.
			quotaReserve := enrichLimit - enrichAttempts
			queryCap = maxInt(1, (limit-len(events)-quotaReserve)/remainingQueries)
		}
		queryEvents := make([]genericEvent, 0)
		baseEvents := 0
		resultCount := 0
		results := extractYouTubeSearchResults(string(body), query, searchURL)
		for _, result := range results {
			if queryCap > 0 && baseEvents >= queryCap {
				break
			}
			if isSuppressedYouTubeVideoID(result["parsed_video_id"]) || containsSuppressedYouTubeReference(result["result_url"]) {
				continue
			}
			payload := mapStringAny(result)
			addYouTubeSelectionMetadata(payload, item)
			queryEvents = append(queryEvents, newGenericEvent("r.youtube.search.result.v1", "youtube_public_search_html", result["result_url"], "R-YouTube", "", "", "", payload))
			baseEvents++
			resultCount++
			if result["parsed_video_id"] == "" || queryCap > 0 && baseEvents >= queryCap {
				continue
			}
			canEnrich := enrich != nil && enrichAttempts < enrichLimit
			if limit > 0 && len(events)+len(queryEvents)+2+remainingQueries-1 > limit {
				canEnrich = false
			}
			if canEnrich {
				seedPayload := map[string]any{"title": query, "url": result["result_url"], "category": "search_result", "source_type": "video", "source_confidence": "search_html_discovered", "language_hint": "und"}
				// Bound attempts, including errors, rather than successful enrichments.
				enrichAttempts++
				if videoPayload, err := enrich(result["parsed_video_id"], result["result_url"], seedPayload); err == nil {
					videoPayload["source_category"] = "search_result"
					videoPayload["source_confidence"] = "search_html_discovered"
					videoPayload["search_query"] = query
					addYouTubeSelectionMetadata(videoPayload, item)
					queryEvents = append(queryEvents, newGenericEvent("r.youtube.video.snapshot.v1", stringAny(videoPayload["source_method"]), result["result_url"], "R-YouTube", "", "", stringAny(videoPayload["published_at"]), videoPayload))
					baseEvents++
					if strings.Contains(stringAny(videoPayload["source_method"]), "youtube_data_api") {
						queryEvents = append(queryEvents, youtubeQuotaUsageEvent(result["result_url"]))
					}
					for _, mention := range youtubeMetadataPackageMentionEvents(result["parsed_video_id"], videoPayload) {
						if queryCap > 0 && baseEvents >= queryCap {
							break
						}
						queryEvents = append(queryEvents, mention)
						baseEvents++
					}
					continue
				}
			}
			videoPayload := youtubeUnenrichedSearchCandidate(result)
			videoPayload["video_description"] = "Discovered from YouTube search query: " + query
			addYouTubeSelectionMetadata(videoPayload, item)
			finalizeYouTubeVideoPayload(videoPayload)
			queryEvents = append(queryEvents, newGenericEvent("r.youtube.video.candidate.v1", "youtube_public_search_html", result["result_url"], "R-YouTube", "", "", "", videoPayload))
			baseEvents++
		}
		events = append(events, queryEvents...)
		status := "completed"
		if resultCount == 0 {
			status = "empty"
		}
		if err := outcome(item.Selection, status, resultCount); err != nil {
			return events, err
		}
	}
	return events, nil
}

func addYouTubeSelectionMetadata(payload map[string]any, item youtubeSearchPlanItem) {
	payload["query_origin"] = item.Selection.Origin
	payload["query_keyword"] = item.Selection.Keyword
	payload["query_language"] = item.Selection.Language
	payload["query_snapshot_id"] = item.Selection.SnapshotID
	payload["query_run_id"] = item.Selection.RunID
	payload["query_scope"] = item.Selection.Scope
	payload["query_sources"] = item.Selection.Sources
}

func youtubeUnenrichedSearchCandidate(result map[string]string) map[string]any {
	videoID := result["parsed_video_id"]
	return map[string]any{
		"youtube_video_id": videoID, "youtube_channel_id": "", "playlist_ids_json": "[]",
		"video_title": "YouTube video " + videoID, "canonical_url": result["result_url"],
		"thumbnail_url": "https://i.ytimg.com/vi/" + videoID + "/hqdefault.jpg", "published_at": "",
		"duration_seconds": "0", "view_count": "0", "like_count": "0", "comment_count": "0",
		"favorite_count": "0", "caption_available": "0", "default_audio_language": "", "default_language": "",
		"language_code": "und", "tags_json": "[]",
		"thumbnail_urls_json": mustJSON(map[string]any{"hqdefault": map[string]any{"url": "https://i.ytimg.com/vi/" + videoID + "/hqdefault.jpg"}}),
		"channel_title":       "", "privacy_status": "", "source_method": "youtube_public_search_html_unenriched_candidate",
		"source_tag": "r_project_ecosystem_youtube", "source_category": "search_result",
		"source_confidence": "search_html_discovered_unenriched", "metadata_errors_json": mustJSON([]string{"metadata_enrich_limit_reached"}),
		"active": "0", "collection_status": "candidate",
	}
}
