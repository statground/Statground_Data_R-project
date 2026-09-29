package main

import (
	"fmt"
	"testing"
)

const suppressedYouTubeTestVideoID = "_adWwYQXPd8"

func TestFilterSuppressedYouTubeEventsCoversAutomaticEventFamilies(t *testing.T) {
	tests := []struct {
		name  string
		event genericEvent
	}{
		{
			name:  "search result parsed video id",
			event: youtubeSuppressionTestEvent("r.youtube.search.result.v1", "https://www.youtube.com/results?search_query=R", `{"parsed_video_id":"_adWwYQXPd8"}`),
		},
		{
			name:  "active video snapshot payload",
			event: youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "", `{"youtube_video_id":"_adWwYQXPd8","active":"1"}`),
		},
		{
			name:  "inactive candidate canonical url",
			event: youtubeSuppressionTestEvent("r.youtube.video.candidate.v1", "", `{"canonical_url":"https://www.youtube.com/watch?v=_adWwYQXPd8","active":"0"}`),
		},
		{
			name:  "page source url",
			event: youtubeSuppressionTestEvent("r.youtube.page.snapshot.v1", "https://youtu.be/_adWwYQXPd8", `{}`),
		},
		{
			name:  "link target url",
			event: youtubeSuppressionTestEvent("r.youtube.link.edge.v1", "", `{"target_url":"https://www.youtube.com/shorts/_adWwYQXPd8"}`),
		},
		{
			name:  "transcript segment",
			event: youtubeSuppressionTestEvent("r.youtube.transcript.segment.v1", "", `{"youtube_video_id":"_adWwYQXPd8","text":"sample"}`),
		},
		{
			name:  "comment thread",
			event: youtubeSuppressionTestEvent("r.youtube.comment.thread.v1", "", `{"youtube_video_id":"_adWwYQXPd8","comment_id":"comment-1"}`),
		},
		{
			name:  "package mention",
			event: youtubeSuppressionTestEvent("r.youtube.package.mention.v1", "", `{"youtube_video_id":"_adWwYQXPd8","package_name":"ggplot2"}`),
		},
		{
			name:  "metadata backfill failure source url",
			event: youtubeSuppressionTestEvent("r.youtube.collection.failure.v1", "https://www.youtube.com/watch?v=_adWwYQXPd8", `not-json`),
		},
		{
			name:  "malformed payload still fails closed",
			event: youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "", `broken-payload:_adWwYQXPd8`),
		},
		{
			name:  "nested thumbnail url",
			event: youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "", `{"thumbnails":{"high":{"url":"https://i.ytimg.com/vi/_adWwYQXPd8/hqdefault.jpg"}}}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filtered, suppressed := filterSuppressedYouTubeEvents([]genericEvent{test.event})
			if suppressed != 1 || len(filtered) != 0 {
				t.Fatalf("filterSuppressedYouTubeEvents() = (%d kept, %d suppressed), want (0, 1)", len(filtered), suppressed)
			}
		})
	}
}

func TestFilterSuppressedYouTubeEventsUsesVideoIDNotEventUUID(t *testing.T) {
	suppressed := youtubeSuppressionTestEvent(
		"r.youtube.video.snapshot.v1",
		"",
		`{"youtube_video_id":"_adWwYQXPd8","stable_uuid":"cf24eaec-486d-5804-935a-0b21612f8f9a"}`,
	)
	suppressed.EventID = "different-observation-uuid"
	uuidOnly := youtubeSuppressionTestEvent(
		"r.youtube.video.snapshot.v1",
		"https://www.youtube.com/watch?v=otherVideo1",
		`{"youtube_video_id":"otherVideo1","legacy_uuid":"01a092a6-5dbb-735e-a5dc-739d76564e9f"}`,
	)
	uuidOnly.EventID = "01a092a6-5dbb-735e-a5dc-739d76564e9f"

	filtered, count := filterSuppressedYouTubeEvents([]genericEvent{suppressed, uuidOnly})
	if count != 1 || len(filtered) != 1 || filtered[0].EventID != uuidOnly.EventID {
		t.Fatalf("filter result = %#v, suppressed=%d; want UUID-only event preserved", filtered, count)
	}
}

func TestFilterSuppressedYouTubeEventsMatchesExactVideoID(t *testing.T) {
	events := []genericEvent{
		youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "", fmt.Sprintf(`{"youtube_video_id":"prefix%s"}`, suppressedYouTubeTestVideoID)),
		youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "", fmt.Sprintf(`{"youtube_video_id":"%ssuffix"}`, suppressedYouTubeTestVideoID)),
		youtubeSuppressionTestEvent("r.community.item.v1", "", fmt.Sprintf(`{"youtube_video_id":"%s"}`, suppressedYouTubeTestVideoID)),
		youtubeSuppressionTestEvent("r.youtube.comment.thread.v1", "https://www.youtube.com/watch?v=otherVideo1", fmt.Sprintf(`{"youtube_video_id":"otherVideo1","text_normalized":"See video %s for context"}`, suppressedYouTubeTestVideoID)),
		youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "https://www.youtube.com/watch?v=otherVideo1", fmt.Sprintf(`{"youtube_video_id":"otherVideo1","video_title":"Review of %s","video_description":"Mentions %s without changing source identity"}`, suppressedYouTubeTestVideoID, suppressedYouTubeTestVideoID)),
	}

	filtered, count := filterSuppressedYouTubeEvents(events)
	if count != 0 || len(filtered) != len(events) {
		t.Fatalf("filter result = %d kept, %d suppressed; want all non-matches preserved", len(filtered), count)
	}
}

func TestFilterSuppressedYouTubeEventsPreservesOrder(t *testing.T) {
	first := youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "https://www.youtube.com/watch?v=firstVideo1", `{"youtube_video_id":"firstVideo1"}`)
	blocked := youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "https://www.youtube.com/watch?v=_adWwYQXPd8", `not-json`)
	last := youtubeSuppressionTestEvent("r.youtube.video.snapshot.v1", "https://www.youtube.com/watch?v=lastVideo01", `{"youtube_video_id":"lastVideo01"}`)

	filtered, count := filterSuppressedYouTubeEvents([]genericEvent{first, blocked, last})
	if count != 1 || len(filtered) != 2 || filtered[0].SourceURL != first.SourceURL || filtered[1].SourceURL != last.SourceURL {
		t.Fatalf("filter result = %#v, suppressed=%d; want unblocked events in input order", filtered, count)
	}
}

func TestFilterSuppressedYouTubeEventsPreservesUnrelatedMalformedPayload(t *testing.T) {
	event := youtubeSuppressionTestEvent(
		"r.youtube.collection.failure.v1",
		"https://www.youtube.com/watch?v=otherVideo1",
		`upstream returned malformed metadata for otherVideo1`,
	)

	filtered, count := filterSuppressedYouTubeEvents([]genericEvent{event})
	if count != 0 || len(filtered) != 1 {
		t.Fatalf("filter result = %#v, suppressed=%d; want unrelated malformed event preserved", filtered, count)
	}
}

func TestYouTubeCandidateAndSeedBoundariesRejectSuppressedVideoID(t *testing.T) {
	t.Setenv("R_YOUTUBE_VIDEO_IDS", "")
	t.Setenv("R_YOUTUBE_VIDEO_URLS", "")
	seeds := []map[string]any{
		{
			"parsed_video_id": suppressedYouTubeTestVideoID,
			"url":             "https://www.youtube.com/watch?v=" + suppressedYouTubeTestVideoID,
		},
		{
			"parsed_video_id": "otherVideo1",
			"url":             "https://www.youtube.com/watch?v=otherVideo1",
		},
	}

	candidates := youtubeVideoCandidates(seeds, 0)
	if len(candidates) != 1 || candidates[0].videoID != "otherVideo1" {
		t.Fatalf("youtubeVideoCandidates() = %#v, want only the unsuppressed source", candidates)
	}
	seedEvents := youtubeSeedEvents(seeds, 0)
	if len(seedEvents) != 1 || containsSuppressedYouTubeReference(seedEvents[0].SourceURL) {
		t.Fatalf("youtubeSeedEvents() = %#v, want only the unsuppressed source", seedEvents)
	}
}

func youtubeSuppressionTestEvent(eventType, sourceURL, payload string) genericEvent {
	return genericEvent{
		EventID:   "test-event-uuid",
		EventType: eventType,
		SourceURL: sourceURL,
		Payload:   payload,
	}
}
