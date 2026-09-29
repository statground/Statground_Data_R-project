package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type youtubeTestTransport func(*http.Request) (*http.Response, error)

func (f youtubeTestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func withYouTubeTestTransport(t *testing.T, f youtubeTestTransport) {
	t.Helper()
	previous := http.DefaultTransport
	http.DefaultTransport = f
	t.Cleanup(func() { http.DefaultTransport = previous })
}

func youtubeTestResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestYouTubePageSignalsCannotPublishActiveVideo(t *testing.T) {
	for _, response := range []struct {
		name   string
		status int
		body   string
	}{
		{"private generic page", 200, `<title>YouTube</title>`},
		{"unreachable page", 503, `temporary outage`},
		{"real title still requires enrichment", 200, `<meta property="og:title" content="R packages tutorial"><meta property="og:image" content="https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg">`},
	} {
		t.Run(response.name, func(t *testing.T) {
			withYouTubeTestTransport(t, func(*http.Request) (*http.Response, error) {
				return youtubeTestResponse(response.status, response.body), nil
			})
			events := youtubePageEvents([]map[string]any{{"url": "https://www.youtube.com/watch?v=abcdefghijk", "source_type": "video"}}, 1)
			if len(events) != 2 || events[1].EventType != "r.youtube.video.candidate.v1" {
				t.Fatalf("page must emit page evidence plus a candidate, got %#v", events)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(events[1].Payload), &payload); err != nil {
				t.Fatal(err)
			}
			if payload["active"] != "0" {
				t.Fatalf("unverified candidate active=%v", payload["active"])
			}
		})
	}
}

func TestYouTubeMissingAPIVideoCannotFallBackToCachedMetadata(t *testing.T) {
	t.Setenv("YOUTUBE_API_KEY", "test-key")
	t.Setenv("GOOGLE_YOUTUBE_API_KEY", "")
	t.Setenv("R_YOUTUBE_DISABLE_DATA_API", "false")
	calls := 0
	withYouTubeTestTransport(t, func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Host != "www.googleapis.com" {
			t.Fatalf("withdrawn API video reached fallback %s", req.URL.Host)
		}
		return youtubeTestResponse(200, `{"items":[]}`), nil
	})
	_, err := fetchYouTubeVideoSnapshotPayload("abcdefghijk", "https://www.youtube.com/watch?v=abcdefghijk", map[string]any{"title": "Previous good title"})
	if reason, ok := youtubeUnavailableReason(err); !ok || reason != "api_video_not_public" {
		t.Fatalf("error=%v", err)
	}
	if calls != 1 {
		t.Fatalf("API absence caused %d requests", calls)
	}
}

func TestYouTubeTransientErrorsAndGenericHTMLCannotPromoteVideo(t *testing.T) {
	t.Setenv("YOUTUBE_API_KEY", "test-key")
	t.Setenv("R_YOUTUBE_DISABLE_DATA_API", "false")
	t.Setenv("R_YOUTUBE_DISABLE_YTDLP", "true")
	withYouTubeTestTransport(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/watch" {
			return youtubeTestResponse(200, `<title>YouTube</title>`), nil
		}
		return youtubeTestResponse(403, `quota or authentication failure`), nil
	})
	_, err := fetchYouTubeVideoSnapshotPayload("abcdefghijk", "https://www.youtube.com/watch?v=abcdefghijk", map[string]any{"title": "Previous good title"})
	if err == nil {
		t.Fatal("generic HTTP200 HTML must not activate a video")
	}
	if _, unavailable := youtubeUnavailableReason(err); unavailable {
		t.Fatalf("transient failure classified as withdrawn: %v", err)
	}
}

func TestYouTubeMalformedAPISuccessIsNotWithdrawal(t *testing.T) {
	for _, body := range []string{`{}`, `{"items":null}`, `{"items":"invalid"}`, `{"error":{"code":403}}`} {
		t.Run(body, func(t *testing.T) {
			withYouTubeTestTransport(t, func(*http.Request) (*http.Response, error) { return youtubeTestResponse(200, body), nil })
			_, err := fetchYouTubeAPIVideoPayload("abcdefghijk", "test-key", nil)
			if err == nil {
				t.Fatal("malformed successful response accepted")
			}
			if _, unavailable := youtubeUnavailableReason(err); unavailable {
				t.Fatalf("malformed response withdrew video: %v", err)
			}
		})
	}
}

func TestYouTubePrivateWatchCanConfirmWithdrawalAfterAPIFailure(t *testing.T) {
	t.Setenv("YOUTUBE_API_KEY", "test-key")
	t.Setenv("R_YOUTUBE_DISABLE_DATA_API", "false")
	t.Setenv("R_YOUTUBE_DISABLE_YTDLP", "true")
	withYouTubeTestTransport(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/watch" {
			if req.URL.Host != "www.youtube.com" || req.URL.Query().Get("hl") != "en" {
				t.Fatalf("untrusted availability URL: %s", req.URL.String())
			}
			return youtubeTestResponse(200, `<script>var ytInitialPlayerResponse = {"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"This is a private video"}};</script>`), nil
		}
		return youtubeTestResponse(403, `API quota or oEmbed denial`), nil
	})
	_, err := fetchYouTubeVideoSnapshotPayload("abcdefghijk", "https://www.youtube.com/watch?v=abcdefghijk", map[string]any{"title": "Previous title"})
	if reason, ok := youtubeUnavailableReason(err); !ok || reason != "watch_private" {
		t.Fatalf("error=%v", err)
	}
}

func TestYouTubeWatchRequiresMatchingPlayableDetails(t *testing.T) {
	for _, test := range []struct {
		name, body, reason string
		success            bool
	}{
		{"private English", `{"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"This is a private video"}}`, "watch_private", false},
		{"private Korean", `{"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"비공개 동영상"}}`, "watch_private", false},
		{"terminated uploader", `{"playabilityStatus":{"status":"ERROR","reason":"This video is no longer available because the uploader has closed their YouTube account."}}`, "watch_removed", false},
		{"bot check", `{"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"Sign in to confirm you're not a bot"}}`, "", false},
		{"age check", `{"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"Sign in to confirm your age"}}`, "", false},
		{"generic unavailable", `{"playabilityStatus":{"status":"ERROR","reason":"Video unavailable"}}`, "", false},
		{"wrong identity", `{"playabilityStatus":{"status":"OK"},"videoDetails":{"videoId":"otherVideo1","title":"R tutorial"}}`, "", false},
		{"placeholder details", `{"playabilityStatus":{"status":"OK"},"videoDetails":{"videoId":"abcdefghijk","title":"youtube video #abcdefghijk"}}`, "", false},
		{"verified watch", `{"playabilityStatus":{"status":"OK"},"videoDetails":{"videoId":"abcdefghijk","title":"R packages tutorial","channelId":"UC_test","author":"R teacher","lengthSeconds":"360","viewCount":"123"}}`, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := youtubeWatchVideoPayload("abcdefghijk", `<script>var ytInitialPlayerResponse = `+test.body+`; window.other={};</script>`)
			reason, unavailable := youtubeUnavailableReason(err)
			if reason != test.reason || unavailable != (test.reason != "") {
				t.Fatalf("reason=%q unavailable=%v err=%v", reason, unavailable, err)
			}
			if (err == nil) != test.success {
				t.Fatalf("payload=%v error=%v", payload, err)
			}
		})
	}
}

func TestYouTubeRefreshWithdrawsOnlyConfirmedUnavailable(t *testing.T) {
	row := map[string]any{"youtube_video_id": "abcdefghijk", "stable_uuid": "d5d659de-001c-4426-bc0d-ef5052caa082", "video_title": "R tutorial", "thumbnail_url": "https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg", "source_tag": "web_r_official_youtube", "uuid_article": "d5d659de-001c-4426-bc0d-ef5052caa083"}
	for _, test := range []struct {
		name     string
		failure  error
		snapshot bool
		active   string
	}{
		{"network", errors.New("connection reset"), false, ""},
		{"quota", errors.New("HTTP 403: quotaExceeded"), false, ""},
		{"metadata missing", errors.New("watch page has no usable matching video details"), false, ""},
		{"private", youtubeVideoUnavailableError{reason: "watch_private"}, true, "0"},
		{"deleted", youtubeVideoUnavailableError{reason: "api_video_not_public"}, true, "0"},
		{"public", nil, true, "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := youtubeMetadataRefreshEvents([]map[string]any{row}, func(id, canonical string, seed map[string]any) (map[string]any, error) {
				if test.failure != nil {
					return nil, test.failure
				}
				return baseYouTubeVideoPayload(id, canonical, seed), nil
			})
			found := false
			checks := 0
			for _, event := range events {
				if event.EventType == "r.youtube.availability.check.v1" {
					checks++
					var payload map[string]any
					if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
						t.Fatal(err)
					}
					status := "unknown"
					if test.failure == nil {
						status = "available"
					} else if _, unavailable := youtubeUnavailableReason(test.failure); unavailable {
						status = "unavailable"
					}
					if payload["collection_status"] != status || payload["youtube_video_id"] != row["youtube_video_id"] || stringAny(payload["checked_at"]) == "" {
						t.Fatalf("missing durable rotation checkpoint: %v", payload)
					}
				}
				if event.EventType != "r.youtube.video.snapshot.v1" {
					continue
				}
				if found {
					t.Fatal("one refresh must not publish deactivate-then-activate pair")
				}
				found = true
				var payload map[string]any
				if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
					t.Fatal(err)
				}
				for key, want := range map[string]string{"active": test.active, "stable_uuid": stringAny(row["stable_uuid"]), "uuid_article": stringAny(row["uuid_article"]), "source_tag": stringAny(row["source_tag"])} {
					if got := stringAny(payload[key]); got != want {
						t.Fatalf("%s=%q want=%q", key, got, want)
					}
				}
			}
			if found != test.snapshot {
				t.Fatalf("snapshot=%v events=%#v", found, events)
			}
			if checks != 1 {
				t.Fatalf("rotation checkpoints=%d, want one per checked source", checks)
			}
		})
	}
}

func TestYouTubeAPIAndYTDLPDoNotConfuseTransientFailuresWithWithdrawal(t *testing.T) {
	for _, title := range []string{"youtube video #qLZmigdY7wg", "YouTube video qLZmigdY7wg", "YouTube"} {
		if !isBadYouTubeTitleValue(title) {
			t.Fatalf("placeholder title %q accepted", title)
		}
	}
	if isBadYouTubeTitleValue("YouTube video editing with R charts") {
		t.Fatal("real title rejected as a placeholder")
	}
	for _, state := range []string{"deleted", "failed", "rejected"} {
		if _, ok := youtubeUnavailableReason(youtubeAPIAvailabilityError(map[string]any{"uploadStatus": state})); !ok {
			t.Fatalf("uploadStatus %s not withdrawn", state)
		}
	}
	if err := youtubeAPIAvailabilityError(map[string]any{"privacyStatus": "unlisted", "uploadStatus": "processed", "embeddable": false}); err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{"HTTP Error 403: Forbidden", "Sign in to confirm you're not a bot", "Video unavailable", "Unable to download API page: timed out"} {
		if err := youtubeYTDLPAvailabilityError(message); err != nil {
			t.Fatalf("%q classified withdrawn: %v", message, err)
		}
	}
	for _, message := range []string{"ERROR: Private video. Sign in", "This video has been removed by the uploader"} {
		if _, ok := youtubeUnavailableReason(youtubeYTDLPAvailabilityError(message)); !ok {
			t.Fatalf("%s not withdrawn", fmt.Sprintf("%q", message))
		}
	}
}
