package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// This type is reserved for an upstream response that confirms withdrawal.
// Network, quota, authentication, captcha, and incomplete-metadata failures
// must remain ordinary errors so refresh does not withdraw a verified video.
type youtubeVideoUnavailableError struct {
	reason string
}

func (e youtubeVideoUnavailableError) Error() string { return "youtube_video_unavailable: " + e.reason }

func youtubeUnavailableReason(err error) (string, bool) {
	var unavailable youtubeVideoUnavailableError
	if !errors.As(err, &unavailable) {
		return "", false
	}
	return unavailable.reason, true
}

func youtubeAvailabilityCheckEvent(videoID, canonicalURL, status, reason string) genericEvent {
	return newGenericEvent("r.youtube.availability.check.v1", "youtube_availability_refresh", canonicalURL, "R-YouTube", "", "", "", map[string]any{
		"youtube_video_id":    videoID,
		"source_method":       "youtube_availability_refresh",
		"collection_status":   status,
		"availability_reason": reason,
		"checked_at":          time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	})
}

func youtubeAPIAvailabilityError(status map[string]any) error {
	if stringAny(status["privacyStatus"]) == "private" {
		return youtubeVideoUnavailableError{reason: "api_private"}
	}
	switch stringAny(status["uploadStatus"]) {
	case "deleted", "failed", "rejected":
		return youtubeVideoUnavailableError{reason: "api_upload_" + stringAny(status["uploadStatus"])}
	}
	return nil
}

func youtubeYTDLPAvailabilityError(message string) error {
	lower := strings.ToLower(message)
	for _, marker := range []string{"private video", "this video is private"} {
		if strings.Contains(lower, marker) {
			return youtubeVideoUnavailableError{reason: "ytdlp_private"}
		}
	}
	for _, marker := range []string{"this video has been removed", "video has been removed", "this video was removed", "video has been deleted", "this video is no longer available"} {
		if strings.Contains(lower, marker) {
			return youtubeVideoUnavailableError{reason: "ytdlp_removed"}
		}
	}
	return nil
}

var youtubeInitialPlayerResponseRE = regexp.MustCompile(`(?:var\s+)?ytInitialPlayerResponse\s*=\s*`)
var youtubeVideoIDRE = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)
var youtubePlaceholderTitleRE = regexp.MustCompile(`(?i)^youtube video\s+#?[A-Za-z0-9_-]{11}$`)

func fetchYouTubeWatchVideoPayload(videoID, canonicalURL string) (map[string]any, error) {
	// The availability authority is the official watch host, not seed URLs.
	query := url.Values{}
	query.Set("v", videoID)
	query.Set("hl", "en")
	body, err := fetchBytes("https://www.youtube.com/watch?" + query.Encode())
	if err != nil {
		return nil, err
	}
	return youtubeWatchVideoPayload(videoID, string(body))
}

func youtubeWatchVideoPayload(videoID, body string) (map[string]any, error) {
	location := youtubeInitialPlayerResponseRE.FindStringIndex(body)
	if location == nil {
		return nil, errors.New("watch page has no initial player response")
	}
	var player map[string]any
	if err := json.NewDecoder(strings.NewReader(body[location[1]:])).Decode(&player); err != nil {
		return nil, errors.New("watch page initial player response is invalid")
	}
	playability := mapAny(player["playabilityStatus"])
	status := stringAny(playability["status"])
	reason := strings.ToLower(stringAny(playability["reason"]))
	if status != "OK" {
		// LOGIN_REQUIRED also covers age checks and bot challenges. Only the
		// explicit private/removed reason is a durable withdrawal signal.
		if status == "LOGIN_REQUIRED" || status == "ERROR" || status == "UNPLAYABLE" {
			if strings.Contains(reason, "private video") || strings.Contains(reason, "비공개 동영상") {
				return nil, youtubeVideoUnavailableError{reason: "watch_private"}
			}
			if strings.Contains(reason, "this video has been removed") || strings.Contains(reason, "video has been deleted") || strings.Contains(reason, "this video is no longer available") {
				return nil, youtubeVideoUnavailableError{reason: "watch_removed"}
			}
		}
		return nil, errors.New("watch playability could not be verified")
	}
	details := mapAny(player["videoDetails"])
	if stringAny(details["videoId"]) != videoID || isBadYouTubeTitleValue(details["title"]) {
		return nil, errors.New("watch page has no usable matching video details")
	}
	thumbnails := anySlice(mapAny(details["thumbnail"])["thumbnails"])
	microformat := mapAny(mapAny(player["microformat"])["playerMicroformatRenderer"])
	return map[string]any{
		"youtube_video_id":   videoID,
		"youtube_channel_id": stringAny(details["channelId"]),
		"video_title":        stringAny(details["title"]),
		"video_description":  stringAny(details["shortDescription"]),
		"thumbnail_url":      bestYTDLPThumbnail(thumbnails),
		"published_at":       firstNonEmpty(stringAny(microformat["publishDate"]), stringAny(microformat["uploadDate"])),
		"duration_seconds":   intString(details["lengthSeconds"]),
		"view_count":         intString(details["viewCount"]),
		"channel_title":      stringAny(details["author"]),
		"privacy_status":     "public",
		"canonical_url":      "https://www.youtube.com/watch?v=" + videoID,
	}, nil
}
