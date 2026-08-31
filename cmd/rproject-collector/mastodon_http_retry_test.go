package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type mastodonHTTPDoerFunc func(*http.Request) (*http.Response, error)

func (f mastodonHTTPDoerFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

type mastodonRetryableNetworkError struct{}

func (mastodonRetryableNetworkError) Error() string   { return "temporary network failure" }
func (mastodonRetryableNetworkError) Timeout() bool   { return true }
func (mastodonRetryableNetworkError) Temporary() bool { return true }

func mastodonTestResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestFetchMastodonBytesRetriesOnlyTransientHTTPStatuses(t *testing.T) {
	statuses := []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusOK}
	attempts := 0
	delays := make([]time.Duration, 0, len(statuses)-1)
	doer := mastodonHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		status := statuses[attempts]
		attempts++
		return mastodonTestResponse(status, "ok"), nil
	})

	body, err := fetchMastodonBytesWithRetry(
		context.Background(),
		doer,
		"https://example.test/@R_Foundation.rss",
		len(statuses),
		time.Second,
		10*time.Second,
		func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("fetchMastodonBytesWithRetry error = %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body = %q, want ok", string(body))
	}
	if attempts != len(statuses) {
		t.Fatalf("attempts = %d, want %d", attempts, len(statuses))
	}
	wantDelays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	if len(delays) != len(wantDelays) {
		t.Fatalf("delays = %v, want %v", delays, wantDelays)
	}
	for i := range wantDelays {
		if delays[i] != wantDelays[i] {
			t.Fatalf("delays[%d] = %s, want %s", i, delays[i], wantDelays[i])
		}
	}
}

func TestFetchMastodonBytesRetriesNetworkErrors(t *testing.T) {
	attempts := 0
	doer := mastodonHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, mastodonRetryableNetworkError{}
		}
		return mastodonTestResponse(http.StatusOK, "rss"), nil
	})

	body, err := fetchMastodonBytesWithRetry(
		context.Background(), doer, "https://example.test/feed", 2, 0, 0, nil,
	)
	if err != nil {
		t.Fatalf("fetchMastodonBytesWithRetry error = %v", err)
	}
	if string(body) != "rss" || attempts != 2 {
		t.Fatalf("body=%q attempts=%d, want rss and 2", string(body), attempts)
	}
}

func TestFetchMastodonBytesStopsAfterBoundedAttempts(t *testing.T) {
	attempts := 0
	doer := mastodonHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return mastodonTestResponse(http.StatusServiceUnavailable, "temporary"), nil
	})

	_, err := fetchMastodonBytesWithRetry(
		context.Background(), doer, "https://example.test/feed", 3, 0, 0, nil,
	)
	var statusErr *mastodonHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.statusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %v, want Mastodon HTTP status %d", err, http.StatusServiceUnavailable)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestFetchMastodonBytesFailsFastForPermanentHTTP4xx(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			attempts := 0
			doer := mastodonHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
				attempts++
				return mastodonTestResponse(status, "permanent"), nil
			})
			_, err := fetchMastodonBytesWithRetry(
				context.Background(), doer, "https://example.test/feed", 4, 0, 0, nil,
			)
			var statusErr *mastodonHTTPStatusError
			if !errors.As(err, &statusErr) || statusErr.statusCode != status {
				t.Fatalf("error = %v, want Mastodon HTTP status %d", err, status)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
		})
	}
}

func TestFetchMastodonBytesFailsFastForNonNetworkError(t *testing.T) {
	attempts := 0
	wantErr := errors.New("invalid transport contract")
	doer := mastodonHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return nil, wantErr
	})
	_, err := fetchMastodonBytesWithRetry(
		context.Background(), doer, "https://example.test/feed", 4, 0, 0, nil,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestParseMastodonRSSFailsWithoutHTTPRetry(t *testing.T) {
	attempts := 0
	doer := mastodonHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return mastodonTestResponse(http.StatusOK, "<rss><broken>"), nil
	})
	body, err := fetchMastodonBytesWithRetry(
		context.Background(), doer, "https://example.test/feed", 4, 0, 0, nil,
	)
	if err != nil {
		t.Fatalf("fetchMastodonBytesWithRetry error = %v", err)
	}
	if _, err := parseMastodonRSS(body); err == nil {
		t.Fatal("parseMastodonRSS succeeded for malformed XML")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestFetchMastodonJSONFailsParsingWithoutHTTPRetry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{")
	}))
	defer server.Close()
	t.Setenv("MASTODON_HTTP_ATTEMPTS", "4")
	t.Setenv("MASTODON_HTTP_RETRY_DELAY_SECONDS", "0")

	var out map[string]any
	err := fetchMastodonJSON(context.Background(), server.URL, &out)
	if err == nil || !strings.Contains(err.Error(), "decode Mastodon JSON") {
		t.Fatalf("error = %v, want Mastodon JSON decode error", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRetryableMastodonHTTPErrorRejectsMalformedURLContract(t *testing.T) {
	err := &url.Error{Op: http.MethodGet, URL: "://bad", Err: errors.New("unsupported protocol scheme")}
	if retryableMastodonHTTPError(err) {
		t.Fatalf("malformed URL error must not be retried: %v", err)
	}
}
