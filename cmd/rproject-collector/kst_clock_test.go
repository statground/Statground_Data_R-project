package main

import (
	"testing"
	"time"
)

func TestNowKSTPreservesCurrentInstantAndOffset(t *testing.T) {
	before := time.Now()
	now := nowKST()
	after := time.Now()
	if now.Before(before) || now.After(after) {
		t.Errorf("nowKST instant %s falls outside current time [%s, %s]", now, before, after)
	}
	if _, offset := now.Zone(); offset != 9*3600 {
		t.Errorf("nowKST UTC offset = %d, want +09:00", offset)
	}
}

func TestFormatNowKSTMatchesCurrentKoreanTime(t *testing.T) {
	before := time.Now().Truncate(time.Millisecond)
	formatted := formatKST(nowKST())
	after := time.Now().Truncate(time.Millisecond)
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.000", formatted, time.FixedZone("KST", 9*3600))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Before(before) || parsed.After(after) {
		t.Fatalf("formatted KST timestamp %q represents %s, outside current time [%s, %s]", formatted, parsed, before, after)
	}
}

func TestFormatKSTPreservesInstantAcrossMidnight(t *testing.T) {
	for _, tt := range []struct {
		utc  string
		want string
	}{
		{"2026-10-03T14:59:59.999Z", "2026-10-03 23:59:59.999"},
		{"2026-10-03T15:00:00.000Z", "2026-10-04 00:00:00.000"},
	} {
		instant, err := time.Parse(time.RFC3339Nano, tt.utc)
		if err != nil {
			t.Fatal(err)
		}
		for _, input := range []time.Time{instant, instant.In(time.FixedZone("KST", 9*3600))} {
			if got := formatKST(input); got != tt.want {
				t.Errorf("formatKST(%s) = %q, want %q", input, got, tt.want)
			}
		}
	}
}
