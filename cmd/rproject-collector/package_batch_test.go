package main

import (
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestPackagePageBatchIndexesContinuesAfterCursor(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}

	got := packagePageBatchIndexes(keys, "b", 2)
	want := []int{2, 3}
	if !sameInts(got, want) {
		t.Fatalf("after b: got %v want %v", got, want)
	}

	got = packagePageBatchIndexes(keys, "d", 2)
	want = []int{0, 1}
	if !sameInts(got, want) {
		t.Fatalf("wrap after d: got %v want %v", got, want)
	}
}

func TestSelectPackagePageRecordsUsesForcedThenStableBatch(t *testing.T) {
	t.Setenv("RPKG_PACKAGE_PAGE_CURSOR_MODE", "off")

	records := []cranRecord{
		{"Package": "d", "Version": "1.0.0"},
		{"Package": "a", "Version": "1.0.0"},
		{"Package": "b", "Version": "1.0.0"},
		{"Package": "c", "Version": "1.0.0"},
	}

	selected, batch := selectPackagePageRecords(records, 3, []string{"c"}, "test-source")
	got := packageNames(selected)
	want := []string{"c", "a", "b"}
	if !sameStrings(got, want) {
		t.Fatalf("selected packages got %v want %v", got, want)
	}
	if batch.ForcedCount != 1 || batch.SelectedCount != 3 || batch.NextCursorKey != "b" {
		t.Fatalf("batch metadata = %+v", batch)
	}
}

func TestFixedPartitionBalancerUsesRequestedPartition(t *testing.T) {
	balancer := fixedPartitionBalancer{partition: 7}
	if got := balancer.Balance(kafka.Message{}, 0, 3, 7, 9); got != 7 {
		t.Fatalf("fixed partition = %d, want 7", got)
	}
	if got := balancer.Balance(kafka.Message{}, 1, 2, 3); got != 1 {
		t.Fatalf("fallback partition = %d, want first available partition", got)
	}
}

func TestSplitIntCSVSortsAndDeduplicates(t *testing.T) {
	got := splitIntCSV("5, 1, bad, -1, 5, 3")
	want := []int{1, 3, 5}
	if !sameInts(got, want) {
		t.Fatalf("splitIntCSV got %v want %v", got, want)
	}
}

func TestShouldUsePartitionFallbackForLeaderMetadataErrors(t *testing.T) {
	err := errors.New("[6] Not Leader For Partition: metadata are likely out of date")
	if !shouldUsePartitionFallback(err) {
		t.Fatal("expected leader metadata error to use fixed partition fallback")
	}
	if shouldUsePartitionFallback(errors.New("connection reset by peer")) {
		t.Fatal("network errors should use normal retry path")
	}
}

func packageNames(records []cranRecord) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, record["Package"])
	}
	return out
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
