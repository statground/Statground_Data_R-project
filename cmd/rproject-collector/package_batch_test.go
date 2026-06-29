package main

import (
	"errors"
	"testing"
	"time"

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

func TestPackageClickHouseFallbackOnlyForRPackageEvents(t *testing.T) {
	t.Setenv("RPKG_CLICKHOUSE_FALLBACK_ENABLED", "true")
	pub := &publisher{}
	if !pub.packageClickHouseFallbackEnabled([]genericEvent{{EventType: "rpkg.cran.package_snapshot.v1"}}) {
		t.Fatal("expected rpkg event to allow ClickHouse fallback")
	}
	if pub.packageClickHouseFallbackEnabled([]genericEvent{{EventType: "r.youtube.video.snapshot.v1"}}) {
		t.Fatal("non-rpkg generic events must not use package ClickHouse fallback")
	}
	pub.dryRun = true
	if pub.packageClickHouseFallbackEnabled([]genericEvent{{EventType: "rpkg.cran.package_snapshot.v1"}}) {
		t.Fatal("dry-run should not use ClickHouse fallback")
	}
}

func TestShouldUsePackageClickHouseFallbackRequiresWholeChunkFailure(t *testing.T) {
	allFailed := errors.New("kafka fixed partition fallback exhausted partitions=[0 1 2] failed_messages=100 last_error=Kafka write errors (100/100), errors: [[6] Not Leader For Partition]")
	if !shouldUsePackageClickHouseFallback(allFailed, 100) {
		t.Fatal("expected whole-chunk leader failure to allow ClickHouse fallback")
	}
	partialFailed := errors.New("kafka fixed partition fallback exhausted partitions=[0 1 2] failed_messages=3 last_error=Kafka write errors (3/100), errors: [[6] Not Leader For Partition]")
	if shouldUsePackageClickHouseFallback(partialFailed, 100) {
		t.Fatal("partial Kafka writes must not be blindly duplicated into ClickHouse")
	}
	otherCount := errors.New("kafka fixed partition fallback exhausted partitions=[0 1 2] failed_messages=1000 last_error=Kafka write errors (1000/1000), errors: [[6] Not Leader For Partition]")
	if shouldUsePackageClickHouseFallback(otherCount, 100) {
		t.Fatal("failed_messages=1000 must not be treated as failed_messages=100")
	}
}

func TestPackageRawEventFallbackRowFormatsDateTimes(t *testing.T) {
	event := genericEvent{
		EventID:        "0197b9b4-90c0-7000-8000-000000000001",
		EventType:      "rpkg.cran.package_snapshot.v1",
		SchemaVersion:  1,
		Source:         "cran_metadata",
		SourceURL:      "https://cran.r-project.org/web/packages/A3/index.html",
		Repository:     "CRAN",
		PackageName:    "A3",
		PackageVersion: "1.0.0",
		ObservedAt:     "2026-06-30T00:00:00.000Z",
		CollectedAt:    "2026-06-30T01:00:00.000Z",
		PayloadHash:    "abc123",
		Payload:        `{"Package":"A3"}`,
	}
	row := packageRawEventFallbackRow(event, time.Date(2026, 6, 30, 2, 0, 0, 0, time.UTC))
	if row["observed_at"] != "2026-06-30 09:00:00.000" {
		t.Fatalf("observed_at = %v", row["observed_at"])
	}
	if row["collected_at"] != "2026-06-30 10:00:00.000" {
		t.Fatalf("collected_at = %v", row["collected_at"])
	}
	if row["ingested_at"] != "2026-06-30 11:00:00.000" {
		t.Fatalf("ingested_at = %v", row["ingested_at"])
	}
}

func TestPublicClickHouseErrorSanitizesDetails(t *testing.T) {
	err := errors.New("ClickHouse HTTP 500: DB::Exception: table Data_R_Package_Raw.r_package_event_raw does not exist")
	if got := publicClickHouseError(err); got != "clickhouse-server-error" {
		t.Fatalf("publicClickHouseError = %q", got)
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
