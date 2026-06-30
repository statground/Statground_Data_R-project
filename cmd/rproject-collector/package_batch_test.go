package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaTimeoutError struct{}

func (kafkaTimeoutError) Error() string {
	return "kafka.(*Client).Produce: dial tcp 203.0.113.10:9092: i/o timeout"
}
func (kafkaTimeoutError) Timeout() bool   { return true }
func (kafkaTimeoutError) Temporary() bool { return true }

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

func TestFallbackPartitionIDsUsesCachedMetadata(t *testing.T) {
	pub := &publisher{
		topic:           "rpkg.events",
		knownPartitions: []int{2, 0, 2, 1},
	}
	got, err := pub.fallbackPartitionIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []int{0, 1, 2}
	if !sameInts(got, want) {
		t.Fatalf("fallbackPartitionIDs got %v want %v", got, want)
	}
}

func TestPartitionIDsForTopicFiltersAndSorts(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "other.events", ID: 9},
		{Topic: "rpkg.events", ID: 2},
		{Topic: "rpkg.events", ID: 0},
		{Topic: "rpkg.events", ID: 2},
	}
	got := partitionIDsForTopic(partitions, "rpkg.events")
	want := []int{0, 2}
	if !sameInts(got, want) {
		t.Fatalf("partitionIDsForTopic got %v want %v", got, want)
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

func TestShouldTryPartitionFallbackOnlyAfterNormalRetries(t *testing.T) {
	err := errors.New("[6] Not Leader For Partition: metadata are likely out of date")
	if shouldTryPartitionFallback(err, 1, 3, true) {
		t.Fatal("fixed partition fallback should wait until normal writer retries are exhausted")
	}
	if !shouldTryPartitionFallback(err, 3, 3, true) {
		t.Fatal("fixed partition fallback should run on the final retryable leader error")
	}
	if shouldTryPartitionFallback(err, 3, 3, false) {
		t.Fatal("disabled partition fallback must stay disabled")
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

func TestRetryableFailedMessagesExtractsPartialKafkaWriteErrors(t *testing.T) {
	messages := []kafka.Message{
		{Key: []byte("ok-1")},
		{Key: []byte("failed-1")},
		{Key: []byte("ok-2")},
		{Key: []byte("failed-2")},
	}
	failed, retryable := retryableFailedMessages(messages, kafka.WriteErrors{
		nil,
		kafkaTimeoutError{},
		nil,
		kafkaTimeoutError{},
	})
	if !retryable {
		t.Fatal("expected i/o timeout write errors to be retryable")
	}
	if len(failed) != 2 {
		t.Fatalf("failed message count = %d, want 2", len(failed))
	}
	if string(failed[0].Key) != "failed-1" || string(failed[1].Key) != "failed-2" {
		t.Fatalf("failed messages = %q, %q; want failed-1, failed-2", failed[0].Key, failed[1].Key)
	}
}

func TestRetryableKafkaWriteErrorTreatsAttemptDeadlineAsRetryable(t *testing.T) {
	if !retryableKafkaWriteError(context.DeadlineExceeded) {
		t.Fatal("per-attempt deadline should be retryable")
	}
	if retryableKafkaWriteError(context.Canceled) {
		t.Fatal("canceled context should not be retried")
	}
}

func TestFailedPackageEventsFromKafkaErrorUsesOnlyFailedMessages(t *testing.T) {
	events := []genericEvent{
		{
			EventID:       "0197b9b4-90c0-7000-8000-000000000001",
			EventType:     "rpkg.cran.reverse_dependency_edge.v1",
			SchemaVersion: 1,
			Repository:    "CRAN",
			PackageName:   "A3",
			PayloadHash:   "hash-a3",
			Payload:       `{"package":"A3"}`,
		},
		{
			EventID:       "0197b9b4-90c0-7000-8000-000000000002",
			EventType:     "rpkg.cran.reverse_dependency_edge.v1",
			SchemaVersion: 1,
			Repository:    "CRAN",
			PackageName:   "A4",
			PayloadHash:   "hash-a4",
			Payload:       `{"package":"A4"}`,
		},
	}
	messages, err := genericEventsToMessages(events)
	if err != nil {
		t.Fatal(err)
	}
	publishErr := newKafkaPublishError(kafka.WriteErrors{nil, kafkaTimeoutError{}}, []kafka.Message{messages[1]})
	failedEvents, err := failedPackageEventsFromKafkaError(publishErr)
	if err != nil {
		t.Fatal(err)
	}
	if len(failedEvents) != 1 || failedEvents[0].EventID != events[1].EventID {
		t.Fatalf("failed events = %+v, want only second event", failedEvents)
	}
}

func TestRetryableClickHouseFallbackError(t *testing.T) {
	if !retryableClickHouseFallbackError(errors.New("ClickHouse HTTP 500: timeout exceeded")) {
		t.Fatal("expected ClickHouse HTTP 5xx timeout to be retryable")
	}
	if retryableClickHouseFallbackError(errors.New("ClickHouse HTTP 403: not enough privileges")) {
		t.Fatal("permission errors should not be retried")
	}
}

func TestPackageRawEventInsertPrefixDefaultsToAsyncDistributedInsert(t *testing.T) {
	got := packageRawEventInsertPrefix(clickHouseQueryConfig{})
	if !strings.Contains(got, "insert_distributed_sync = 0") {
		t.Fatalf("fallback insert should not wait for distributed sync by default: %s", got)
	}
	if !strings.Contains(got, "insert_deduplicate = 1") {
		t.Fatalf("fallback insert should request insert deduplication: %s", got)
	}

	got = packageRawEventInsertPrefix(clickHouseQueryConfig{InsertDistributedSync: true})
	if !strings.Contains(got, "insert_distributed_sync = 1") {
		t.Fatalf("explicit distributed sync setting not reflected: %s", got)
	}
}

func TestShouldDeferPackagePublishFailureOnlyForTransientErrors(t *testing.T) {
	t.Setenv("RPKG_PUBLISH_TRANSIENT_FAIL_OPEN", "true")
	transient := errors.New("kafka publish failed and ClickHouse package raw fallback failed: clickhouse-timeout; original_error=kafka publish failed after fixed-partition fallback: leader not available")
	if !shouldDeferPackagePublishFailure(transient) {
		t.Fatal("transient Kafka plus ClickHouse timeout should be deferred")
	}
	auth := errors.New("kafka publish failed: SASL authentication failed: invalid credentials")
	if shouldDeferPackagePublishFailure(auth) {
		t.Fatal("auth errors must remain fatal")
	}
}

func TestKafkaAuthErrorIsNotRetryableUnlessNetworkHandshakeFailed(t *testing.T) {
	if retryableKafkaWriteError(errors.New("SASL authentication failed: invalid credentials")) {
		t.Fatal("bad credentials should not be retried")
	}
	if !retryableKafkaWriteError(errors.New("SASL handshake failed: read tcp 10.1.0.222:59538->203.0.113.10:9092: connection reset by peer")) {
		t.Fatal("network failure during SASL handshake should be retryable")
	}
}

func TestShortKafkaErrorSanitizesIPAddresses(t *testing.T) {
	err := errors.New("SASL handshake failed: read tcp 10.1.0.222:59538->203.0.113.10:9092: connection reset by peer")
	got := shortKafkaError(err)
	if strings.Contains(got, "10.1.0.222") || strings.Contains(got, "203.0.113.10") {
		t.Fatalf("shortKafkaError leaked IP address: %s", got)
	}
	if !strings.Contains(got, "[ip]") {
		t.Fatalf("shortKafkaError should keep a sanitized endpoint marker: %s", got)
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

func TestNormalizePublishModeDefaultsToClickHouse(t *testing.T) {
	cases := map[string]string{
		"":                 "clickhouse",
		"db":               "clickhouse",
		"direct":           "clickhouse",
		"kafka":            "kafka",
		"dual":             "dual",
		"clickhouse+kafka": "dual",
		"unexpected":       "clickhouse",
	}
	for input, want := range cases {
		if got := normalizePublishMode(input); got != want {
			t.Fatalf("normalizePublishMode(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestPublisherValidateSkipsKafkaInClickHouseMode(t *testing.T) {
	pub := &publisher{publishMode: "clickhouse"}
	if err := pub.validate(context.Background()); err != nil {
		t.Fatalf("clickhouse publish mode should not require Kafka config: %v", err)
	}
}

func TestGenericEventDirectTargetRoutesSupportedFamilies(t *testing.T) {
	cases := []struct {
		eventType      string
		table          string
		includePackage bool
	}{
		{"rpkg.cran.package_snapshot.v1", "Data_R_Package_Raw.r_package_event_raw", true},
		{"r.youtube.video.snapshot.v1", "Data_R_Community_Raw.r_youtube_event_raw", true},
		{"r.community.item.v1", "Data_R_Community_Raw.r_community_event_raw", false},
	}
	for _, tc := range cases {
		got, err := genericEventDirectTarget(genericEvent{EventType: tc.eventType})
		if err != nil {
			t.Fatalf("genericEventDirectTarget(%q) returned error: %v", tc.eventType, err)
		}
		if got.table != tc.table || got.includePackage != tc.includePackage {
			t.Fatalf("genericEventDirectTarget(%q) = %+v, want table=%s includePackage=%t", tc.eventType, got, tc.table, tc.includePackage)
		}
	}
}

func TestGenericEventsDirectTargetRejectsMixedTables(t *testing.T) {
	_, err := genericEventsDirectTarget([]genericEvent{
		{EventType: "rpkg.cran.package_snapshot.v1"},
		{EventType: "r.community.item.v1"},
	})
	if err == nil {
		t.Fatal("mixed direct ClickHouse targets should fail instead of inserting into the wrong raw table")
	}
}

func TestGenericRawEventDirectRowOmitsPackageFieldsForCommunity(t *testing.T) {
	event := genericEvent{
		EventID:       "0197b9b4-90c0-7000-8000-000000000001",
		EventType:     "r.community.item.v1",
		SchemaVersion: 1,
		Source:        "R-Community",
		SourceURL:     "https://example.com/item",
		Repository:    "R-Community",
		ObservedAt:    "2026-06-30T00:00:00.000Z",
		CollectedAt:   "2026-06-30T01:00:00.000Z",
		PayloadHash:   "abc123",
		Payload:       `{"source_id":"community:test"}`,
	}
	row := genericRawEventDirectRow(event, time.Date(2026, 6, 30, 2, 0, 0, 0, time.UTC), false)
	if _, ok := row["package_name"]; ok {
		t.Fatal("community raw events must not include package_name")
	}
	if row["uuid"] != event.EventID || row["event_type"] != event.EventType {
		t.Fatalf("unexpected direct row identity: %+v", row)
	}
	if row["ingested_at"] != "2026-06-30 11:00:00.000" {
		t.Fatalf("ingested_at = %v", row["ingested_at"])
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
