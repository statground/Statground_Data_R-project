package main

import (
	"errors"
	"strings"
	"testing"
)

func TestRbloggerClickHouseNotInitializedClassification(t *testing.T) {
	err := errors.New("clickhouse statement failed HTTP 500: Code: 667. DB::Exception: Table is not initialized yet. (NOT_INITIALIZED)")
	if !retryableClickHouseStatementError(err) {
		t.Fatal("NOT_INITIALIZED should be retried")
	}
	if splittableClickHouseStatementError(err) {
		t.Fatal("NOT_INITIALIZED should not be split into smaller chunks")
	}
	if got := publicClickHouseStatementError(err); got != "clickhouse-not-initialized" {
		t.Fatalf("publicClickHouseStatementError = %q, want clickhouse-not-initialized", got)
	}
}

func TestRbloggerPublishFailureDefersOnlyTransientErrors(t *testing.T) {
	transient := errors.New("ClickHouse R-bloggers direct publish failed target=raw: clickhouse-not-initialized")
	if !shouldDeferRbloggerPublishFailure(transient) {
		t.Fatal("R-bloggers transient ClickHouse initialization failures should be deferred by default")
	}
	t.Setenv("RBLOGGER_PUBLISH_TRANSIENT_FAIL_OPEN", "false")
	if shouldDeferRbloggerPublishFailure(transient) {
		t.Fatal("R-bloggers transient deferral should respect opt-out env")
	}
	t.Setenv("RBLOGGER_PUBLISH_TRANSIENT_FAIL_OPEN", "true")
	permission := errors.New("ClickHouse R-bloggers direct publish failed target=raw: clickhouse-permission")
	if shouldDeferRbloggerPublishFailure(permission) {
		t.Fatal("permission errors must remain fatal")
	}
}

func TestRbloggerInsertRowsStatementUsesAsyncDeduplicatedInsert(t *testing.T) {
	query, err := rbloggerInsertRowsStatement("Data_R_Community_Raw.r_blogger_article_raw", []map[string]any{{"uuid": "u1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "insert_distributed_sync = 0") {
		t.Fatalf("insert should not wait for distributed sync by default: %s", query)
	}
	if !strings.Contains(query, "insert_deduplicate = 1") {
		t.Fatalf("insert should request deduplication: %s", query)
	}

	t.Setenv("RPROJECT_CLICKHOUSE_INSERT_DISTRIBUTED_SYNC", "true")
	query, err = rbloggerInsertRowsStatement("Data_R_Community_Raw.r_blogger_article_raw", []map[string]any{{"uuid": "u1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "insert_distributed_sync = 1") {
		t.Fatalf("explicit distributed sync setting not reflected: %s", query)
	}
}
