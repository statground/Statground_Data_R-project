package main

import "testing"

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
