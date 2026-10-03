// Canonical SQL-owned client; consumer copies must remain byte-identical.
package lexicon

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Candidate struct {
	Keyword        string   `json:"keyword"`
	NormalizedWord string   `json:"normalized_word"`
	Language       string   `json:"language"`
	Sources        []string `json:"sources"`
	Confidence     float64  `json:"confidence"`
	SnapshotID     string   `json:"snapshot_id"`
	Scope          string   `json:"scope"`
}
type Selection struct {
	Keyword        string   `json:"keyword"`
	NormalizedWord string   `json:"normalized_word"`
	Language       string   `json:"language"`
	Origin         string   `json:"origin"`
	SnapshotID     string   `json:"snapshot_id"`
	RunID          string   `json:"run_id"`
	Scope          string   `json:"scope"`
	Sources        []string `json:"sources"`
	SelectedAt     string   `json:"selected_at"`
	Confidence     float64  `json:"confidence"`
}

var idOnce sync.Once
var processID string
var safeTag = regexp.MustCompile("^[a-z][a-z0-9_-]{0,47}$")
var snapshotTag = regexp.MustCompile("^[a-f0-9]{64}$")
var pendingTag = regexp.MustCompile("^pending-[0-9]{20}-[a-f0-9]{64}\\.json$")

func first(names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v
		}
	}
	return ""
}
func RunID() string {
	if id := first("LEXICON_RUN_ID", "GITHUB_RUN_ID"); id != "" {
		return id
	}
	idOnce.Do(func() {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			panic("keyword_run_entropy_unavailable")
		}
		processID = hex.EncodeToString(b)
	})
	return processID
}
func validScope(s string) bool {
	return s == "kakao-book" || s == "gmarket" || s == "kurly" || s == "r-youtube"
}
func literal(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "\\'") + "'"
}
func key(s string) string    { return strings.ToLower(strings.TrimSpace(s)) }
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func validCandidate(c Candidate) bool {
	if c.Language == "" || c.Language == "und" || math.IsNaN(c.Confidence) || math.IsInf(c.Confidence, 0) || c.Confidence < .65 || c.Confidence > 1 || !snapshotTag.MatchString(c.SnapshotID) || len(c.Sources) == 0 || c.NormalizedWord != key(c.Keyword) {
		return false
	}
	if len([]rune(c.Keyword)) < 2 || len([]rune(c.Keyword)) > 64 {
		return false
	}
	for _, r := range c.Keyword {
		if !unicode.IsLetter(r) && !unicode.IsMark(r) {
			return false
		}
	}
	return true
}

type connection struct{ endpoint, user, password string }

func connect() (connection, error) {
	c := connection{endpoint: first("LEXICON_CLICKHOUSE_HTTP_URL"), user: first("LEXICON_CLICKHOUSE_USER", "CLICKHOUSE_USER", "CH_USER"), password: first("LEXICON_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD", "CH_PASSWORD")}
	if c.endpoint == "" {
		host := first("CLICKHOUSE_HOST", "CH_HOST")
		port := first("CLICKHOUSE_HTTP_PORT", "CH_HTTP_PORT", "CLICKHOUSE_PORT", "CH_PORT")
		proto := first("CLICKHOUSE_PROTOCOL", "CH_PROTOCOL")
		if proto == "" && strings.EqualFold(first("CLICKHOUSE_SECURE", "CH_SECURE"), "true") {
			proto = "https"
		}
		if proto == "" {
			proto = "http"
		}
		if strings.HasPrefix(host, "https://") || strings.HasPrefix(host, "http://") {
			c.endpoint = host
		} else if host != "" && port != "" {
			c.endpoint = proto + "://" + host + ":" + port + "/"
		}
	}
	if c.endpoint == "" {
		if raw := first("CLICKHOUSE_DSN"); raw != "" {
			u, e := url.Parse(raw)
			if e == nil && (u.Scheme == "http" || u.Scheme == "https") {
				if u.User != nil {
					c.user = u.User.Username()
					c.password, _ = u.User.Password()
					u.User = nil
				}
				c.endpoint = u.String()
			}
		}
	}
	u, e := url.Parse(c.endpoint)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || c.user == "" {
		return c, errors.New("keyword_reader_config_missing")
	}
	if path := first("CLICKHOUSE_HTTP_URL_PATH", "CH_HTTP_URL_PATH"); path != "" && (u.Path == "" || u.Path == "/") {
		u.Path = path
		c.endpoint = u.String()
	}
	return c, nil
}
func request(ctx context.Context, sql string, body []byte, token string) ([]byte, error) {
	c, e := connect()
	if e != nil {
		return nil, e
	}
	u, _ := url.Parse(c.endpoint)
	q := u.Query()
	q.Set("max_execution_time", "10")
	q.Set("max_threads", "1")
	q.Set("max_memory_usage", "268435456")
	q.Set("wait_end_of_query", "1")
	q.Set("skip_unavailable_shards", "0")
	if body != nil {
		q.Set("query", sql)
		q.Set("distributed_foreground_insert", "1")
		q.Set("async_insert", "0")
		q.Set("insert_quorum", "auto")
		q.Set("insert_quorum_parallel", "0")
		q.Set("insert_deduplication_token", token)
	} else {
		body = []byte(sql)
	}
	u.RawQuery = q.Encode()
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if e != nil {
		return nil, errors.New("keyword_reader_request_invalid")
	}
	req.SetBasicAuth(c.user, c.password)
	client := &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, e := client.Do(req)
	if e != nil {
		return nil, errors.New("keyword_reader_unavailable")
	}
	defer response.Body.Close()
	data, e := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024+1))
	if e != nil || len(data) > 8*1024*1024 {
		return nil, errors.New("keyword_reader_response_limit")
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("keyword_reader_http_%d", response.StatusCode)
	}
	return data, nil
}
func FromEnv(ctx context.Context, scope string) ([]Candidate, string, error) {
	if !validScope(scope) {
		return nil, "", errors.New("keyword_scope_invalid")
	}
	var pool []Candidate
	// Resume an acknowledged plan before requiring a newer pool generation.
	if path, e := directory(); e == nil {
		if data, e := os.ReadFile(filepath.Join(path, digest([]byte(scope+"\x00"+RunID()))+".json")); e == nil {
			var prior receipt
			if json.Unmarshal(data, &prior) != nil || prior.Scope != scope || prior.RunID != RunID() || prior.Count < 2 || prior.Count > 200 || len(prior.Selections) > prior.Count || !validateSelections(prior.Selections, scope, RunID()) {
				return nil, "", errors.New("keyword_receipt_conflict")
			}
			for _, s := range prior.Selections {
				if s.Origin == "lexicon" {
					pool = append(pool, Candidate{Keyword: s.Keyword, NormalizedWord: s.NormalizedWord, Language: s.Language, Sources: s.Sources, Confidence: s.Confidence, SnapshotID: s.SnapshotID, Scope: s.Scope})
				}
			}
			if len(pool) > 0 {
				return pool, pool[0].SnapshotID, nil
			}
		}
	}
	if first("LEXICON_CANDIDATES_FILE") == "" {
		if _, e := connect(); e == nil {
			data, e := request(ctx, "SELECT keyword,normalized_word,language,sources,snapshot_id,scope,argMax(confidence,version) AS confidence FROM Data_Content_Lexicon.keyword_selection_log WHERE scope="+literal(scope)+" AND run_id="+literal(RunID())+" AND origin='lexicon' GROUP BY keyword,normalized_word,language,sources,snapshot_id,scope FORMAT JSON", nil, "")
			if e != nil {
				return nil, "", e
			}
			var prior struct {
				Data []Candidate `json:"data"`
			}
			if json.Unmarshal(data, &prior) != nil {
				return nil, "", errors.New("keyword_ledger_response_invalid")
			}
			if len(prior.Data) > 0 {
				for _, c := range prior.Data {
					if !validCandidate(c) {
						return nil, "", errors.New("keyword_ledger_candidate_invalid")
					}
				}
				return prior.Data, prior.Data[0].SnapshotID, nil
			}
		}
	}
	if path := first("LEXICON_CANDIDATES_FILE"); path != "" {
		f, e := os.Open(path)
		if e != nil {
			return nil, "", errors.New("keyword_pool_file_unavailable")
		}
		defer f.Close()
		var envelope struct {
			Candidates  []Candidate `json:"candidates"`
			SnapshotID  string      `json:"snapshot_id"`
			GeneratedAt time.Time   `json:"generated_at"`
		}
		if json.NewDecoder(io.LimitReader(f, 8*1024*1024)).Decode(&envelope) != nil {
			return nil, "", errors.New("keyword_pool_file_invalid")
		}
		if envelope.GeneratedAt.IsZero() || time.Since(envelope.GeneratedAt) > 7*24*time.Hour || envelope.GeneratedAt.After(time.Now().Add(time.Minute)) {
			return nil, "", errors.New("keyword_pool_expired")
		}
		for _, c := range envelope.Candidates {
			if c.Scope == scope {
				if c.SnapshotID == "" {
					c.SnapshotID = envelope.SnapshotID
				}
				pool = append(pool, c)
			}
		}
	} else {
		sql := "SELECT keyword,normalized_word,language,sources,confidence,snapshot_id,scope FROM Data_Content_Lexicon.keyword_candidate_pool_latest WHERE scope=" + literal(scope) + " AND normalized_word GLOBAL NOT IN (SELECT normalized_word FROM (SELECT normalized_word,run_id,argMax(cooldown_until,version) AS until FROM Data_Content_Lexicon.keyword_selection_log WHERE scope=" + literal(scope) + " AND origin='lexicon' AND run_id!=" + literal(RunID()) + " GROUP BY normalized_word,run_id) WHERE until>now()) ORDER BY cityHash64(normalized_word," + literal(RunID()) + ") LIMIT 2000 FORMAT JSON"
		data, e := request(ctx, sql, nil, "")
		if e != nil {
			return nil, "", e
		}
		var envelope struct {
			Data []Candidate `json:"data"`
		}
		if json.Unmarshal(data, &envelope) != nil {
			return nil, "", errors.New("keyword_pool_response_invalid")
		}
		pool = envelope.Data
	}
	filtered := []Candidate{}
	snapshot := ""
	for _, c := range pool {
		if validCandidate(c) {
			filtered = append(filtered, c)
			snapshot = c.SnapshotID
		}
	}
	if len(filtered) == 0 {
		return nil, "", errors.New("keyword_pool_empty")
	}
	return filtered, snapshot, nil
}

type receipt struct {
	Scope       string
	RunID       string
	Count       int
	CuratedHash string
	Selections  []Selection
}

func directory() (string, error) {
	path := first("LEXICON_STATE_DIR")
	if path == "" {
		base, e := os.UserCacheDir()
		if e != nil {
			return "", e
		}
		path = filepath.Join(base, "statground", "keyword-discovery")
	}
	if e := os.MkdirAll(path, 0700); e != nil {
		return "", e
	}
	return path, nil
}
func validateSelections(rows []Selection, scope, seed string) bool {
	if len(rows) == 0 || len(rows)%2 != 0 {
		return false
	}
	origins := map[string]int{}
	seen := map[string]bool{}
	for _, s := range rows {
		k := key(s.Keyword)
		if k == "" || seen[k] || s.Scope != scope || s.RunID != seed {
			return false
		}
		seen[k] = true
		origins[s.Origin]++
		if !validSelection(s) {
			return false
		}
	}
	return origins["curated"] == len(rows)/2 && origins["lexicon"] == len(rows)/2
}
func Select(curated []string, pool []Candidate, count int, seed, scope string) ([]Selection, error) {
	if !validScope(scope) || seed == "" || count < 2 || count > 200 {
		return nil, errors.New("keyword_plan_invalid")
	}
	count -= count % 2
	raw, _ := json.Marshal(curated)
	curatedHash := digest(raw)
	dir, e := directory()
	if e != nil {
		return nil, errors.New("keyword_state_unavailable")
	}
	path := filepath.Join(dir, digest([]byte(scope+"\x00"+seed))+".json")
	if data, readErr := os.ReadFile(path); readErr == nil {
		var prior receipt
		if json.Unmarshal(data, &prior) != nil || prior.Scope != scope || prior.RunID != seed || prior.Count != count || len(prior.Selections) > count || prior.CuratedHash != curatedHash || !validateSelections(prior.Selections, scope, seed) {
			return nil, errors.New("keyword_receipt_conflict")
		}
		return prior.Selections, nil
	} else if !os.IsNotExist(readErr) {
		return nil, errors.New("keyword_receipt_unavailable")
	}
	if _, ce := connect(); ce == nil {
		data, re := request(context.Background(), "SELECT keyword,normalized_word,language,origin,snapshot_id,run_id,scope,sources,toString(argMax(selected_at,version)) AS selected_at,argMax(confidence,version) AS confidence FROM Data_Content_Lexicon.keyword_selection_log WHERE scope="+literal(scope)+" AND run_id="+literal(seed)+" GROUP BY keyword,normalized_word,language,origin,snapshot_id,run_id,scope,sources FORMAT JSON", nil, "")
		if re != nil {
			return nil, re
		}
		var old struct {
			Data []Selection `json:"data"`
		}
		if json.Unmarshal(data, &old) != nil {
			return nil, errors.New("keyword_ledger_response_invalid")
		}
		if len(old.Data) > 0 {
			if !validateSelections(old.Data, scope, seed) || len(old.Data) > count {
				return nil, errors.New("keyword_ledger_plan_conflict")
			}
			return save(path, receipt{scope, seed, count, curatedHash, old.Data})
		}
	}
	h := sha256.Sum256([]byte(scope + "\x00" + seed))
	var n int64
	for _, b := range h[:8] {
		n = (n << 8) | int64(b)
	}
	random := mathrand.New(mathrand.NewSource(n))
	base := []string{}
	seen := map[string]bool{}
	for _, word := range curated {
		word = strings.TrimSpace(word)
		k := key(word)
		if k != "" && !seen[k] {
			seen[k] = true
			base = append(base, word)
		}
	}
	unique := map[string]Candidate{}
	for _, c := range pool {
		if c.Scope != "" && c.Scope != scope {
			continue
		}
		k := key(c.NormalizedWord)
		if validCandidate(c) && !seen[k] {
			if _, exists := unique[k]; !exists {
				unique[k] = c
			}
		}
	}
	words := []string{}
	for word := range unique {
		words = append(words, word)
	}
	sort.Strings(words)
	random.Shuffle(len(base), func(i, j int) { base[i], base[j] = base[j], base[i] })
	random.Shuffle(len(words), func(i, j int) { words[i], words[j] = words[j], words[i] })
	half := count / 2
	if len(base) < half {
		half = len(base)
	}
	if len(words) < half {
		half = len(words)
	}
	if half == 0 {
		return nil, errors.New("keyword_balanced_pool_empty")
	}
	selected := []Selection{}
	now := time.Now().UTC().Format("2006-01-02 15:04:05.000")
	for i := 0; i < half; i++ {
		selected = append(selected, Selection{Keyword: base[i], NormalizedWord: key(base[i]), Language: "und", Origin: "curated", RunID: seed, Scope: scope, Sources: []string{}, SelectedAt: now})
		c := unique[words[i]]
		selected = append(selected, Selection{Keyword: c.Keyword, NormalizedWord: c.NormalizedWord, Language: c.Language, Origin: "lexicon", SnapshotID: c.SnapshotID, RunID: seed, Scope: scope, Sources: c.Sources, SelectedAt: now, Confidence: c.Confidence})
	}
	return save(path, receipt{scope, seed, count, curatedHash, selected})
}
func save(path string, value receipt) ([]Selection, error) {
	data, e := json.Marshal(value)
	if e != nil {
		return nil, e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".receipt-*")
	if e != nil {
		return nil, errors.New("keyword_receipt_write_failed")
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil || ce != nil {
		return nil, errors.New("keyword_receipt_write_failed")
	}
	if os.Rename(name, path) != nil {
		return nil, errors.New("keyword_receipt_write_failed")
	}
	return value.Selections, nil
}
func Record(ctx context.Context, rows []Selection) error { return logRows(ctx, rows, "selected", 0) }
func Outcome(ctx context.Context, rows []Selection, outcome string, resultCount int) error {
	if !safeTag.MatchString(outcome) || resultCount < 0 {
		return errors.New("keyword_outcome_invalid")
	}
	return logRows(ctx, rows, outcome, resultCount)
}
func logRows(ctx context.Context, rows []Selection, outcome string, count int) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) > 200 || !safeTag.MatchString(outcome) || count < 0 {
		return errors.New("keyword_selection_invalid")
	}
	var body bytes.Buffer
	seen := map[string]bool{}
	for _, s := range rows {
		identity := s.Scope + "\x00" + s.RunID + "\x00" + s.NormalizedWord + "\x00" + s.Origin
		if !validSelection(s) || seen[identity] {
			return errors.New("keyword_selection_invalid")
		}
		seen[identity] = true
		selected, e := time.Parse("2006-01-02 15:04:05.000", s.SelectedAt)
		if e != nil {
			return errors.New("keyword_selected_time_invalid")
		}
		version := time.Now().UnixMicro()
		if outcome == "selected" {
			version = selected.UnixMicro()
		}
		cooldown := selected.Add(7 * 24 * time.Hour)
		if outcome == "empty" || (outcome == "completed" && count == 0) {
			cooldown = selected.Add(14 * 24 * time.Hour)
		}
		if strings.Contains(outcome, "error") || strings.Contains(outcome, "unavailable") {
			cooldown = selected.Add(time.Hour)
		}
		sources := s.Sources
		if sources == nil {
			sources = []string{}
		}
		row := map[string]any{"scope": s.Scope, "run_id": s.RunID, "normalized_word": s.NormalizedWord, "keyword": s.Keyword, "language": s.Language, "origin": s.Origin, "snapshot_id": s.SnapshotID, "sources": sources, "confidence": s.Confidence, "outcome": outcome, "result_count": count, "selected_at": s.SelectedAt, "cooldown_until": cooldown.Format("2006-01-02 15:04:05.000"), "version": version}
		if e := json.NewEncoder(&body).Encode(row); e != nil {
			return e
		}
	}
	path, e := pendingSave(body.Bytes())
	if e != nil {
		return e
	}
	if e := FlushPending(ctx); e != nil {
		return e
	}
	if _, e := os.Stat(path); os.IsNotExist(e) {
		return nil
	}
	return deliverPending(ctx, path)
}

func validSelection(s Selection) bool {
	if !validScope(s.Scope) || s.RunID == "" || len(s.RunID) > 128 || s.NormalizedWord != key(s.Keyword) || s.NormalizedWord == "" || len([]rune(s.Keyword)) > 200 || strings.ContainsAny(s.Keyword+s.RunID, "\x00\r\n\t") {
		return false
	}
	when, e := time.Parse("2006-01-02 15:04:05.000", s.SelectedAt)
	if e != nil || when.UnixMicro() <= 0 || when.After(time.Now().Add(time.Minute)) {
		return false
	}
	if s.Origin == "curated" {
		return s.SnapshotID == "" && s.Confidence == 0
	}
	return s.Origin == "lexicon" && validCandidate(Candidate{Keyword: s.Keyword, NormalizedWord: s.NormalizedWord, Language: s.Language, Sources: s.Sources, Confidence: s.Confidence, SnapshotID: s.SnapshotID, Scope: s.Scope})
}

type deliveryRow struct {
	Selection
	Outcome       string      `json:"outcome"`
	ResultCount   json.Number `json:"result_count"`
	CooldownUntil string      `json:"cooldown_until"`
	Version       json.Number `json:"version"`
}

func immutablePlanMatches(expected, got deliveryRow) bool {
	a, b := expected.Selection, got.Selection
	if a.Scope != b.Scope || a.RunID != b.RunID || a.NormalizedWord != b.NormalizedWord || a.Keyword != b.Keyword || a.Language != b.Language || a.Origin != b.Origin || a.SnapshotID != b.SnapshotID || a.SelectedAt != b.SelectedAt || float32(a.Confidence) != float32(b.Confidence) || len(a.Sources) != len(b.Sources) {
		return false
	}
	left, right := append([]string{}, a.Sources...), append([]string{}, b.Sources...)
	sort.Strings(left)
	sort.Strings(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func deliverPending(ctx context.Context, path string) error {
	f, e := os.Open(path)
	if e != nil {
		return errors.New("keyword_pending_corrupt")
	}
	body, e := io.ReadAll(io.LimitReader(f, (2<<20)+1))
	_ = f.Close()
	name := filepath.Base(path)
	if e != nil || len(body) > 2<<20 || !pendingTag.MatchString(name) || digest(body) != strings.TrimSuffix(name[len(name)-69:], ".json") {
		return errors.New("keyword_pending_corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	expected := []deliveryRow{}
	keys := []string{}
	for decoder.More() {
		var row deliveryRow
		if decoder.Decode(&row) != nil || !validSelection(row.Selection) || !safeTag.MatchString(row.Outcome) {
			return errors.New("keyword_pending_corrupt")
		}
		if _, e := strconv.ParseUint(row.Version.String(), 10, 64); e != nil {
			return errors.New("keyword_pending_corrupt")
		}
		if _, e := strconv.ParseUint(row.ResultCount.String(), 10, 64); e != nil {
			return errors.New("keyword_pending_corrupt")
		}
		expected = append(expected, row)
		keys = append(keys, "("+literal(row.Scope)+","+literal(row.RunID)+","+literal(row.NormalizedWord)+","+literal(row.Origin)+")")
	}
	if len(expected) == 0 || len(expected) > 200 {
		return errors.New("keyword_pending_corrupt")
	}
	if e := writeGate(ctx); e != nil {
		return e
	}
	if _, e := request(ctx, "INSERT INTO Data_Content_Lexicon.keyword_selection_log FORMAT JSONEachRow", body, digest(body)); e != nil {
		return e
	}
	// Reusing the source column name as an aggregate alias makes ClickHouse
	// substitute max(version) inside argMax and reject the readback query.
	sql := "SELECT scope,run_id,normalized_word,origin,argMax(keyword,version) AS keyword,argMax(language,version) AS language,argMax(snapshot_id,version) AS snapshot_id,argMax(sources,version) AS sources,argMax(confidence,version) AS confidence,toString(argMax(selected_at,version)) AS selected_at,argMax(outcome,version) AS outcome,argMax(result_count,version) AS result_count,toString(argMax(cooldown_until,version)) AS cooldown_until,max(version) AS ack_version FROM Data_Content_Lexicon.keyword_selection_log WHERE (scope,run_id,normalized_word,origin) IN (" + strings.Join(keys, ",") + ") GROUP BY scope,run_id,normalized_word,origin FORMAT JSON"
	data, e := request(ctx, sql, nil, "")
	if e != nil {
		return e
	}
	var readback struct {
		Data []struct {
			deliveryRow
			ACKVersion json.Number `json:"ack_version"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &readback) != nil || len(readback.Data) != len(expected) {
		return errors.New("keyword_selection_readback_mismatch")
	}
	got := map[string]deliveryRow{}
	for _, acknowledged := range readback.Data {
		row := acknowledged.deliveryRow
		row.Version = acknowledged.ACKVersion
		identity := row.Scope + "\x00" + row.RunID + "\x00" + row.NormalizedWord + "\x00" + row.Origin
		if _, seen := got[identity]; seen {
			return errors.New("keyword_selection_readback_mismatch")
		}
		got[identity] = row
	}
	for _, want := range expected {
		actual, ok := got[want.Scope+"\x00"+want.RunID+"\x00"+want.NormalizedWord+"\x00"+want.Origin]
		wv, we := strconv.ParseUint(want.Version.String(), 10, 64)
		gv, ge := strconv.ParseUint(actual.Version.String(), 10, 64)
		if !ok || we != nil || ge != nil || !immutablePlanMatches(want, actual) {
			return errors.New("keyword_selection_readback_mismatch")
		}
		if gv > wv && want.Outcome == "selected" {
			continue
		}
		if gv != wv || want.Outcome != actual.Outcome || want.ResultCount.String() != actual.ResultCount.String() || want.CooldownUntil != actual.CooldownUntil {
			return errors.New("keyword_selection_readback_mismatch")
		}
	}
	if os.Remove(path) != nil {
		return errors.New("keyword_pending_ack_failed")
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return errors.New("keyword_pending_ack_failed")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return errors.New("keyword_pending_ack_failed")
	}
	return nil
}

func pendingSave(body []byte) (string, error) {
	if len(body) > 2<<20 {
		return "", errors.New("keyword_pending_limit")
	}
	dir, e := directory()
	if e != nil {
		return "", errors.New("keyword_state_unavailable")
	}
	var firstRow deliveryRow
	if json.NewDecoder(bytes.NewReader(body)).Decode(&firstRow) != nil {
		return "", errors.New("keyword_pending_corrupt")
	}
	version, e := strconv.ParseUint(firstRow.Version.String(), 10, 64)
	if e != nil {
		return "", errors.New("keyword_pending_corrupt")
	}
	path := filepath.Join(dir, fmt.Sprintf("pending-%020d-%s.json", version, digest(body)))
	if _, e := os.Stat(path); e == nil {
		return path, nil
	}
	entries, e := os.ReadDir(dir)
	if e != nil {
		return "", errors.New("keyword_state_unavailable")
	}
	count, total := 0, int64(len(body))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "pending-") {
			count++
			info, e := entry.Info()
			if e != nil {
				return "", errors.New("keyword_state_unavailable")
			}
			total += info.Size()
		}
	}
	if count >= 128 || total > 16<<20 {
		return "", errors.New("keyword_pending_limit")
	}
	f, e := os.CreateTemp(dir, ".pending-")
	if e != nil {
		return "", errors.New("keyword_state_unavailable")
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(body); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil || ce != nil || os.Rename(name, path) != nil {
		return "", errors.New("keyword_pending_write_failed")
	}
	directory, e := os.Open(dir)
	if e != nil {
		return "", errors.New("keyword_pending_write_failed")
	}
	defer directory.Close()
	if directory.Sync() != nil {
		return "", errors.New("keyword_pending_write_failed")
	}
	return path, nil
}

// FlushPending replays at most eight durable operations. Provider collection is
// never retried here; acknowledged state is removed only after exact readback.
func FlushPending(ctx context.Context) error {
	dir, e := directory()
	if e != nil {
		return errors.New("keyword_state_unavailable")
	}
	entries, e := os.ReadDir(dir)
	if e != nil {
		return errors.New("keyword_state_unavailable")
	}
	count := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "pending-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if count == 8 {
			break
		}
		count++
		if e := deliverPending(ctx, filepath.Join(dir, entry.Name())); e != nil {
			return e
		}
	}
	return nil
}

func writeGate(ctx context.Context) error {
	sql := `SELECT hostName() AS host,is_readonly,is_session_expired,active_replicas,total_replicas,queue_size,absolute_delay FROM clusterAllReplicas('statground_cluster',system.replicas) WHERE database='Data_Content_Lexicon' AND table='keyword_selection_log_local' FORMAT JSON`
	data, e := request(ctx, sql, nil, "")
	if e != nil {
		return e
	}
	var replicas struct {
		Data []struct {
			Host     string `json:"host"`
			Readonly int    `json:"is_readonly"`
			Expired  int    `json:"is_session_expired"`
			Active   int    `json:"active_replicas"`
			Total    int    `json:"total_replicas"`
			Queue    int    `json:"queue_size"`
			Delay    int    `json:"absolute_delay"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &replicas) != nil || len(replicas.Data) != 4 {
		return errors.New("keyword_write_replicas_unverified")
	}
	hosts := map[string]bool{}
	expectedHosts := map[string]bool{"clickhouse-s1-r1": true, "clickhouse-s1-r2": true, "clickhouse-s2-r1": true, "clickhouse-s2-r2": true}
	for _, r := range replicas.Data {
		if hosts[r.Host] || !expectedHosts[r.Host] || r.Readonly != 0 || r.Expired != 0 || r.Active != 2 || r.Total != 2 || r.Queue < 0 || r.Queue > 64 || r.Delay < 0 || r.Delay > 120 {
			return errors.New("keyword_write_replicas_unhealthy")
		}
		hosts[r.Host] = true
	}
	data, e = request(ctx, `SELECT hostName() AS host,free_space/total_space AS free_ratio FROM clusterAllReplicas('statground_cluster',system.disks) WHERE name='default' FORMAT JSON`, nil, "")
	if e != nil {
		return e
	}
	var disks struct {
		Data []struct {
			Host  string   `json:"host"`
			Ratio *float64 `json:"free_ratio"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &disks) != nil || len(disks.Data) != 4 {
		return errors.New("keyword_write_disks_unverified")
	}
	seen := map[string]bool{}
	for _, d := range disks.Data {
		if !hosts[d.Host] || seen[d.Host] || d.Ratio == nil || math.IsNaN(*d.Ratio) || math.IsInf(*d.Ratio, 0) || *d.Ratio < .20 || *d.Ratio > 1 {
			return errors.New("keyword_write_capacity_blocked")
		}
		seen[d.Host] = true
	}
	data, e = request(ctx, `SELECT hostName() AS host,value AS iowait FROM clusterAllReplicas('statground_cluster',system.asynchronous_metrics) WHERE metric='OSIOWaitTimeNormalized' FORMAT JSON`, nil, "")
	if e != nil {
		return e
	}
	var pressure struct {
		Data []struct {
			Host string   `json:"host"`
			IO   *float64 `json:"iowait"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &pressure) != nil || len(pressure.Data) != 4 {
		return errors.New("keyword_write_pressure_unverified")
	}
	seen = map[string]bool{}
	for _, p := range pressure.Data {
		if !hosts[p.Host] || seen[p.Host] || p.IO == nil || math.IsNaN(*p.IO) || math.IsInf(*p.IO, 0) || *p.IO < 0 || *p.IO > .50 {
			return errors.New("keyword_write_pressure_blocked")
		}
		seen[p.Host] = true
	}
	return nil
}
