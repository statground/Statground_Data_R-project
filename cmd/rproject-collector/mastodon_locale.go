package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"
)

// The public language menu has 23 entries; Korean is owned by the existing
// Mastodon collector. Keep this opt-in backfill to at most two other languages
// and one hundred source rows per language in a run.
var mastodonLocaleNames = map[string]string{
	"en": "English", "ja": "Japanese", "zh-Hans": "Simplified Chinese",
	"zh-Hant": "Traditional Chinese", "es": "Spanish", "fr": "French",
	"de": "German", "pt-BR": "Brazilian Portuguese", "ru": "Russian",
	"id": "Indonesian", "vi": "Vietnamese", "th": "Thai",
	"ms": "Malay", "fil": "Filipino", "hi": "Hindi", "ar": "Arabic",
	"it": "Italian", "nl": "Dutch", "pl": "Polish", "sv": "Swedish",
	"tr": "Turkish", "uk": "Ukrainian",
}

var mastodonLocaleUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var mastodonLocaleSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

type mastodonLocaleCandidate struct {
	UUID          string
	StatusURL     string
	StatusCreated string
	ContentText   string
	SourceSHA256  string
	SourceVersion string
}

func parseMastodonLocales(raw string) ([]string, error) {
	locales := make([]string, 0, 2)
	seen := map[string]bool{}
	for _, requested := range splitCSV(raw) {
		canonical := ""
		for code := range mastodonLocaleNames {
			if strings.EqualFold(code, requested) {
				canonical = code
				break
			}
		}
		if canonical == "" {
			return nil, fmt.Errorf("unsupported Mastodon target locale %q", requested)
		}
		if !seen[canonical] {
			locales = append(locales, canonical)
			seen[canonical] = true
		}
	}
	if len(locales) == 0 || len(locales) > 2 {
		return nil, errors.New("Mastodon locale backfill needs one or two non-Korean locales")
	}
	return locales, nil
}

func runMastodonLocale(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mastodon-locale", flag.ExitOnError)
	rawLocales := fs.String("locales", envString("MASTODON_EXTRA_LOCALES", ""), "one or two comma-separated target locales")
	limit := fs.Int("limit", envInt("MASTODON_EXTRA_LOCALE_LIMIT", 2), "maximum current source rows per locale (1-100)")
	model := fs.String("translation-model", envString("MASTODON_TRANSLATION_MODEL", envString("RBLOGGER_TRANSLATION_MODEL", "google/gemini-2.0-flash-exp:free")), "AI model for non-English locales")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "read and translate without database writes")
	fs.Parse(args)
	locales, err := parseMastodonLocales(*rawLocales)
	if err != nil {
		return err
	}
	if *limit < 1 || *limit > 100 {
		return errors.New("Mastodon locale limit must be 1-100")
	}
	pub := newPublisher(defaultWebRTopic, "statground-mastodon-locale-go-collector", *dryRun)
	if !pub.usesClickHouse() || pub.usesKafka() {
		return errors.New("Mastodon locale backfill requires direct ClickHouse publishing")
	}
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return err
	}
	var ai *aiClient
	for _, locale := range locales {
		if locale != "en" {
			ai = newAIClient(time.Duration(maxInt(30, envInt("AI_TIMEOUT", 300))) * time.Second)
			if !ai.enabled() {
				return errors.New("non-English Mastodon locale requires an AI provider key")
			}
			break
		}
	}
	if err := pub.validate(ctx); err != nil {
		return err
	}
	for _, locale := range locales {
		rows, err := cfg.queryJSONEachRow(mastodonLocaleCandidatesSQL(locale, *limit))
		if err != nil {
			return fmt.Errorf("Mastodon locale candidate read failed locale=%s: %w", locale, err)
		}
		events := make([]webREvent, 0, len(rows))
		expected := make(map[string]string, len(rows))
		for _, row := range rows {
			candidate, err := mastodonLocaleCandidateFromRow(row)
			if err != nil {
				return err
			}
			title, content, err := translateMastodonLocale(ai, *model, candidate, locale)
			if err != nil {
				return fmt.Errorf("Mastodon locale translation failed locale=%s uuid=%s: %w", locale, candidate.UUID, err)
			}
			createdAt := parseKSTTime(candidate.StatusCreated, time.Time{})
			if createdAt.IsZero() {
				return errors.New("Mastodon locale source timestamp is invalid")
			}
			now := nowKST()
			translationModel := *model
			if locale == "en" {
				translationModel = "source_copy"
			}
			payload := mastodonBoardPayload(candidate.UUID, candidate.StatusURL, "", createdAt, now, title, content)
			payload["language_code"] = locale
			payload["created_log"] = map[string]any{
				"type": "mastodon_board_locale_translation_v1", "source": "Statground_Data_R-project",
				"source_language": "en", "target_language": locale, "source_sha256": candidate.SourceSHA256,
				"source_version": candidate.SourceVersion, "source_status_url": candidate.StatusURL,
				"translation_model": translationModel,
			}
			events = append(events, newWebREvent("webr.mastodon.board.v1", candidate.StatusURL, payload, now))
			expected[candidate.UUID] = candidate.SourceSHA256
		}
		if len(events) == 0 {
			fmt.Printf("locale=%s selected=0 published=0\n", locale)
			continue
		}
		// A source edit or withdrawal during translation invalidates the batch.
		// The CDN exporter repeats this check after the durable board insert.
		if err := mastodonLocaleSourceMatches(cfg, expected); err != nil {
			return err
		}
		if err := pub.publishWebR(ctx, events); err != nil {
			return err
		}
		if !*dryRun {
			if err := mastodonLocaleSourceMatches(cfg, expected); err != nil {
				return err
			}
			if err := mastodonLocaleBoardMatches(cfg, locale, expected); err != nil {
				return err
			}
		}
		fmt.Printf("locale=%s selected=%d published=%d dry_run=%t\n", locale, len(rows), len(events), *dryRun)
	}
	return nil
}

func mastodonLocaleCandidateFromRow(row map[string]any) (mastodonLocaleCandidate, error) {
	c := mastodonLocaleCandidate{
		UUID: stringAny(row["uuid"]), StatusURL: stringAny(row["status_url"]),
		StatusCreated: stringAny(row["status_created_at"]), ContentText: stringAny(row["content_text"]),
		SourceSHA256: stringAny(row["source_sha256"]), SourceVersion: stringAny(row["source_version"]),
	}
	if !mastodonLocaleUUID.MatchString(c.UUID) || c.StatusURL == "" || c.ContentText == "" || c.SourceVersion == "" || c.StatusCreated == "" || !mastodonLocaleSHA256.MatchString(c.SourceSHA256) {
		return c, errors.New("Mastodon locale candidate has incomplete source identity")
	}
	return c, nil
}

func translateMastodonLocale(ai *aiClient, model string, row mastodonLocaleCandidate, locale string) (string, string, error) {
	name, ok := mastodonLocaleNames[locale]
	if !ok {
		return "", "", errors.New("unsupported Mastodon locale")
	}
	sourceTitle := firstNonEmpty(firstWords(row.ContentText, 16), "R Foundation")
	if locale == "en" {
		// Preserve the English announcement text while omitting outbound URLs,
		// which the shared board sanitizer intentionally rejects.
		return cleanBoardTitle(sourceTitle), safeMastodonBoardContent(sourceTitle, "<p>"+html.EscapeString(removeBoardURLs(row.ContentText))+"</p>"), nil
	}
	if ai == nil || !ai.enabled() {
		return "", "", errors.New("AI provider key is required")
	}
	title, err := ai.chat(fmt.Sprintf("Translate this R Project announcement title into %s. Preserve names and numbers. Return only the title, without links or added facts.\n\n%s", name, sourceTitle), model)
	if err != nil {
		return "", "", err
	}
	content, err := ai.chat(fmt.Sprintf("Translate this R Project announcement into %s. Preserve names, numbers and meaning. Return only an HTML fragment with p, ul, ol, li, strong, em, or code. Do not add links or facts.\n\n%s", name, row.ContentText), model)
	if err != nil {
		return "", "", err
	}
	title = cleanBoardTitle(title)
	content, err = sanitizeBoardHTML(content)
	if err != nil {
		return "", "", err
	}
	if title == "" || strings.TrimSpace(stripTags(content)) == "" || strings.EqualFold(strings.TrimSpace(stripTags(content)), strings.TrimSpace(row.ContentText)) {
		return "", "", errors.New("empty or untranslated Mastodon locale content")
	}
	return title, content, nil
}

// The latest raw row is selected before checking active. A later withdrawal
// therefore cannot expose an older active snapshot or its translation.
func mastodonLocaleRawSQL(uuidFilter string) string {
	return `SELECT toString(uuid) AS uuid, active, visibility, language_code, status_url, content_text,
       toString(status_created_at) AS status_created_at, toString(fetched_at) AS source_version,
       lower(hex(SHA256(concat(ifNull(content_text, ''), unhex('0A'), ifNull(content_html, ''), unhex('0A'),
           ifNull(status_url, ''), unhex('0A'), toString(status_created_at), unhex('0A'),
           ifNull(toString(status_edited_at), ''), unhex('0A'), ifNull(visibility, ''))))) AS source_sha256,
       row_number() OVER (PARTITION BY uuid ORDER BY fetched_at DESC, ingested_at DESC, event_uuid DESC) AS rn
  FROM Data_R_Community_Raw.mastodon_status_raw
 WHERE instance_host = 'fosstodon.org' AND account_acct = 'R_Foundation'` + uuidFilter
}

func mastodonLocaleCandidatesSQL(locale string, limit int) string {
	return fmt.Sprintf(`SELECT r.uuid, r.status_url, r.content_text, r.status_created_at, r.source_version, r.source_sha256
FROM (%s) AS r
LEFT JOIN
(
  SELECT toString(uuid) AS uuid, active, created_log,
         row_number() OVER (PARTITION BY uuid ORDER BY version_at DESC, created_at DESC) AS rn
    FROM Data_R_Community_Service.mastodon_board
   WHERE language_code = '%s'
) AS b ON b.uuid = r.uuid AND b.rn = 1
WHERE r.rn = 1 AND r.active = 1 AND r.visibility IN ('public', 'unlisted')
  AND r.language_code = 'en' AND notEmpty(r.content_text)
  AND (b.uuid IS NULL OR coalesce(b.active, 0) != 1
       OR JSONExtractString(toString(b.created_log), 'target_language') != '%s'
       OR JSONExtractString(toString(b.created_log), 'source_sha256') != r.source_sha256)
ORDER BY r.status_created_at DESC, r.uuid ASC
LIMIT %d
SETTINGS join_use_nulls = 1, max_execution_time = 30, max_threads = 2
FORMAT JSONEachRow`, mastodonLocaleRawSQL(""), locale, locale, limit)
}

func mastodonLocaleUUIDFilter(expected map[string]string) (string, error) {
	ids := make([]string, 0, len(expected))
	for id := range expected {
		if !mastodonLocaleUUID.MatchString(id) {
			return "", errors.New("invalid Mastodon locale UUID")
		}
		ids = append(ids, "'"+id+"'")
	}
	if len(ids) == 0 || len(ids) > 100 {
		return "", errors.New("Mastodon locale UUID set must contain 1-100 rows")
	}
	return " AND toString(uuid) IN (" + strings.Join(ids, ",") + ")", nil
}

func mastodonLocaleSourceMatches(cfg clickHouseQueryConfig, expected map[string]string) error {
	filter, err := mastodonLocaleUUIDFilter(expected)
	if err != nil {
		return err
	}
	rows, err := cfg.queryJSONEachRow(fmt.Sprintf(`SELECT uuid, active, visibility, language_code, source_sha256
FROM (%s) WHERE rn = 1
SETTINGS max_execution_time = 20, max_threads = 2 FORMAT JSONEachRow`, mastodonLocaleRawSQL(filter)))
	if err != nil {
		return err
	}
	if len(rows) != len(expected) {
		return errors.New("Mastodon locale source count changed")
	}
	for _, row := range rows {
		id := stringAny(row["uuid"])
		visibility := stringAny(row["visibility"])
		if expected[id] == "" || expected[id] != stringAny(row["source_sha256"]) || stringAny(row["language_code"]) != "en" || intAny(row["active"]) != 1 || (visibility != "public" && visibility != "unlisted") {
			return errors.New("Mastodon locale source changed or withdrawn")
		}
	}
	return nil
}

func mastodonLocaleBoardMatches(cfg clickHouseQueryConfig, locale string, expected map[string]string) error {
	filter, err := mastodonLocaleUUIDFilter(expected)
	if err != nil {
		return err
	}
	rows, err := cfg.queryJSONEachRow(fmt.Sprintf(`SELECT toString(uuid) AS uuid, active, title, content,
       JSONExtractString(toString(created_log), 'target_language') AS target_language,
       JSONExtractString(toString(created_log), 'source_sha256') AS source_sha256
FROM
(
  SELECT *, row_number() OVER (PARTITION BY uuid ORDER BY version_at DESC, created_at DESC) AS rn
    FROM Data_R_Community_Service.mastodon_board
   WHERE language_code = '%s'%s
) WHERE rn = 1
SETTINGS max_execution_time = 20, max_threads = 2 FORMAT JSONEachRow`, locale, filter))
	if err != nil {
		return err
	}
	if len(rows) != len(expected) {
		return errors.New("Mastodon locale board readback count mismatch")
	}
	for _, row := range rows {
		id := stringAny(row["uuid"])
		if expected[id] == "" || expected[id] != stringAny(row["source_sha256"]) || stringAny(row["target_language"]) != locale || intAny(row["active"]) != 1 || stringAny(row["title"]) == "" || stringAny(row["content"]) == "" {
			return errors.New("Mastodon locale board readback mismatch")
		}
	}
	return nil
}
