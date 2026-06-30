package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	defaultHomeURL = "https://www.r-bloggers.com/"
	defaultPageURL = "https://www.r-bloggers.com/page/%d/"
	userAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

var (
	attrRE            = regexp.MustCompile(`(?is)\s([a-zA-Z_:][-a-zA-Z0-9_:]*)\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))`)
	h3LinkRE          = regexp.MustCompile(`(?is)<h3\b[^>]*>.*?<a\b[^>]*href\s*=\s*["']([^"']+)["']`)
	fallbackLinkRE    = regexp.MustCompile(`(?is)<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>`)
	metaTagRE         = regexp.MustCompile(`(?is)<meta\b[^>]*>`)
	linkTagRE         = regexp.MustCompile(`(?is)<link\b[^>]*>`)
	scriptTagRE       = regexp.MustCompile(`(?is)<script\b[^>]*>(.*?)</script>`)
	htmlCommentRE     = regexp.MustCompile(`(?is)<!--.*?-->`)
	tagRE             = regexp.MustCompile(`(?is)</?\s*([a-zA-Z0-9]+)\b[^>]*>`)
	spaceRE           = regexp.MustCompile(`[ \t\x{00a0}]+`)
	blankLinesRE      = regexp.MustCompile(`\n{3,}`)
	wordRE            = regexp.MustCompile(`[\p{L}\p{N}_]+`)
	urlRE             = regexp.MustCompile(`(?i)\b(?:https?://|www\.)[^\s<>()"']+`)
	markdownLinkRE    = regexp.MustCompile(`\[([^\]]+)\]\((?:https?://|www\.)[^)]+\)`)
	firstAllowedTagRE = regexp.MustCompile(`(?is)<\s*(h2|h3|p|ul|ol|li|strong|em|code|pre|blockquote)\b`)
)

var allowedContentTags = map[string]bool{
	"h2": true, "h3": true, "p": true, "ul": true, "ol": true, "li": true,
	"strong": true, "em": true, "code": true, "pre": true, "blockquote": true,
}

type Config struct {
	MaxPagesFromHome      int           `json:"max_pages_from_home"`
	MaxURLs               int           `json:"max_urls"`
	Sleep                 time.Duration `json:"sleep"`
	TranslateEnabled      bool          `json:"translate_enabled"`
	StaleTranslationLimit int           `json:"stale_translation_limit"`
	FailOnListError       bool          `json:"fail_on_list_error"`
	FailOnCrawlError      bool          `json:"fail_on_crawl_error"`
	FailOnTranslationErr  bool          `json:"fail_on_translation_error"`
	RbloggerHomeURL       string        `json:"rblogger_home_url"`
	RbloggerPageURL       string        `json:"rblogger_page_url"`
	TranslationModel      string        `json:"translation_model"`
	AITimeout             time.Duration `json:"ai_timeout"`
	RebuildLimit          int           `json:"rebuild_limit"`
	RebuildBatchSize      int           `json:"rebuild_batch_size"`
	PublishMode           string        `json:"publish_mode"`
	Kafka                 KafkaConfig      `json:"kafka"`
	ClickHouse            ClickHouseConfig `json:"clickhouse"`
}

type KafkaConfig struct {
	Brokers         []string      `json:"brokers"`
	Username        string        `json:"username"`
	Password        string        `json:"-"`
	SecurityProtocol string      `json:"security_protocol"`
	Topic           string        `json:"topic"`
	ClientID        string        `json:"client_id"`
	BatchSize       int           `json:"batch_size"`
	BatchTimeout    time.Duration `json:"batch_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout"`
	WriteChunkSize  int           `json:"write_chunk_size"`
	MaxMessageBytes int           `json:"max_message_bytes"`
	ProducerSource  string        `json:"producer_source"`
	ProducerIP      string        `json:"producer_ip"`
}

type ClickHouseConfig struct {
	Host     string        `json:"host"`
	Port     int           `json:"port"`
	User     string        `json:"user"`
	Password string        `json:"-"`
	Database string        `json:"database"`
	Secure   bool          `json:"secure"`
	Timeout  time.Duration `json:"timeout"`
}

type KafkaEvent struct {
	EventUUID string `json:"event_uuid"`
	Source    string `json:"source"`
	Host      string `json:"host"`
	UUIDUser  string `json:"uuid_user"`
	IP        string `json:"ip"`
	URL       string `json:"url"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

type AIClient struct {
	httpClient *http.Client
	keys       map[string]string
	providers  []string
	rng        *mrand.Rand
}

type Article struct {
	URL                string              `json:"url"`
	CanonicalURL       string              `json:"canonical_url"`
	HTMLTitle          string              `json:"html_title"`
	H1Title            string              `json:"h1_title"`
	MetaDescription    string              `json:"meta_description"`
	MetaKeywords       string              `json:"meta_keywords"`
	OGTitle            string              `json:"og_title"`
	OGDescription      string              `json:"og_description"`
	OGImage            string              `json:"og_image"`
	TwitterTitle       string              `json:"twitter_title"`
	TwitterDescription string              `json:"twitter_description"`
	ArticleHeadline    string              `json:"article_headline"`
	ArticleSection     string              `json:"article_section"`
	ArticleTags        any                 `json:"article_tags"`
	ArticleAuthor      string              `json:"article_author"`
	ArticlePublished   string              `json:"article_published"`
	ArticleModified    string              `json:"article_modified"`
	MainText           string              `json:"main_text"`
	WordCount          int                 `json:"word_count"`
	ReadingTimeMin     float64             `json:"reading_time_min"`
	InternalLinks      []map[string]string `json:"internal_links"`
	ExternalLinks      []map[string]string `json:"external_links"`
	Images             []map[string]string `json:"images"`
	Lang               string              `json:"lang"`
	CrawledAt          string              `json:"crawled_at"`
}

type StaleRawArticle struct {
	UUID                 string `json:"uuid"`
	Title                string `json:"title"`
	Content              string `json:"content"`
	URL                  string `json:"url"`
	RawCreatedAt         string `json:"raw_created_at"`
	PreviousTranslatedAt string `json:"previous_translated_at"`
}

func main() {
	dryRun := flag.Bool("dry-run", false, "validate configuration without crawling or publishing")
	rebuildBoard := flag.Bool("rebuild-board", false, "retranslate all active raw rows and rebuild ClickHouse board rows")
	flag.Parse()

	cfg, err := loadConfig(*rebuildBoard)
	if err != nil {
		fatal(err)
	}
	ctx := context.Background()
	var result map[string]any
	if *rebuildBoard {
		result, err = runBoardRebuild(ctx, cfg, *dryRun)
	} else {
		result, err = runPipeline(ctx, cfg, *dryRun)
	}
	if err != nil {
		fatal(err)
	}
	printJSON(result)
	if ok, _ := result["ok"].(bool); !ok {
		os.Exit(1)
	}
}

func runPipeline(ctx context.Context, cfg Config, dryRun bool) (map[string]any, error) {
	ai := newAIClient(cfg.AITimeout)
	if cfg.TranslateEnabled && !ai.enabled() {
		return nil, errors.New("TRANSLATE_ENABLED is true, but no AI provider key is configured")
	}
	publisher := NewKafkaPublisher(cfg.Kafka)
	if usesKafkaPublishMode(cfg.PublishMode) {
		if err := publisher.Validate(ctx); err != nil {
			return nil, err
		}
	}
	if usesClickHousePublishMode(cfg.PublishMode) {
		if err := validateClickHouseConfig(cfg.ClickHouse); err != nil {
			return nil, err
		}
	}
	if dryRun {
		return map[string]any{
			"ok":                      true,
			"dry_run":                 true,
			"ai_providers":            ai.providers,
			"publish_mode":            cfg.PublishMode,
			"stale_translation_limit": cfg.StaleTranslationLimit,
			"config":                  cfg,
		}, nil
	}

	httpTimeout := time.Duration(maxInt(10, envInt("RBLOGGER_HTTP_TIMEOUT", envInt("HTTP_TIMEOUT", 30)))) * time.Second
	httpClient := &http.Client{Timeout: httpTimeout}
	listErrors := make([]string, 0)
	urls, err := collectFrontURLs(httpClient, cfg)
	if err != nil {
		listErrors = append(listErrors, fmt.Sprintf("front_urls: %v", err))
		if cfg.FailOnListError {
			return nil, err
		}
		urls = []string{}
	}
	urlHashes := make([]string, 0, len(urls))
	for _, listingURL := range urls {
		urlHashes = append(urlHashes, hashString(canonicalizeURL(listingURL)))
	}
	clickHouse := NewClickHouseReader(cfg.ClickHouse)
	knownHashes := make(map[string]bool)
	if len(urlHashes) > 0 {
		knownHashes, err = clickHouse.KnownURLHashes(ctx, urlHashes)
		if err != nil {
			return nil, err
		}
	}

	events := make([]KafkaEvent, 0, len(urls)*3+2)
	runStarted := nowKST()
	events = append(events, publisher.NewEvent("webr.rblogger.log.v1", defaultHomeURL, map[string]any{
		"uuid":          newUUID(),
		"created_at":    formatClickHouseTime(runStarted),
		"created_log":   map[string]any{"type": "rblogger_pipeline", "stage": "run_started", "config": cfg.publicLogConfig()},
		"language_code": "en",
	}, runStarted))

	crawled := 0
	skippedState := 0
	publishedRaw := 0
	publishedBoard := 0
	staleCandidates := 0
	stalePublished := 0
	crawlErrors := make([]string, 0)
	translationErrors := make([]string, 0)
	staleErrors := make([]string, 0)

	for _, listingURL := range urls {
		canonical := canonicalizeURL(listingURL)
		hash := hashString(canonical)
		if knownHashes[hash] {
			skippedState++
			continue
		}

		article, err := crawlArticle(httpClient, listingURL)
		if err != nil {
			crawlErrors = append(crawlErrors, fmt.Sprintf("%s: %v", listingURL, err))
			if cfg.FailOnCrawlError {
				return nil, err
			}
			continue
		}
		crawled++
		canonical = firstNonEmpty(article.CanonicalURL, article.URL, canonical)
		hash = hashString(canonical)
		if !knownHashes[hash] {
			finalKnown, err := clickHouse.KnownURLHashes(ctx, []string{hash})
			if err != nil {
				return nil, err
			}
			for knownHash, known := range finalKnown {
				knownHashes[knownHash] = known
			}
		}
		if knownHashes[hash] {
			skippedState++
			continue
		}
		rowUUID := deterministicUUID(canonical)
		createdAt := nowKST()

		rawPayload := rawPayload(rowUUID, article, hash, createdAt)
		events = append(events, publisher.NewEvent("webr.rblogger.raw.v1", canonical, rawPayload, createdAt))
		publishedRaw++

		if cfg.TranslateEnabled {
			title, content, err := translateArticle(ai, cfg.TranslationModel, article)
			if err != nil {
				translationErrors = append(translationErrors, fmt.Sprintf("%s: %v", canonical, err))
				if cfg.FailOnTranslationErr {
					return nil, err
				}
			} else {
				boardPayload := boardPayload(rowUUID, canonical, title, content, createdAt)
				events = append(events, publisher.NewEvent("webr.rblogger.board.v1", canonical, boardPayload, createdAt))
				publishedBoard++
			}
		}

		if cfg.Sleep > 0 {
			time.Sleep(cfg.Sleep)
		}
	}

	if cfg.TranslateEnabled && cfg.StaleTranslationLimit > 0 {
		staleRows, err := NewClickHouseReader(cfg.ClickHouse).StaleTranslations(ctx, cfg.StaleTranslationLimit)
		if err != nil {
			staleErrors = append(staleErrors, err.Error())
			if cfg.FailOnTranslationErr {
				return nil, err
			}
		} else {
			staleCandidates = len(staleRows)
			for _, row := range staleRows {
				createdAt := parseClickHouseTime(row.RawCreatedAt, nowKST())
				updatedAt := nowKST()
				title, content, err := translateArticle(ai, cfg.TranslationModel, Article{
					CanonicalURL:    row.URL,
					URL:             row.URL,
					ArticleHeadline: row.Title,
					MainText:        row.Content,
				})
				if err != nil {
					staleErrors = append(staleErrors, fmt.Sprintf("%s: %v", firstNonEmpty(row.URL, row.UUID), err))
					if cfg.FailOnTranslationErr {
						return nil, err
					}
					continue
				}
				payload := boardPayloadWithUpdated(row.UUID, row.URL, title, content, createdAt, updatedAt)
				payload["created_log"] = map[string]any{
					"type":                   "rblogger_board_translation_refresh",
					"source":                 "Statground_Data_R-project",
					"raw_url":                row.URL,
					"raw_created_at":         row.RawCreatedAt,
					"refresh_reason":         "translation_missing_or_older_than_one_month",
					"previous_translated_at": row.PreviousTranslatedAt,
					"prompt_language":        "en",
					"hyperlinks":             "removed",
					"content_fallback":       "title_when_blank",
				}
				events = append(events, publisher.NewEvent("webr.rblogger.board.v1", row.URL, payload, createdAt))
				stalePublished++
			}
		}
	}

	doneAt := nowKST()
	events = append(events, publisher.NewEvent("webr.rblogger.log.v1", defaultHomeURL, map[string]any{
		"uuid":       newUUID(),
		"created_at": formatClickHouseTime(doneAt),
		"created_log": map[string]any{
			"type":               "rblogger_pipeline",
			"stage":              "run_done",
			"collected_urls":     len(urls),
			"crawled":            crawled,
			"skipped_state":      skippedState,
			"published_raw":      publishedRaw,
			"published_board":    publishedBoard,
			"stale_candidates":   staleCandidates,
			"stale_published":    stalePublished,
			"list_errors":        firstN(listErrors, 20),
			"crawl_errors":       firstN(crawlErrors, 50),
			"translation_errors": firstN(translationErrors, 50),
			"stale_errors":       firstN(staleErrors, 50),
			"prompt_language":    "en",
			"hyperlink_policy":   "remove_urls_markdown_links_a_tags_and_href",
		},
		"language_code": "en",
	}, doneAt))

	directCounts := map[string]int{}
	if usesClickHousePublishMode(cfg.PublishMode) {
		counts, err := clickHouse.InsertRbloggerEvents(ctx, events, cfg.Kafka.WriteChunkSize)
		if err != nil {
			return nil, err
		}
		directCounts = counts
	}
	if usesKafkaPublishMode(cfg.PublishMode) {
		if err := publisher.Publish(ctx, events); err != nil {
			return nil, err
		}
	}

	ok := (!cfg.FailOnListError || len(listErrors) == 0) && (!cfg.FailOnCrawlError || len(crawlErrors) == 0) && (!cfg.FailOnTranslationErr || len(translationErrors) == 0)
	return map[string]any{
		"ok":                 ok,
		"collected_urls":     len(urls),
		"crawled":            crawled,
		"skipped_state":      skippedState,
		"published_raw":      publishedRaw,
		"published_board":    publishedBoard,
		"stale_candidates":   staleCandidates,
		"stale_published":    stalePublished,
		"publish_mode":       cfg.PublishMode,
		"published_events":   len(events),
		"direct_tables":      directCounts,
		"list_errors":        listErrors,
		"crawl_errors":       crawlErrors,
		"translation_errors": translationErrors,
		"stale_errors":       staleErrors,
	}, nil
}

func runBoardRebuild(ctx context.Context, cfg Config, dryRun bool) (map[string]any, error) {
	ai := newAIClient(cfg.AITimeout)
	if !ai.enabled() {
		return nil, errors.New("at least one AI provider key is required for board rebuild")
	}
	ch := NewClickHouseReader(cfg.ClickHouse)
	rawRows, err := ch.AllRawArticles(ctx, cfg.RebuildLimit)
	if err != nil {
		return nil, err
	}
	if dryRun {
		return map[string]any{
			"ok":                 true,
			"dry_run":            true,
			"raw_rows":           len(rawRows),
			"rebuild_limit":      cfg.RebuildLimit,
			"rebuild_batch_size": cfg.RebuildBatchSize,
			"ai_providers":       ai.providers,
		}, nil
	}

	payloads := make([]map[string]any, 0, len(rawRows))
	translationErrors := make([]string, 0)
	rebuildAt := nowKST()
	for idx, row := range rawRows {
		createdAt := parseClickHouseTime(row.RawCreatedAt, rebuildAt)
		title, content, err := translateArticle(ai, cfg.TranslationModel, Article{
			CanonicalURL:    row.URL,
			URL:             row.URL,
			ArticleHeadline: row.Title,
			MainText:        row.Content,
		})
		if err != nil {
			translationErrors = append(translationErrors, fmt.Sprintf("%s: %v", firstNonEmpty(row.URL, row.UUID), err))
			continue
		}
		payload := boardPayloadWithUpdated(row.UUID, row.URL, title, content, createdAt, rebuildAt)
		payload["created_log"] = map[string]any{
			"type":             "rblogger_board_full_rebuild",
			"source":           "Statground_Data_R-project",
			"raw_url":          row.URL,
			"raw_created_at":   row.RawCreatedAt,
			"rebuild_index":    idx + 1,
			"rebuild_total":    len(rawRows),
			"prompt_language":  "en",
			"hyperlinks":       "removed",
			"content_fallback": "title_when_blank",
		}
		payload["updated_log"] = map[string]any{
			"type":       "rblogger_board_full_rebuild_insert",
			"source":     "Statground_Data_R-project",
			"updated_at": formatClickHouseTime(rebuildAt),
		}
		payloads = append(payloads, payload)
	}
	if len(payloads) == 0 {
		return map[string]any{
			"ok":                 false,
			"raw_rows":           len(rawRows),
			"inserted_board":     0,
			"translation_errors": translationErrors,
		}, nil
	}
	if len(translationErrors) > 0 {
		return map[string]any{
			"ok":                 false,
			"raw_rows":           len(rawRows),
			"prepared_board":     len(payloads),
			"inserted_board":     0,
			"mutation_started":   false,
			"translation_errors": translationErrors,
		}, nil
	}
	deactivateAll := cfg.RebuildLimit == 0
	if err := ch.DeactivateBoardRows(ctx, rawRows, cfg.RebuildBatchSize, deactivateAll); err != nil {
		return nil, err
	}
	if err := ch.InsertBoardPayloads(ctx, payloads, cfg.RebuildBatchSize); err != nil {
		return nil, err
	}
	deactivatedScope := "limited_raw_uuid_rows"
	if deactivateAll {
		deactivatedScope = "all_existing_korean_board_rows"
	}
	return map[string]any{
		"ok":                      true,
		"raw_rows":                len(rawRows),
		"inserted_board":          len(payloads),
		"created_at_aligned_rows": len(rawRows),
		"deactivated_board_scope": deactivatedScope,
		"translation_errors":      translationErrors,
	}, nil
}

func loadConfig(rebuildBoard bool) (Config, error) {
	publishMode := normalizePublishMode(envString("RPROJECT_PUBLISH_MODE", envString("R_DATA_PUBLISH_MODE", "clickhouse")))
	kafkaCfg := KafkaConfig{
		Brokers:         splitCSV(firstNonEmpty(os.Getenv("KAFKA_BROKERS"), os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))),
		Username:        firstNonEmpty(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_EXTERNAL_USER")),
		Password:        firstNonEmpty(os.Getenv("KAFKA_PASSWORD"), os.Getenv("KAFKA_EXTERNAL_PASSWORD")),
		SecurityProtocol: envString("KAFKA_SECURITY_PROTOCOL", ""),
		Topic:           envString("KAFKA_TOPIC", "webr.events"),
		ClientID:        envString("KAFKA_CLIENT_ID", "statground-rblogger-crawler"),
		BatchSize:       maxInt(1, envInt("KAFKA_BATCH_SIZE", 50)),
		BatchTimeout:    envFloatDuration("KAFKA_BATCH_TIMEOUT", 0.5),
		WriteTimeout:    time.Duration(maxInt(1, envInt("KAFKA_WRITE_TIMEOUT", 30))) * time.Second,
		WriteChunkSize:  maxInt(1, envInt("KAFKA_WRITE_CHUNK_SIZE", 50)),
		MaxMessageBytes: maxInt(131072, envInt("KAFKA_MAX_MESSAGE_BYTES", 524288)),
		ProducerSource:  envString("PRODUCER_SOURCE", "github_actions"),
		ProducerIP:      envString("PRODUCER_IP", "::"),
	}
	if !rebuildBoard && usesKafkaPublishMode(publishMode) && len(kafkaCfg.Brokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is required")
	}
	if !rebuildBoard && usesKafkaPublishMode(publishMode) && strings.TrimSpace(kafkaCfg.Topic) == "" {
		return Config{}, errors.New("KAFKA_TOPIC is required")
	}
	clickHouseCfg := ClickHouseConfig{
		Host:     firstNonEmpty(os.Getenv("CH_HOST"), os.Getenv("CLICKHOUSE_HOST")),
		Port:     maxInt(1, envInt("CH_PORT", envInt("CLICKHOUSE_PORT", 8123))),
		User:     firstNonEmpty(os.Getenv("CH_USER"), os.Getenv("CLICKHOUSE_USER")),
		Password: firstNonEmpty(os.Getenv("CH_PASSWORD"), os.Getenv("CLICKHOUSE_PASSWORD")),
		Database: envString("CH_DATABASE", envString("CLICKHOUSE_DATABASE", "Data_R_Community_Raw")),
		Secure:   envBool("CH_SECURE", envBool("CLICKHOUSE_SECURE", false)),
		Timeout:  time.Duration(maxInt(10, envInt("CH_TIMEOUT", envInt("CLICKHOUSE_TIMEOUT", 60)))) * time.Second,
	}
	staleLimit := maxInt(0, envInt("STALE_TRANSLATION_LIMIT", envInt("RBLOGGER_STALE_TRANSLATION_LIMIT", 20)))
	translateEnabled := envBool("TRANSLATE_ENABLED", true)
	if rebuildBoard || usesClickHousePublishMode(publishMode) || (translateEnabled && staleLimit > 0) {
		if err := validateClickHouseConfig(clickHouseCfg); err != nil {
			return Config{}, err
		}
	}
	return Config{
		MaxPagesFromHome:     maxInt(1, envInt("MAX_PAGES_FROM_HOME", 1)),
		MaxURLs:              maxInt(0, envInt("MAX_URLS", 0)),
		Sleep:                envFloatDuration("SLEEP_SEC", 1.0),
		TranslateEnabled:     translateEnabled,
		StaleTranslationLimit: staleLimit,
		FailOnListError:      envBool("FAIL_ON_LIST_ERROR", envBool("RBLOGGER_FAIL_ON_LIST_ERROR", false)),
		FailOnCrawlError:     envBool("FAIL_ON_CRAWL_ERROR", false),
		FailOnTranslationErr: envBool("FAIL_ON_TRANSLATION_ERROR", false),
		RbloggerHomeURL:      envString("RBLOGGER_HOME_URL", defaultHomeURL),
		RbloggerPageURL:      envString("RBLOGGER_PAGE_URL", defaultPageURL),
		TranslationModel:     envString("RBLOGGER_TRANSLATION_MODEL", "google/gemini-2.0-flash-exp:free"),
		AITimeout:            time.Duration(maxInt(30, envInt("AI_TIMEOUT", 300))) * time.Second,
		RebuildLimit:         maxInt(0, envInt("RBLOGGER_REBUILD_LIMIT", 0)),
		RebuildBatchSize:     maxInt(1, envInt("RBLOGGER_REBUILD_BATCH_SIZE", 50)),
		PublishMode:          publishMode,
		Kafka:                kafkaCfg,
		ClickHouse:           clickHouseCfg,
	}, nil
}

func normalizePublishMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "clickhouse", "db", "direct":
		return "clickhouse"
	case "kafka":
		return "kafka"
	case "dual", "both", "clickhouse+kafka", "db+kafka":
		return "dual"
	default:
		return "clickhouse"
	}
}

func usesClickHousePublishMode(value string) bool {
	value = normalizePublishMode(value)
	return value == "clickhouse" || value == "dual"
}

func usesKafkaPublishMode(value string) bool {
	value = normalizePublishMode(value)
	return value == "kafka" || value == "dual"
}

func validateClickHouseConfig(cfg ClickHouseConfig) error {
	if strings.TrimSpace(cfg.Host) == "" {
		return errors.New("CH_HOST is required when ClickHouse reads or direct writes are enabled")
	}
	if strings.TrimSpace(cfg.User) == "" {
		return errors.New("CH_USER is required when ClickHouse reads or direct writes are enabled")
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return errors.New("CH_PASSWORD is required when ClickHouse reads or direct writes are enabled")
	}
	return nil
}

func (c Config) publicLogConfig() map[string]any {
	return map[string]any{
		"max_pages_from_home":       c.MaxPagesFromHome,
		"max_urls":                  c.MaxURLs,
		"sleep":                     c.Sleep.String(),
		"translate_enabled":         c.TranslateEnabled,
		"fail_on_list_error":        c.FailOnListError,
		"fail_on_crawl_error":       c.FailOnCrawlError,
		"fail_on_translation_error": c.FailOnTranslationErr,
		"rblogger_home_url":         c.RbloggerHomeURL,
		"rblogger_page_url":         c.RbloggerPageURL,
		"translation_model":         c.TranslationModel,
		"stale_translation_limit":   c.StaleTranslationLimit,
		"publish_mode":              c.PublishMode,
		"clickhouse_host":           c.ClickHouse.Host,
		"clickhouse_database":       c.ClickHouse.Database,
	}
}

type KafkaPublisher struct {
	cfg  KafkaConfig
	host string
}

func NewKafkaPublisher(cfg KafkaConfig) *KafkaPublisher {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "github-actions"
	}
	return &KafkaPublisher{cfg: cfg, host: host}
}

func (p *KafkaPublisher) Validate(ctx context.Context) error {
	for _, broker := range p.cfg.Brokers {
		if isLoopbackBrokerEndpoint(broker) {
			return fmt.Errorf("KAFKA_BROKERS must be externally reachable, not %q", broker)
		}
	}
	dialer := &kafka.Dialer{
		ClientID: p.cfg.ClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(p.cfg.Brokers, 10*time.Second),
	}
	if strings.TrimSpace(p.cfg.Username) != "" || strings.TrimSpace(p.cfg.Password) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: p.cfg.Username, Password: p.cfg.Password}
	}
	if kafkaSecurityUsesTLS(p.cfg.SecurityProtocol) {
		dialer.TLS = kafkaTLSConfig()
	}
	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(probeCtx, "tcp", p.cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("kafka preflight failed to connect to bootstrap broker %q: %w", p.cfg.Brokers[0], err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(p.cfg.Topic)
	if err != nil {
		return fmt.Errorf("kafka preflight failed to read metadata for topic %q: %w", p.cfg.Topic, err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka preflight found zero partitions for topic %q", p.cfg.Topic)
	}
	if err := validateKafkaAdvertisedLeaders(partitions, p.cfg.Brokers, "kafka broker metadata"); err != nil {
		return err
	}
	fmt.Printf("[kafka] preflight ok topic=%s partitions=%d bootstrap=%s\n", p.cfg.Topic, len(partitions), p.cfg.Brokers[0])
	return nil
}

func (p *KafkaPublisher) NewEvent(eventType, sourceURL string, payload map[string]any, createdAt time.Time) KafkaEvent {
	payloadJSON, _ := json.Marshal(payload)
	return KafkaEvent{
		EventUUID: newUUID(),
		Source:    p.cfg.ProducerSource,
		Host:      p.host,
		UUIDUser:  "",
		IP:        p.cfg.ProducerIP,
		URL:       sourceURL,
		EventType: eventType,
		Payload:   string(payloadJSON),
		CreatedAt: formatClickHouseTime(createdAt),
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, events []KafkaEvent) error {
	if len(events) == 0 {
		return nil
	}
	writer := p.writer()
	defer writer.Close()
	chunkSize := maxInt(1, p.cfg.WriteChunkSize)
	messages := make([]kafka.Message, 0, minInt(chunkSize, len(events)))
	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if len(body) > p.cfg.MaxMessageBytes {
			return fmt.Errorf("kafka message too large event_type=%s url=%s bytes=%d max=%d", event.EventType, event.URL, len(body), p.cfg.MaxMessageBytes)
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(firstNonEmpty(event.URL, event.EventUUID)),
			Value: body,
			Time:  time.Now(),
		})
		if len(messages) >= chunkSize {
			if err := p.writeMessages(ctx, writer, messages); err != nil {
				return err
			}
			messages = messages[:0]
		}
	}
	return p.writeMessages(ctx, writer, messages)
}

func (p *KafkaPublisher) writer() *kafka.Writer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(p.cfg.Brokers...),
		Topic:                  p.cfg.Topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              p.cfg.BatchSize,
		BatchTimeout:           p.cfg.BatchTimeout,
		WriteTimeout:           p.cfg.WriteTimeout,
		ReadTimeout:            p.cfg.WriteTimeout,
	}
	transport := &kafka.Transport{
		ClientID: p.cfg.ClientID,
		Dial:     kafkaAdvertisedBrokerDialFunc(p.cfg.Brokers, 10*time.Second),
	}
	if strings.TrimSpace(p.cfg.Username) != "" || strings.TrimSpace(p.cfg.Password) != "" {
		transport.SASL = plain.Mechanism{Username: p.cfg.Username, Password: p.cfg.Password}
	}
	if kafkaSecurityUsesTLS(p.cfg.SecurityProtocol) {
		transport.TLS = kafkaTLSConfig()
	}
	writer.Transport = transport
	return writer
}

func (p *KafkaPublisher) writeMessages(ctx context.Context, writer *kafka.Writer, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	writeCtx, cancel := context.WithTimeout(ctx, p.cfg.WriteTimeout+5*time.Second)
	defer cancel()
	return writer.WriteMessages(writeCtx, messages...)
}

type ClickHouseReader struct {
	cfg        ClickHouseConfig
	httpClient *http.Client
}

func NewClickHouseReader(cfg ClickHouseConfig) *ClickHouseReader {
	return &ClickHouseReader{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: cfg.Timeout},
	}
}

func (r *ClickHouseReader) StaleTranslations(ctx context.Context, limit int) ([]StaleRawArticle, error) {
	if limit <= 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
WITH
raw_latest AS
(
    SELECT *
    FROM
    (
        SELECT
            uuid,
            title,
            content,
            url,
            created_at,
            updated_at,
            active,
            language_code,
            row_number() OVER (PARTITION BY uuid, language_code ORDER BY coalesce(updated_at, created_at, now64(3, 'Asia/Seoul')) DESC) AS rn
        FROM Data_R_Community_Raw.r_blogger_article_raw
        WHERE language_code = 'en' AND coalesce(active, 0) = 1
    )
    WHERE rn = 1
),
board_latest AS
(
    SELECT *
    FROM
    (
        SELECT
            uuid,
            created_at,
            updated_at,
            active,
            language_code,
            row_number() OVER (PARTITION BY uuid, language_code ORDER BY coalesce(updated_at, created_at, now64(3, 'Asia/Seoul')) DESC) AS rn
        FROM Data_R_Community_Service.r_blogger_board
        WHERE language_code = 'ko'
    )
    WHERE rn = 1
)
SELECT
    toString(r.uuid) AS uuid,
    ifNull(r.title, '') AS title,
    ifNull(r.content, '') AS content,
    ifNull(r.url, '') AS url,
    ifNull(toString(r.created_at), '') AS raw_created_at,
    ifNull(toString(coalesce(b.updated_at, b.created_at)), '') AS previous_translated_at
FROM raw_latest AS r
LEFT JOIN board_latest AS b ON b.uuid = r.uuid
WHERE notEmpty(ifNull(r.title, ''))
  AND (
      b.uuid IS NULL
      OR coalesce(b.active, 0) = 0
      OR coalesce(b.updated_at, b.created_at, toDateTime64('1970-01-01 00:00:00', 3, 'Asia/Seoul')) < now64(3, 'Asia/Seoul') - toIntervalMonth(1)
  )
ORDER BY coalesce(b.updated_at, b.created_at, toDateTime64('1970-01-01 00:00:00', 3, 'Asia/Seoul')) ASC, r.created_at ASC
LIMIT %d`, limit)
	rows, err := r.queryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]StaleRawArticle, 0, len(rows))
	for _, row := range rows {
		out = append(out, StaleRawArticle{
			UUID:                 stringValue(row["uuid"]),
			Title:                stringValue(row["title"]),
			Content:              stringValue(row["content"]),
			URL:                  stringValue(row["url"]),
			RawCreatedAt:         stringValue(row["raw_created_at"]),
			PreviousTranslatedAt: stringValue(row["previous_translated_at"]),
		})
	}
	return out, nil
}

func (r *ClickHouseReader) AllRawArticles(ctx context.Context, limit int) ([]StaleRawArticle, error) {
	limitSQL := ""
	if limit > 0 {
		limitSQL = fmt.Sprintf("\nLIMIT %d", limit)
	}
	query := `
SELECT
    toString(uuid) AS uuid,
    ifNull(title, '') AS title,
    ifNull(content, '') AS content,
    ifNull(url, '') AS url,
    ifNull(toString(created_at), '') AS raw_created_at
FROM
(
    SELECT
        uuid,
        title,
        content,
        url,
        created_at,
        updated_at,
        active,
        language_code,
        row_number() OVER (PARTITION BY uuid, language_code ORDER BY coalesce(updated_at, created_at, now64(3, 'Asia/Seoul')) DESC) AS rn
    FROM Data_R_Community_Raw.r_blogger_article_raw
    WHERE language_code = 'en' AND coalesce(active, 0) = 1
)
WHERE rn = 1
  AND notEmpty(ifNull(title, ''))
ORDER BY created_at ASC, uuid ASC` + limitSQL
	rows, err := r.queryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]StaleRawArticle, 0, len(rows))
	for _, row := range rows {
		out = append(out, StaleRawArticle{
			UUID:         stringValue(row["uuid"]),
			Title:        stringValue(row["title"]),
			Content:      stringValue(row["content"]),
			URL:          stringValue(row["url"]),
			RawCreatedAt: stringValue(row["raw_created_at"]),
		})
	}
	return out, nil
}

func (r *ClickHouseReader) KnownURLHashes(ctx context.Context, hashes []string) (map[string]bool, error) {
	out := map[string]bool{}
	seen := map[string]bool{}
	cleaned := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		hash = strings.TrimSpace(hash)
		if hash == "" || seen[hash] {
			continue
		}
		seen[hash] = true
		out[hash] = false
		cleaned = append(cleaned, hash)
	}
	if len(cleaned) == 0 {
		return out, nil
	}
	quoted := make([]string, 0, len(cleaned))
	for _, hash := range cleaned {
		quoted = append(quoted, "'"+sqlString(hash)+"'")
	}
	query := fmt.Sprintf(`
SELECT DISTINCT url_hash
FROM Data_R_Community_Raw.r_blogger_article_raw
WHERE language_code = 'en'
  AND coalesce(active, 0) = 1
  AND url_hash IN (%s)`, strings.Join(quoted, ", "))
	rows, err := r.queryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		hash := stringValue(row["url_hash"])
		if hash != "" {
			out[hash] = true
		}
	}
	return out, nil
}

func (r *ClickHouseReader) DeactivateBoardRows(ctx context.Context, rows []StaleRawArticle, batchSize int, deactivateAll bool) error {
	for _, batch := range chunkRawArticles(rows, batchSize) {
		conditions := make([]string, 0, len(batch)*2+1)
		uuidList := make([]string, 0, len(batch))
		for _, row := range batch {
			if strings.TrimSpace(row.UUID) == "" || strings.TrimSpace(row.RawCreatedAt) == "" {
				continue
			}
			uuidExpr := fmt.Sprintf("toUUID('%s')", sqlString(row.UUID))
			createdExpr := fmt.Sprintf("toDateTime64('%s', 3, 'Asia/Seoul')", sqlString(normalizeClickHouseTimeString(row.RawCreatedAt)))
			conditions = append(conditions, fmt.Sprintf("uuid = %s", uuidExpr), createdExpr)
			uuidList = append(uuidList, uuidExpr)
		}
		if len(uuidList) == 0 {
			continue
		}
		query := fmt.Sprintf(`
ALTER TABLE Data_R_Community_Service.r_blogger_board_local
ON CLUSTER statground_cluster
UPDATE
    active = 0,
    created_at = multiIf(%s, created_at)
WHERE language_code = 'ko'
  AND uuid IN (%s)
SETTINGS mutations_sync = 2`, strings.Join(conditions, ", "), strings.Join(uuidList, ", "))
		if err := r.exec(ctx, query); err != nil {
			return err
		}
	}
	if !deactivateAll {
		return nil
	}
	return r.exec(ctx, `
ALTER TABLE Data_R_Community_Service.r_blogger_board_local
ON CLUSTER statground_cluster
UPDATE active = 0
WHERE language_code = 'ko'
  AND coalesce(active, 0) != 0
SETTINGS mutations_sync = 2`)
}

func (r *ClickHouseReader) InsertBoardPayloads(ctx context.Context, payloads []map[string]any, batchSize int) error {
	for _, batch := range chunkPayloads(payloads, batchSize) {
		var body strings.Builder
		body.WriteString("INSERT INTO Data_R_Community_Service.r_blogger_board (uuid, title, content, active, created_at, updated_at, created_log, updated_log, language_code) FORMAT JSONEachRow\n")
		for _, payload := range batch {
			line, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			body.Write(line)
			body.WriteByte('\n')
		}
		if err := r.post(ctx, body.String()); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClickHouseReader) InsertRbloggerEvents(ctx context.Context, events []KafkaEvent, batchSize int) (map[string]int, error) {
	counts := map[string]int{}
	rowsByTable := map[string][]map[string]any{}
	for _, event := range events {
		payload, err := decodeEventPayload(event)
		if err != nil {
			return counts, err
		}
		table, row, err := rbloggerEventDirectRow(event, payload)
		if err != nil {
			return counts, err
		}
		rowsByTable[table] = append(rowsByTable[table], row)
	}
	tables := make([]string, 0, len(rowsByTable))
	for table := range rowsByTable {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		rows := rowsByTable[table]
		if err := r.insertRows(ctx, table, rows, batchSize); err != nil {
			return counts, err
		}
		counts[table] += len(rows)
	}
	return counts, nil
}

func (r *ClickHouseReader) insertRows(ctx context.Context, table string, rows []map[string]any, batchSize int) error {
	batchSize = maxInt(1, envInt("RPROJECT_CLICKHOUSE_CHUNK_SIZE", batchSize))
	for _, batch := range chunkPayloads(rows, batchSize) {
		var body strings.Builder
		body.WriteString(fmt.Sprintf("INSERT INTO %s SETTINGS insert_distributed_sync = 0, insert_deduplicate = 1 FORMAT JSONEachRow\n", table))
		for _, row := range batch {
			line, err := json.Marshal(row)
			if err != nil {
				return err
			}
			body.Write(line)
			body.WriteByte('\n')
		}
		if err := r.post(ctx, body.String()); err != nil {
			return err
		}
	}
	return nil
}

func decodeEventPayload(event KafkaEvent) (map[string]any, error) {
	payload := map[string]any{}
	if strings.TrimSpace(event.Payload) == "" {
		return payload, nil
	}
	if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid event payload event_type=%s: %w", event.EventType, err)
	}
	return payload, nil
}

func rbloggerEventDirectRow(event KafkaEvent, payload map[string]any) (string, map[string]any, error) {
	switch event.EventType {
	case "webr.rblogger.log.v1":
		return "Data_R_Community_Log.r_blogger_log", rbloggerLogRow(event, payload), nil
	case "webr.rblogger.raw.v1":
		return "Data_R_Community_Raw.r_blogger_article_raw", rbloggerRawRow(event, payload), nil
	case "webr.rblogger.board.v1":
		return "Data_R_Community_Service.r_blogger_board", rbloggerBoardRow(event, payload), nil
	default:
		return "", nil, fmt.Errorf("direct ClickHouse R-bloggers publish does not support event_type=%q", event.EventType)
	}
}

func rbloggerLogRow(event KafkaEvent, payload map[string]any) map[string]any {
	return map[string]any{
		"uuid":          firstNonEmpty(stringValue(payload["uuid"]), event.EventUUID),
		"created_at":    nullableString(firstNonEmpty(stringValue(payload["created_at"]), event.CreatedAt)),
		"created_log":   nullableJSON(payload["created_log"]),
		"language_code": firstNonEmpty(stringValue(payload["language_code"]), "en"),
	}
}

func rbloggerRawRow(event KafkaEvent, payload map[string]any) map[string]any {
	createdLog := mapValue(payload["created_log"])
	articleLog := mapValue(createdLog["article"])
	return map[string]any{
		"uuid":                  firstNonEmpty(stringValue(payload["uuid"]), event.EventUUID),
		"created_at":            nullableString(firstNonEmpty(stringValue(payload["created_at"]), event.CreatedAt)),
		"created_log":           nullableJSON(payload["created_log"]),
		"updated_at":            nullableString(stringValue(payload["updated_at"])),
		"updated_log":           nullableJSON(payload["updated_log"]),
		"active":                nullableUInt8(payload["active"]),
		"github_path":           nullableString(stringValue(payload["github_path"])),
		"title":                 nullableString(stringValue(payload["title"])),
		"content":               nullableString(stringValue(payload["content"])),
		"url":                   nullableString(firstNonEmpty(stringValue(payload["url"]), event.URL)),
		"url_hash":              firstNonEmpty(stringValue(payload["url_hash"]), hashString(firstNonEmpty(stringValue(payload["url"]), event.URL))),
		"language_code":         firstNonEmpty(stringValue(payload["language_code"]), "en"),
		"canonical_url":         firstNonEmpty(stringValue(payload["canonical_url"]), stringValue(articleLog["canonical_url"])),
		"html_title":            firstNonEmpty(stringValue(payload["html_title"]), stringValue(articleLog["html_title"])),
		"h1_title":              firstNonEmpty(stringValue(payload["h1_title"]), stringValue(articleLog["h1_title"])),
		"meta_description":      firstNonEmpty(stringValue(payload["meta_description"]), stringValue(articleLog["meta_description"])),
		"meta_keywords":         firstNonEmpty(stringValue(payload["meta_keywords"]), stringValue(articleLog["meta_keywords"])),
		"og_title":              firstNonEmpty(stringValue(payload["og_title"]), stringValue(articleLog["og_title"])),
		"og_description":        firstNonEmpty(stringValue(payload["og_description"]), stringValue(articleLog["og_description"])),
		"og_image":              firstNonEmpty(stringValue(payload["og_image"]), stringValue(articleLog["og_image"])),
		"twitter_title":         firstNonEmpty(stringValue(payload["twitter_title"]), stringValue(articleLog["twitter_title"])),
		"twitter_description":   firstNonEmpty(stringValue(payload["twitter_description"]), stringValue(articleLog["twitter_description"])),
		"article_headline":      firstNonEmpty(stringValue(payload["article_headline"]), stringValue(articleLog["article_headline"])),
		"article_section":       firstNonEmpty(stringValue(payload["article_section"]), stringValue(articleLog["article_section"])),
		"article_tags_json":     jsonString(firstNonEmptyValue(payload["article_tags"], articleLog["article_tags"]), "[]"),
		"article_author":        firstNonEmpty(stringValue(payload["article_author"]), stringValue(articleLog["article_author"])),
		"article_published_at":  nullableString(firstNonEmpty(stringValue(payload["article_published"]), stringValue(articleLog["article_published"]))),
		"article_modified_at":   nullableString(firstNonEmpty(stringValue(payload["article_modified"]), stringValue(articleLog["article_modified"]))),
		"word_count":            uint32Value(firstNonEmptyValue(payload["word_count"], articleLog["word_count"])),
		"reading_time_min":      float32Value(firstNonEmptyValue(payload["reading_time_min"], articleLog["reading_time_min"])),
		"internal_links_json":   jsonString(firstNonEmptyValue(payload["internal_links"], articleLog["internal_links"]), "[]"),
		"external_links_json":   jsonString(firstNonEmptyValue(payload["external_links"], articleLog["external_links"]), "[]"),
		"images_json":           jsonString(firstNonEmptyValue(payload["images"], articleLog["images"]), "[]"),
		"main_text_excerpt":     firstNonEmpty(stringValue(payload["main_text_excerpt"]), stringValue(articleLog["main_text_excerpt"])),
		"raw_article_json":      jsonString(articleLog, "{}"),
	}
}

func rbloggerBoardRow(event KafkaEvent, payload map[string]any) map[string]any {
	title := stringValue(payload["title"])
	content := stringValue(payload["content"])
	if strings.TrimSpace(content) == "" {
		content = title
	}
	return map[string]any{
		"uuid":          firstNonEmpty(stringValue(payload["uuid"]), event.EventUUID),
		"title":         nullableString(title),
		"content":       nullableString(content),
		"active":        nullableUInt8(payload["active"]),
		"created_at":    nullableString(firstNonEmpty(stringValue(payload["created_at"]), event.CreatedAt)),
		"updated_at":    nullableString(stringValue(payload["updated_at"])),
		"created_log":   nullableJSON(payload["created_log"]),
		"updated_log":   nullableJSON(payload["updated_log"]),
		"language_code": firstNonEmpty(stringValue(payload["language_code"]), "ko"),
	}
}

func (r *ClickHouseReader) queryRows(ctx context.Context, query string) ([]map[string]any, error) {
	endpoint, err := r.endpoint()
	if err != nil {
		return nil, err
	}
	body := strings.TrimSpace(query) + "\nFORMAT JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if strings.TrimSpace(r.cfg.User) != "" || strings.TrimSpace(r.cfg.Password) != "" {
		req.SetBasicAuth(r.cfg.User, r.cfg.Password)
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("clickhouse query failed HTTP %d: %s", resp.StatusCode, string(payload[:minInt(len(payload), 1000)]))
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	rows := make([]map[string]any, 0)
	for {
		row := map[string]any{}
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *ClickHouseReader) exec(ctx context.Context, query string) error {
	return r.post(ctx, strings.TrimSpace(query))
}

func (r *ClickHouseReader) post(ctx context.Context, body string) error {
	endpoint, err := r.endpoint()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if strings.TrimSpace(r.cfg.User) != "" || strings.TrimSpace(r.cfg.Password) != "" {
		req.SetBasicAuth(r.cfg.User, r.cfg.Password)
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("clickhouse statement failed HTTP %d: %s", resp.StatusCode, string(payload[:minInt(len(payload), 1000)]))
	}
	return nil
}

func (r *ClickHouseReader) endpoint() (string, error) {
	rawHost := strings.TrimSpace(r.cfg.Host)
	if rawHost == "" {
		return "", errors.New("CH_HOST is required")
	}
	if strings.HasPrefix(rawHost, "http://") || strings.HasPrefix(rawHost, "https://") {
		parsed, err := url.Parse(rawHost)
		if err != nil {
			return "", err
		}
		q := parsed.Query()
		if strings.TrimSpace(r.cfg.Database) != "" && q.Get("database") == "" {
			q.Set("database", r.cfg.Database)
		}
		parsed.RawQuery = q.Encode()
		return parsed.String(), nil
	}
	host := rawHost
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, strconv.Itoa(r.cfg.Port))
	}
	scheme := "http"
	if r.cfg.Secure {
		scheme = "https"
	}
	endpoint := url.URL{Scheme: scheme, Host: host, Path: "/"}
	q := endpoint.Query()
	if strings.TrimSpace(r.cfg.Database) != "" {
		q.Set("database", r.cfg.Database)
	}
	endpoint.RawQuery = q.Encode()
	return endpoint.String(), nil
}

func rawPayload(rowUUID string, article Article, urlHash string, createdAt time.Time) map[string]any {
	articleLog := compactArticleLog(article, urlHash)
	return map[string]any{
		"uuid":          rowUUID,
		"created_at":    formatClickHouseTime(createdAt),
		"created_log":   map[string]any{"type": "rblogger_crawl", "source": "Statground_Data_R-project", "article": articleLog},
		"updated_at":    nil,
		"updated_log":   nil,
		"active":        1,
		"github_path":   nil,
		"title":         sourceTitle(article),
		"content":       sourceContent(article),
		"url":           firstNonEmpty(article.CanonicalURL, article.URL),
		"url_hash":      urlHash,
		"language_code": "en",
		"canonical_url":       article.CanonicalURL,
		"html_title":          article.HTMLTitle,
		"h1_title":            article.H1Title,
		"meta_description":    article.MetaDescription,
		"meta_keywords":       article.MetaKeywords,
		"og_title":            article.OGTitle,
		"og_description":      article.OGDescription,
		"og_image":            article.OGImage,
		"twitter_title":       article.TwitterTitle,
		"twitter_description": article.TwitterDescription,
		"article_headline":    article.ArticleHeadline,
		"article_section":     article.ArticleSection,
		"article_tags":        article.ArticleTags,
		"article_author":      article.ArticleAuthor,
		"article_published":   article.ArticlePublished,
		"article_modified":    article.ArticleModified,
		"word_count":          article.WordCount,
		"reading_time_min":    article.ReadingTimeMin,
		"internal_links":      firstNMaps(article.InternalLinks, 30),
		"external_links":      firstNMaps(article.ExternalLinks, 30),
		"images":              firstNMaps(article.Images, 30),
		"main_text_excerpt":   truncateRunes(article.MainText, 3000),
	}
}

func boardPayload(rowUUID, rawURL, title, content string, createdAt time.Time) map[string]any {
	return boardPayloadWithUpdated(rowUUID, rawURL, title, content, createdAt, time.Time{})
}

func boardPayloadWithUpdated(rowUUID, rawURL, title, content string, createdAt, updatedAt time.Time) map[string]any {
	title = cleanTitleOutput(title)
	if title == "" {
		title = "R-bloggers"
	}
	content = safeBoardContent(title, content)
	var updated any
	if !updatedAt.IsZero() {
		updated = formatClickHouseTime(updatedAt)
	}
	return map[string]any{
		"uuid":       rowUUID,
		"title":      title,
		"content":    content,
		"active":     1,
		"created_at": formatClickHouseTime(createdAt),
		"updated_at": updated,
		"created_log": map[string]any{
			"type":            "rblogger_board_translation",
			"source":          "Statground_Data_R-project",
			"raw_url":         rawURL,
			"prompt_language": "en",
			"hyperlinks":      "removed",
			"content_fallback": "title_when_blank",
		},
		"updated_log":   nil,
		"language_code": "ko",
	}
}

func translateArticle(ai *AIClient, model string, article Article) (string, string, error) {
	srcTitle := sourceTitle(article)
	srcContent := sourceContent(article)
	translatedTitle := srcTitle
	translatedContent := srcContent
	var err error

	if !looksKorean(srcTitle, 0.20) && srcTitle != "" {
		translatedTitle, err = ai.chat(titlePrompt(srcTitle), model)
		if err != nil {
			return "", "", err
		}
	}
	translatedTitle = cleanTitleOutput(translatedTitle)
	if translatedTitle == "" {
		translatedTitle = cleanTitleOutput(srcTitle)
	}

	if !looksKorean(srcContent, 0.25) && srcContent != "" {
		translatedContent, err = ai.chat(contentPrompt(srcTitle, srcContent), model)
		if err != nil {
			return "", "", err
		}
	}
	translatedContent, err = sanitizeHTMLFragment(translatedContent)
	if err != nil {
		return "", "", err
	}
	if translatedContent == "" {
		translatedContent, err = sanitizeHTMLFragment("<p>" + html.EscapeString(removeURLs(firstNonEmpty(srcContent, translatedTitle))) + "</p>")
		if err != nil {
			return "", "", err
		}
	}
	translatedContent = safeBoardContent(translatedTitle, translatedContent)
	return translatedTitle, translatedContent, nil
}

func newAIClient(timeout time.Duration) *AIClient {
	keys := map[string]string{
		"openrouter":    strings.TrimSpace(os.Getenv("OPENROUTER_API_KEY")),
		"groq":          strings.TrimSpace(os.Getenv("GROQ_API_KEY")),
		"cerebras":      strings.TrimSpace(os.Getenv("CEREBRAS_API_KEY")),
		"github_models": strings.TrimSpace(os.Getenv("GH_MODELS_API_KEY")),
	}
	providers := make([]string, 0, 4)
	for _, provider := range []string{"openrouter", "groq", "cerebras", "github_models"} {
		if strings.TrimSpace(keys[provider]) != "" {
			providers = append(providers, provider)
		}
	}
	return &AIClient{
		httpClient: &http.Client{Timeout: timeout},
		keys:       keys,
		providers:  providers,
		rng:        mrand.New(mrand.NewSource(time.Now().UnixNano())),
	}
}

func (a *AIClient) enabled() bool {
	return len(a.providers) > 0
}

func (a *AIClient) chat(prompt, model string) (string, error) {
	providers := append([]string(nil), a.providers...)
	a.rng.Shuffle(len(providers), func(i, j int) {
		providers[i], providers[j] = providers[j], providers[i]
	})
	errs := make([]string, 0)
	for _, provider := range providers {
		out, err := a.callProvider(provider, prompt, model)
		if err == nil && strings.TrimSpace(out) != "" {
			return strings.TrimSpace(out), nil
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", provider, err))
		}
	}
	return "", errors.New(strings.Join(errs, " | "))
}

func (a *AIClient) callProvider(provider, prompt, model string) (string, error) {
	endpoint, headers, usedModel, err := a.providerRequest(provider, model)
	if err != nil {
		return "", err
	}
	payload := map[string]any{
		"model": usedModel,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"stream": false,
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return "", err
	}
	choices, _ := decoded["choices"].([]any)
	if len(choices) == 0 {
		return "", nil
	}
	first, _ := choices[0].(map[string]any)
	if message, _ := first["message"].(map[string]any); message != nil {
		return stringValue(message["content"]), nil
	}
	return stringValue(first["text"]), nil
}

func (a *AIClient) providerRequest(provider, model string) (string, map[string]string, string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	switch provider {
	case "openrouter":
		headers["Authorization"] = "Bearer " + a.keys[provider]
		return "https://openrouter.ai/api/v1/chat/completions", headers, model, nil
	case "groq":
		headers["Authorization"] = "Bearer " + a.keys[provider]
		return "https://api.groq.com/openai/v1/chat/completions", headers, normalizeGroqModel(model), nil
	case "cerebras":
		headers["Authorization"] = "Bearer " + a.keys[provider]
		return "https://api.cerebras.ai/v1/chat/completions", headers, normalizeCerebrasModel(model), nil
	case "github_models":
		headers["Authorization"] = "Bearer " + a.keys[provider]
		headers["Accept"] = "application/vnd.github+json"
		headers["X-GitHub-Api-Version"] = "2026-03-10"
		return "https://models.github.ai/inference/chat/completions", headers, normalizeGitHubModel(model), nil
	default:
		return "", nil, "", fmt.Errorf("unsupported AI provider: %s", provider)
	}
}

func collectFrontURLs(client *http.Client, cfg Config) ([]string, error) {
	out := make([]string, 0)
	seen := map[string]bool{}
	for page := 1; page <= cfg.MaxPagesFromHome; page++ {
		listURL := cfg.RbloggerHomeURL
		if page > 1 {
			listURL = formatPageURL(cfg.RbloggerPageURL, page)
		}
		pageHTML, err := fetchText(client, listURL)
		if err != nil {
			if len(out) > 0 {
				fmt.Printf("[rblogger] list_partial url=%s err=%s\n", listURL, err)
				break
			}
			return nil, err
		}
		matches := h3LinkRE.FindAllStringSubmatch(pageHTML, -1)
		if len(matches) == 0 {
			matches = fallbackLinkRE.FindAllStringSubmatch(pageHTML, -1)
		}
		if len(matches) == 0 {
			break
		}
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			href := strings.Split(strings.TrimSpace(html.UnescapeString(match[1])), "#")[0]
			if href == "" {
				continue
			}
			resolved := resolveURL(listURL, href)
			if !strings.HasPrefix(resolved, "https://www.r-bloggers.com/20") {
				continue
			}
			canonical := canonicalizeURL(resolved)
			if canonical != "" && !seen[canonical] {
				seen[canonical] = true
				out = append(out, canonical)
				if cfg.MaxURLs > 0 && len(out) >= cfg.MaxURLs {
					return out, nil
				}
			}
		}
	}
	return out, nil
}

func crawlArticle(client *http.Client, articleURL string) (Article, error) {
	src, finalURL, err := fetchTextWithFinalURL(client, articleURL)
	if err != nil {
		return Article{}, err
	}
	canonical := firstNonEmpty(canonicalLink(src), canonicalizeURL(finalURL))
	jsonLD := parseJSONLDArticle(src)
	mainBlock := extractMainBlock(src)
	mainText := htmlToText(mainBlock)
	internalLinks, externalLinks := extractLinks(mainBlock, finalURL)
	images := extractImages(mainBlock, finalURL)

	headline := stringValue(jsonLD["headline"])
	section := stringValue(jsonLD["articleSection"])
	author := extractAuthor(jsonLD["author"])
	published := stringValue(jsonLD["datePublished"])
	modified := stringValue(jsonLD["dateModified"])
	wc := len(wordRE.FindAllString(mainText, -1))

	return Article{
		URL:                canonicalizeURL(finalURL),
		CanonicalURL:       canonical,
		HTMLTitle:          extractTitle(src),
		H1Title:            extractSimpleTagText(src, "h1"),
		MetaDescription:    extractMeta(src, "name", "description"),
		MetaKeywords:       extractMeta(src, "name", "keywords"),
		OGTitle:            extractMeta(src, "property", "og:title"),
		OGDescription:      extractMeta(src, "property", "og:description"),
		OGImage:            extractMeta(src, "property", "og:image"),
		TwitterTitle:       extractMeta(src, "name", "twitter:title"),
		TwitterDescription: extractMeta(src, "name", "twitter:description"),
		ArticleHeadline:    headline,
		ArticleSection:     section,
		ArticleTags:        jsonLD["keywords"],
		ArticleAuthor:      author,
		ArticlePublished:   published,
		ArticleModified:    modified,
		MainText:           mainText,
		WordCount:          wc,
		ReadingTimeMin:     float64(wc) / 200.0,
		InternalLinks:      firstNMaps(internalLinks, 100),
		ExternalLinks:      firstNMaps(externalLinks, 100),
		Images:             firstNMaps(images, 100),
		Lang:               extractHTMLLang(src),
		CrawledAt:          formatClickHouseTime(time.Now()),
	}, nil
}

func titlePrompt(title string) string {
	return fmt.Sprintf(`You are a professional Korean translator for statistics, data science, and programming blog post titles.

Output rules:
- Return exactly one line: the Korean title only.
- Do not include explanations, labels, quotes, Markdown, HTML, URLs, or hyperlinks.
- Preserve meaning and keep the title concise.
- Preserve version numbers, numeric values, acronyms, package names, function names, and proper nouns when appropriate.

Source title:
%s`, title)
}

func contentPrompt(title, content string) string {
	return fmt.Sprintf(`You are an editorial Korean translator for R, statistics, data analysis, and programming blog posts.

Translate and lightly edit the source for a Korean Web-R community board post.

Output rules:
- Return only a compact HTML fragment. The first character must be "<".
- Never use <html>, <head>, or <body>.
- Allowed tags only: <h2>, <h3>, <p>, <ul>, <ol>, <li>, <strong>, <em>, <code>, <pre>, <blockquote>.
- Do not output hyperlinks, URLs, Markdown links, HTML <a> tags, href attributes, citations, source links, or "read more" links.
- If the source contains a link, keep only the human-readable text when it is useful, and omit the URL.
- Use polite formal Korean ending in ~합니다 or ~입니다.
- Preserve technical terms, code, package names, function names, numbers, and version strings unless a natural Korean rendering is obvious.
- Do not add an introduction, explanation, label, or meta-commentary.

Source title:
%s

Source body:
%s`, title, content)
}

func sanitizeHTMLFragment(s string) (string, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```html")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	if !strings.HasPrefix(s, "<") {
		if loc := firstAllowedTagRE.FindStringIndex(s); loc != nil {
			s = strings.TrimSpace(s[loc[0]:])
		} else {
			s = "<p>" + html.EscapeString(removeURLs(s)) + "</p>"
		}
	}
	s = removeBlockTag(s, "script")
	s = removeBlockTag(s, "style")
	s = removeBlockTag(s, "iframe")
	s = removeVoidTag(s, "img")
	anchorRE := regexp.MustCompile(`(?is)<a\b[^>]*>(.*?)</a>`)
	anchorInnerRE := regexp.MustCompile(`(?is)^<a\b[^>]*>(.*?)</a>$`)
	for {
		next := anchorRE.ReplaceAllStringFunc(s, func(match string) string {
			inner := anchorInnerRE.FindStringSubmatch(match)
			if len(inner) < 2 {
				return ""
			}
			return html.EscapeString(removeURLs(htmlToText(inner[1])))
		})
		if next == s {
			break
		}
		s = next
	}
	s = markdownLinkRE.ReplaceAllString(s, "$1")
	s = urlRE.ReplaceAllString(s, "")
	s = tagRE.ReplaceAllStringFunc(s, func(tag string) string {
		match := tagRE.FindStringSubmatch(tag)
		if len(match) < 2 {
			return ""
		}
		name := strings.ToLower(match[1])
		if !allowedContentTags[name] {
			return ""
		}
		if strings.HasPrefix(strings.TrimSpace(tag), "</") {
			return "</" + name + ">"
		}
		return "<" + name + ">"
	})
	s = strings.TrimSpace(s)
	s = regexp.MustCompile(`\s+</`).ReplaceAllString(s, "</")
	s = regexp.MustCompile(`>\s+`).ReplaceAllString(s, ">")
	if !strings.Contains(s, "<") {
		s = "<p>" + html.EscapeString(removeURLs(s)) + "</p>"
	}
	lower := strings.ToLower(s)
	if strings.Contains(lower, "<a") || strings.Contains(lower, "href=") || urlRE.MatchString(s) {
		return "", errors.New("sanitized translation still contains a hyperlink or URL")
	}
	return s, nil
}

func cleanTitleOutput(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	prefixRE := regexp.MustCompile(`(?i)^(translation|translated title|title|result|output|번역|번역문|제목|결과|출력)\s*[:\-]\s*`)
	s = prefixRE.ReplaceAllString(strings.TrimSpace(s), "")
	s = strings.Trim(s, " \t\"'“”‘’")
	return removeURLs(s)
}

func looksKorean(s string, threshold float64) bool {
	compact := strings.Join(strings.Fields(s), "")
	if len([]rune(compact)) < 10 {
		return false
	}
	total := 0
	hangul := 0
	for _, r := range compact {
		total++
		if r >= '가' && r <= '힣' {
			hangul++
		}
	}
	return total > 0 && float64(hangul)/float64(total) >= threshold
}

func removeURLs(s string) string {
	s = markdownLinkRE.ReplaceAllString(s, "$1")
	s = urlRE.ReplaceAllString(s, "")
	return strings.TrimSpace(spaceRE.ReplaceAllString(s, " "))
}

func safeBoardContent(title, content string) string {
	content = strings.TrimSpace(content)
	if content != "" && !htmlFragmentBlank(content) {
		return content
	}
	fallback := firstNonEmpty(title, "R-bloggers")
	cleaned, err := sanitizeHTMLFragment("<p>" + html.EscapeString(removeURLs(fallback)) + "</p>")
	if err == nil && strings.TrimSpace(cleaned) != "" && !htmlFragmentBlank(cleaned) {
		return cleaned
	}
	return "<p>" + html.EscapeString(fallback) + "</p>"
}

func htmlFragmentBlank(s string) bool {
	return strings.TrimSpace(htmlToText(s)) == ""
}

type httpStatusError struct {
	StatusCode int
	Body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

func fetchText(client *http.Client, rawURL string) (string, error) {
	text, _, err := fetchTextWithFinalURL(client, rawURL)
	return text, err
}

func fetchTextWithFinalURL(client *http.Client, rawURL string) (string, string, error) {
	attempts := maxInt(1, envInt("RBLOGGER_HTTP_ATTEMPTS", envInt("HTTP_ATTEMPTS", 4)))
	delay := envFloatDuration("RBLOGGER_HTTP_RETRY_DELAY_SEC", 2.0)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		text, finalURL, err := fetchTextWithFinalURLOnce(client, rawURL)
		if err == nil {
			return text, finalURL, nil
		}
		lastErr = err
		if attempt >= attempts || !retryableFetchError(err) {
			break
		}
		sleep := delay + time.Duration(mrand.Int63n(int64(maxDuration(delay, 250*time.Millisecond))))
		fmt.Printf("[rblogger] transient_http_error url=%s attempt=%d/%d err=%s retry_in=%s\n", rawURL, attempt, attempts, err, sleep)
		time.Sleep(sleep)
	}
	return "", "", lastErr
}

func fetchTextWithFinalURLOnce(client *http.Client, rawURL string) (string, string, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", "", &httpStatusError{StatusCode: resp.StatusCode, Body: string(payload[:minInt(len(payload), 500)])}
	}
	return string(payload), resp.Request.URL.String(), nil
}

func retryableFetchError(err error) bool {
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return retryableHTTPStatus(statusErr.StatusCode)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "temporary failure") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "tls handshake timeout")
}

func retryableHTTPStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		520, 521, 522, 523, 524:
		return true
	default:
		return false
	}
}

func extractMeta(src, attrName, attrValue string) string {
	attrValue = strings.ToLower(attrValue)
	for _, tag := range metaTagRE.FindAllString(src, -1) {
		if strings.ToLower(getAttr(tag, attrName)) == attrValue {
			return strings.TrimSpace(html.UnescapeString(getAttr(tag, "content")))
		}
	}
	return ""
}

func canonicalLink(src string) string {
	for _, tag := range linkTagRE.FindAllString(src, -1) {
		if strings.Contains(strings.ToLower(getAttr(tag, "rel")), "canonical") {
			return canonicalizeURL(getAttr(tag, "href"))
		}
	}
	return ""
}

func extractTitle(src string) string {
	return extractSimpleTagText(src, "title")
}

func extractSimpleTagText(src, tag string) string {
	re := regexp.MustCompile(`(?is)<` + regexp.QuoteMeta(tag) + `\b[^>]*>(.*?)</` + regexp.QuoteMeta(tag) + `>`)
	match := re.FindStringSubmatch(src)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(htmlToText(match[1]))
}

func extractHTMLLang(src string) string {
	re := regexp.MustCompile(`(?is)<html\b[^>]*>`)
	if tag := re.FindString(src); tag != "" {
		return getAttr(tag, "lang")
	}
	return ""
}

func parseJSONLDArticle(src string) map[string]any {
	for _, match := range scriptTagRE.FindAllStringSubmatch(src, -1) {
		if len(match) < 2 {
			continue
		}
		tag := strings.Split(match[0], ">")[0] + ">"
		if !strings.Contains(strings.ToLower(getAttr(tag, "type")), "application/ld+json") {
			continue
		}
		var decoded any
		if err := json.Unmarshal([]byte(strings.TrimSpace(html.UnescapeString(match[1]))), &decoded); err != nil {
			continue
		}
		if found := findArticleJSONLD(decoded); found != nil {
			return found
		}
	}
	return map[string]any{}
}

func findArticleJSONLD(v any) map[string]any {
	switch x := v.(type) {
	case map[string]any:
		if isArticleType(x["@type"]) {
			return x
		}
		if graph, ok := x["@graph"]; ok {
			if found := findArticleJSONLD(graph); found != nil {
				return found
			}
		}
		for _, value := range x {
			if found := findArticleJSONLD(value); found != nil {
				return found
			}
		}
	case []any:
		for _, item := range x {
			if found := findArticleJSONLD(item); found != nil {
				return found
			}
		}
	}
	return nil
}

func isArticleType(v any) bool {
	switch x := v.(type) {
	case string:
		t := strings.ToLower(x)
		return t == "article" || t == "blogposting"
	case []any:
		for _, item := range x {
			if isArticleType(item) {
				return true
			}
		}
	}
	return false
}

func extractAuthor(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		return firstNonEmpty(stringValue(x["name"]), stringValue(x["@id"]))
	case []any:
		if len(x) > 0 {
			return extractAuthor(x[0])
		}
	}
	return ""
}

func extractMainBlock(src string) string {
	if block := regexp.MustCompile(`(?is)<article\b[^>]*>.*?</article>`).FindString(src); block != "" {
		return block
	}
	if block := regexp.MustCompile(`(?is)<body\b[^>]*>.*?</body>`).FindString(src); block != "" {
		return block
	}
	return src
}

func htmlToText(src string) string {
	src = htmlCommentRE.ReplaceAllString(src, "")
	for _, tag := range []string{"script", "style", "nav", "footer", "aside"} {
		src = removeBlockTag(src, tag)
	}
	src = regexp.MustCompile(`(?is)<br\s*/?>`).ReplaceAllString(src, "\n")
	src = regexp.MustCompile(`(?is)</(p|h1|h2|h3|h4|li|div|blockquote|pre)>`).ReplaceAllString(src, "\n")
	src = tagRE.ReplaceAllString(src, " ")
	src = html.UnescapeString(src)
	src = strings.ReplaceAll(src, "\r\n", "\n")
	src = strings.ReplaceAll(src, "\r", "\n")
	lines := strings.Split(src, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(spaceRE.ReplaceAllString(line, " "))
		if line != "" {
			cleaned = append(cleaned, line)
		}
	}
	return strings.TrimSpace(blankLinesRE.ReplaceAllString(strings.Join(cleaned, "\n"), "\n\n"))
}

func removeBlockTag(src, tag string) string {
	re := regexp.MustCompile(`(?is)<` + tag + `\b[^>]*>.*?</` + tag + `>`)
	return re.ReplaceAllString(src, "")
}

func removeVoidTag(src, tag string) string {
	re := regexp.MustCompile(`(?is)<` + tag + `\b[^>]*>`)
	return re.ReplaceAllString(src, "")
}

func extractLinks(src, baseURL string) ([]map[string]string, []map[string]string) {
	internal := make([]map[string]string, 0)
	external := make([]map[string]string, 0)
	baseHost := ""
	if parsed, err := url.Parse(baseURL); err == nil {
		baseHost = parsed.Host
	}
	for _, tag := range fallbackLinkRE.FindAllString(src, -1) {
		href := resolveURL(baseURL, getAttr(tag, "href"))
		if href == "" {
			continue
		}
		row := map[string]string{"href": href, "text": htmlToText(tag)}
		if parsed, err := url.Parse(href); err == nil && parsed.Host == baseHost {
			internal = append(internal, row)
		} else {
			external = append(external, row)
		}
	}
	return internal, external
}

func extractImages(src, baseURL string) []map[string]string {
	re := regexp.MustCompile(`(?is)<img\b[^>]*>`)
	out := make([]map[string]string, 0)
	for _, tag := range re.FindAllString(src, -1) {
		img := firstNonEmpty(getAttr(tag, "data-lazy-src"), getAttr(tag, "data-src"), getAttr(tag, "src"))
		if img == "" {
			continue
		}
		out = append(out, map[string]string{
			"src": resolveURL(baseURL, img),
			"alt": strings.TrimSpace(html.UnescapeString(getAttr(tag, "alt"))),
		})
	}
	return out
}

func getAttr(tag, name string) string {
	name = strings.ToLower(name)
	for _, match := range attrRE.FindAllStringSubmatch(tag, -1) {
		if len(match) < 6 || strings.ToLower(match[1]) != name {
			continue
		}
		for _, idx := range []int{3, 4, 5} {
			if match[idx] != "" {
				return html.UnescapeString(match[idx])
			}
		}
	}
	return ""
}

func sourceTitle(a Article) string {
	return strings.TrimSpace(firstNonEmpty(a.ArticleHeadline, a.OGTitle, a.HTMLTitle, a.H1Title))
}

func sourceContent(a Article) string {
	value := strings.TrimSpace(firstNonEmpty(a.MetaDescription, a.OGDescription, a.TwitterDescription, a.MainText))
	if len([]rune(value)) <= 5000 {
		return value
	}
	return string([]rune(value)[:5000])
}

func compactArticleLog(a Article, hash string) any {
	row := map[string]any{
		"url":                 a.URL,
		"canonical_url":       a.CanonicalURL,
		"html_title":          a.HTMLTitle,
		"h1_title":            a.H1Title,
		"meta_description":    a.MetaDescription,
		"meta_keywords":       a.MetaKeywords,
		"og_title":            a.OGTitle,
		"og_description":      a.OGDescription,
		"og_image":            a.OGImage,
		"twitter_title":       a.TwitterTitle,
		"twitter_description": a.TwitterDescription,
		"article_headline":    a.ArticleHeadline,
		"article_section":     a.ArticleSection,
		"article_tags":        a.ArticleTags,
		"article_author":      a.ArticleAuthor,
		"article_published":   a.ArticlePublished,
		"article_modified":    a.ArticleModified,
		"word_count":          a.WordCount,
		"reading_time_min":    a.ReadingTimeMin,
		"lang":                a.Lang,
		"crawled_at":          a.CrawledAt,
		"url_hash":            hash,
		"main_text_excerpt":   truncateRunes(a.MainText, 3000),
		"internal_links":      firstNMaps(a.InternalLinks, 30),
		"external_links":      firstNMaps(a.ExternalLinks, 30),
		"images":              firstNMaps(a.Images, 30),
	}
	payload, _ := json.Marshal(row)
	if len(payload) <= 6000 {
		return row
	}
	return map[string]any{"truncated": true, "json_prefix": string(payload[:6000])}
}

func canonicalizeURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.Scheme = "https"
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Fragment = ""
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	q := url.Values{}
	for key, values := range parsed.Query() {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "utm_") || lower == "ref" || lower == "source" || lower == "fbclid" || lower == "gclid" {
			continue
		}
		for _, value := range values {
			q.Add(key, value)
		}
	}
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

func resolveURL(baseURL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" {
		return ""
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return href
	}
	ref, err := url.Parse(href)
	if err != nil {
		return href
	}
	return base.ResolveReference(ref).String()
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", sum[:])
}

func deterministicUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func newUUID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic(err)
	}
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func nowKST() time.Time {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		return time.Now()
	}
	return time.Now().In(loc)
}

func formatClickHouseTime(t time.Time) string {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err == nil {
		t = t.In(loc)
	}
	return t.Format("2006-01-02 15:04:05.000")
}

func parseClickHouseTime(value string, fallback time.Time) time.Time {
	value = normalizeClickHouseTimeString(value)
	if value == "" {
		return fallback
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		loc = time.FixedZone("KST", 9*3600)
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if t, err := time.ParseInLocation(layout, value, loc); err == nil {
			return t
		}
		if t, err := time.Parse(layout, value); err == nil {
			return t.In(loc)
		}
	}
	return fallback
}

func normalizeClickHouseTimeString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, ".") {
		parts := strings.SplitN(value, ".", 2)
		frac := parts[1]
		if len(frac) > 3 {
			frac = frac[:3]
		}
		for len(frac) < 3 {
			frac += "0"
		}
		return parts[0] + "." + frac
	}
	return value
}

func formatPageURL(pattern string, page int) string {
	if strings.Contains(pattern, "%") {
		return fmt.Sprintf(pattern, page)
	}
	return strings.ReplaceAll(pattern, "{page}", strconv.Itoa(page))
}

func normalizeGroqModel(model string) string {
	model = strings.TrimSuffix(firstNonEmpty(model, "openai/gpt-oss-20b"), ":free")
	if strings.HasPrefix(model, "google/") || strings.HasPrefix(model, "anthropic/") || strings.HasPrefix(model, "x-ai/") {
		return "openai/gpt-oss-20b"
	}
	return model
}

func normalizeCerebrasModel(model string) string {
	model = strings.TrimSuffix(firstNonEmpty(model, "gpt-oss-120b"), ":free")
	switch model {
	case "", "openai/gpt-oss-20b", "openai/gpt-oss-120b", "gpt-oss-20b":
		return "gpt-oss-120b"
	default:
		if strings.Contains(model, "/") {
			return "gpt-oss-120b"
		}
		return model
	}
}

func normalizeGitHubModel(model string) string {
	model = strings.TrimSuffix(firstNonEmpty(model, "openai/gpt-4.1"), ":free")
	switch model {
	case "", "openai/gpt-oss-20b", "openai/gpt-oss-120b", "gpt-oss-20b", "gpt-oss-120b":
		return "openai/gpt-4.1"
	default:
		if strings.Contains(model, "/") {
			return model
		}
		return "openai/gpt-4.1"
	}
}

func isLoopbackBrokerEndpoint(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		host = strings.TrimSpace(raw)
		if strings.Contains(host, ":") {
			host = strings.Split(host, ":")[0]
		}
	}
	return isLoopbackHost(host)
}

func validateKafkaAdvertisedLeaders(partitions []kafka.Partition, brokers []string, label string) error {
	bootstrap := kafkaBootstrapEndpointSet(brokers)
	nonBootstrapLeaders := 0
	topics := map[string]bool{}
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("%s advertises loopback listener %s:%d for topic=%s partition=%d; fix Kafka server KAFKA_PUBLIC_HOST/KAFKA_ADVERTISED_LISTENERS and force-recreate Kafka_Platform", label, leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
		leaderEndpoint := normalizedKafkaEndpoint(leaderHost, fmt.Sprint(partition.Leader.Port))
		if len(bootstrap) > 0 && !bootstrap[leaderEndpoint] {
			nonBootstrapLeaders++
			topics[partition.Topic] = true
		}
	}
	if nonBootstrapLeaders > 0 {
		fmt.Printf("[kafka] %s metadata has %d non-bootstrap advertised broker entries across %d topic(s); producer will dial via bootstrap rewrite\n", label, nonBootstrapLeaders, len(topics))
	}
	return nil
}

func kafkaBootstrapEndpointSet(brokers []string) map[string]bool {
	endpoints := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		host, port, ok := splitKafkaEndpoint(broker)
		if ok {
			endpoints[normalizedKafkaEndpoint(host, port)] = true
		}
	}
	return endpoints
}

func splitKafkaEndpoint(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if strings.Count(raw, ":") != 1 {
			return "", "", false
		}
		parts := strings.SplitN(raw, ":", 2)
		host, port = parts[0], parts[1]
	}
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	return host, port, host != "" && port != ""
}

func normalizedKafkaEndpoint(host, port string) string {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	port = strings.TrimSpace(port)
	return host + ":" + port
}

func kafkaAdvertisedBrokerDialFunc(brokers []string, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if len(brokers) != 1 {
		return dialer.DialContext
	}
	bootstrapHost, bootstrapPort, ok := splitKafkaEndpoint(brokers[0])
	if !ok {
		return dialer.DialContext
	}
	bootstrapAddress := net.JoinHostPort(strings.Trim(bootstrapHost, "[]"), bootstrapPort)
	bootstrapEndpoint := normalizedKafkaEndpoint(bootstrapHost, bootstrapPort)
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		target := address
		if host, port, ok := splitKafkaEndpoint(address); ok {
			endpoint := normalizedKafkaEndpoint(host, port)
			if port == bootstrapPort && endpoint != bootstrapEndpoint {
				target = bootstrapAddress
			}
		}
		return dialer.DialContext(ctx, network, target)
	}
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	switch host {
	case "", "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envFloatDuration(key string, fallback float64) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return time.Duration(fallback * float64(time.Second))
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil {
		n = fallback
	}
	return time.Duration(n * float64(time.Second))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonEmptyValue(values ...any) any {
	for _, value := range values {
		if value == nil {
			continue
		}
		if strings.TrimSpace(stringValue(value)) != "" {
			return value
		}
		switch typed := value.(type) {
		case []any:
			if len(typed) > 0 {
				return value
			}
		case map[string]any:
			if len(typed) > 0 {
				return value
			}
		}
	}
	return nil
}

func kafkaSecurityUsesTLS(value string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	return value == "SSL" || value == "SASL_SSL" || envBool("KAFKA_TLS", false)
}

func kafkaTLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

func stringValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

func mapValue(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func nullableString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" || value == "<nil>" {
		return nil
	}
	return value
}

func nullableJSON(value any) any {
	if value == nil {
		return nil
	}
	if strings.TrimSpace(stringValue(value)) == "" && len(mapValue(value)) == 0 {
		return nil
	}
	return value
}

func nullableUInt8(value any) any {
	if value == nil || strings.TrimSpace(stringValue(value)) == "" {
		return nil
	}
	return uint8Value(value)
}

func uint8Value(value any) int {
	if boolValue(value) {
		return 1
	}
	n := int64Value(value)
	if n < 0 {
		return 0
	}
	if n > 255 {
		return 255
	}
	return int(n)
}

func uint32Value(value any) uint32 {
	n := int64Value(value)
	if n < 0 {
		return 0
	}
	if n > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(n)
}

func float32Value(value any) float32 {
	switch typed := value.(type) {
	case float32:
		return typed
	case float64:
		return float32(typed)
	case int:
		return float32(typed)
	case int64:
		return float32(typed)
	}
	if n, err := strconv.ParseFloat(strings.TrimSpace(stringValue(value)), 32); err == nil {
		return float32(n)
	}
	return 0
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case int32:
		return int64(typed)
	case uint:
		return int64(typed)
	case uint64:
		if typed <= uint64(^uint64(0)>>1) {
			return int64(typed)
		}
	case float64:
		return int64(typed)
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(stringValue(value)), 10, 64); err == nil {
		return n
	}
	return 0
}

func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "y":
			return true
		}
	}
	return false
}

func jsonString(value any, fallback string) string {
	if value == nil {
		return fallback
	}
	if raw := strings.TrimSpace(stringValue(value)); raw != "" && (strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[")) {
		return raw
	}
	body, err := json.Marshal(value)
	if err != nil {
		return fallback
	}
	out := strings.TrimSpace(string(body))
	if out == "" || out == "null" {
		return fallback
	}
	return out
}

func firstN(values []string, n int) []string {
	if len(values) <= n {
		return values
	}
	return values[:n]
}

func firstNMaps(values []map[string]string, n int) []map[string]string {
	if len(values) <= n {
		return values
	}
	return values[:n]
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func chunkRawArticles(rows []StaleRawArticle, n int) [][]StaleRawArticle {
	if n <= 0 {
		n = 50
	}
	chunks := make([][]StaleRawArticle, 0, (len(rows)+n-1)/n)
	for start := 0; start < len(rows); start += n {
		end := minInt(start+n, len(rows))
		chunks = append(chunks, rows[start:end])
	}
	return chunks
}

func chunkPayloads(rows []map[string]any, n int) [][]map[string]any {
	if n <= 0 {
		n = 50
	}
	chunks := make([][]map[string]any, 0, (len(rows)+n-1)/n)
	for start := 0; start < len(rows); start += n {
		end := minInt(start+n, len(rows))
		chunks = append(chunks, rows[start:end])
	}
	return chunks
}

func sqlString(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
