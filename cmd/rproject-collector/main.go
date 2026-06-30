package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	defaultPackageTopic           = "rpkg.events"
	defaultYouTubeTopic           = "r.youtube.events"
	defaultCommunityTopic         = "r.community.events"
	defaultWebRTopic              = "webr.events"
	userAgent                     = "StatgroundBot/1.0 (+https://www.statground.net; R ecosystem collector)"
	youtubeBoilerplateDescription = "Enjoy the videos and music you love, upload original content, and share it all with friends, family, and the world on YouTube."
)

var (
	tagRE             = regexp.MustCompile(`(?is)<[^>]+>`)
	spaceRE           = regexp.MustCompile(`[ \t\r\n\x{00a0}]+`)
	trRE              = regexp.MustCompile(`(?is)<tr\b[^>]*>(.*?)</tr>`)
	cellRE            = regexp.MustCompile(`(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>`)
	linkRE            = regexp.MustCompile(`(?is)<a\b[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>`)
	titleRE           = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
	metaRE            = regexp.MustCompile(`(?is)<meta\b([^>]*)>`)
	attrRE            = regexp.MustCompile(`(?is)\s([a-zA-Z_:][-a-zA-Z0-9_:]*)\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))`)
	headingRE         = regexp.MustCompile(`(?is)<h[1-4]\b[^>]*>(.*?)</h[1-4]>`)
	paragraphRE       = regexp.MustCompile(`(?is)<p\b[^>]*>(.*?)</p>`)
	listItemRE        = regexp.MustCompile(`(?is)<li\b[^>]*>(.*?)</li>`)
	youtubeURLRE      = regexp.MustCompile(`https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s"'<>]+`)
	ytWatchRE         = regexp.MustCompile(`(?:/watch\?v=|watch\\u003fv=|watch\\\?v=)([A-Za-z0-9_-]{11})`)
	ytChannelRE       = regexp.MustCompile(`/(channel/[A-Za-z0-9_-]+|@[A-Za-z0-9._-]+|c/[A-Za-z0-9._-]+|user/[A-Za-z0-9._-]+)`)
	ytPlaylistRE      = regexp.MustCompile(`(?:/playlist\?list=|playlist\\u003flist=|playlist\\\?list=)([A-Za-z0-9_-]+)`)
	githubRepoRE      = regexp.MustCompile(`(?i)^https?://(?:www\.)?github\.com/([^/\s?#]+)/([^/\s?#]+)`)
	cranPackageLinkRE = regexp.MustCompile(`(?i)(?:/web/packages/|\.\./packages/)([^/\s?#"']+)/?`)
	isoDurationRE     = regexp.MustCompile(`^P(?:\d+D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$`)
	kafkaIPv4RE       = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b`)
	depVersionRE      = regexp.MustCompile(`\s*\(.*?\)\s*`)
	boardURLRE        = regexp.MustCompile(`(?i)\b(?:https?://|www\.)[^\s<>()"']+`)
	boardMdLinkRE     = regexp.MustCompile(`\[([^\]]+)\]\((?:https?://|www\.)[^)]+\)`)
	boardFirstTagRE   = regexp.MustCompile(`(?is)<\s*(h2|h3|p|ul|ol|li|strong|em|code|pre|blockquote)\b`)
	boardAnyTagRE     = regexp.MustCompile(`(?is)</?\s*([a-zA-Z0-9]+)\b[^>]*>`)
	boardAnchorRE     = regexp.MustCompile(`(?is)<a\b[^>]*>(.*?)</a>`)
	boardAnchorOnlyRE = regexp.MustCompile(`(?is)^<a\b[^>]*>(.*?)</a>$`)
	sitemapLocRE      = regexp.MustCompile(`(?is)<loc>\s*([^<]+?)\s*</loc>`)
	rdCommandRE       = regexp.MustCompile(`\\[A-Za-z]+`)
	enAnniversaryRE   = regexp.MustCompile(`(?i)\b([0-9]{1,3})(?:st|nd|rd|th)?\s*(?:years?|anniversar(?:y|ies))\b`)
	koAnniversaryRE   = regexp.MustCompile(`([0-9]{1,3})\s*주년`)
	statusOrder       = []string{"ERROR", "FAIL", "WARNING", "NOTE", "OK"}
	newsCandidates    = []string{"news/news.html", "news.html"}
	defaultWebsites   = []string{
		"https://www.r-project.org/",
		"https://cran.r-project.org/",
		"https://cran.r-project.org/web/views/",
		"https://www.bioconductor.org/",
		"https://r-universe.dev/",
		"https://www.r-bloggers.com/",
		"https://rweekly.org/",
		"https://posit.co/blog/",
		"https://www.tidyverse.org/blog/",
		"https://ropensci.org/blog/",
		"https://www.r-consortium.org/news/blogs",
	}
	defaultCommunityRequiredSourceIDs = []string{
		"community:stackoverflow:r",
		"community:posit:latest-r-filtered",
		"reddit:r/rstats",
		"reddit:r/rprogramming",
	}
	defaultCommunityPrioritySourceIDs = []string{
		"community:posit:events",
	}
)

var boardAllowedTags = map[string]bool{
	"h2": true, "h3": true, "p": true, "ul": true, "ol": true, "li": true,
	"strong": true, "em": true, "mark": true, "code": true, "pre": true, "blockquote": true,
}

type genericEvent struct {
	EventID        string `json:"event_id"`
	EventType      string `json:"event_type"`
	SchemaVersion  int    `json:"schema_version"`
	Source         string `json:"source"`
	SourceURL      string `json:"source_url"`
	Repository     string `json:"repository"`
	PackageName    string `json:"package_name"`
	PackageVersion string `json:"package_version"`
	ObservedAt     string `json:"observed_at"`
	CollectedAt    string `json:"collected_at"`
	PayloadHash    string `json:"payload_hash"`
	Payload        string `json:"payload"`
}

type webREvent struct {
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

type clickHouseQueryConfig struct {
	Host                  string
	Port                  int
	User                  string
	Password              string
	Database              string
	Secure                bool
	Timeout               time.Duration
	InsertDistributedSync bool
}

type communityDigestItem struct {
	ExternalID   string `json:"external_id"`
	Title        string `json:"title"`
	CanonicalURL string `json:"canonical_url"`
	Author       string `json:"author,omitempty"`
	PublishedAt  string `json:"published_at,omitempty"`
	SourceName   string `json:"source_name,omitempty"`
	Context      string `json:"context,omitempty"`
}

type communityDigestRecord struct {
	DigestID         string
	DigestUUID       string
	DigestDate       string
	SourceType       string
	SourceID         string
	SourceName       string
	Platform         string
	SourceURL        string
	Title            string
	Summary          string
	ItemCount        int
	DedupedItemCount int
	Items            []communityDigestItem
	Model            string
	PromptHash       string
	Status           string
	GeneratedAt      string
	PayloadHash      string
}

type communityDigestPlan struct {
	GeneratedAt string                      `json:"generated_at"`
	RecordCount int                         `json:"record_count"`
	DigestIDs   []string                    `json:"digest_ids"`
	Records     []communityDigestPlanRecord `json:"records"`
}

type communityDigestPlanRecord struct {
	DigestID   string `json:"digest_id"`
	DigestUUID string `json:"digest_uuid"`
	DigestDate string `json:"digest_date"`
	SourceType string `json:"source_type"`
	SourceID   string `json:"source_id"`
	SourceName string `json:"source_name"`
	Platform   string `json:"platform"`
	ItemCount  int    `json:"item_count"`
	Status     string `json:"generation_status"`
}

type publisher struct {
	topic              string
	brokers            []string
	username           string
	password           string
	security           string
	clientID           string
	dryRun             bool
	publishMode        string
	writeTimeout       time.Duration
	chunkSize          int
	createTopic        bool
	partitions         int
	replicas           int
	writerMaxAttempts  int
	writeAttempts      int
	writeBackoffMin    time.Duration
	writeBackoffMax    time.Duration
	partitionFallback  bool
	fallbackPartitions []int
	knownPartitions    []int
	fallbackTimeout    time.Duration
}

type aiClient struct {
	httpClient *http.Client
	keys       map[string]string
	providers  []string
}

type mastodonDedupState struct {
	raw           map[string]bool
	rawByURL      map[string]string
	translated    map[string]bool
	translatedURL map[string]bool
}

type cranRecord map[string]string

type rssFeed struct {
	Channel struct {
		Title string    `xml:"title"`
		Link  string    `xml:"link"`
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	GUID        string `xml:"guid"`
	PubDate     string `xml:"pubDate"`
	Description string `xml:"description"`
}

type atomFeed struct {
	Title   string      `xml:"title"`
	Links   []atomLink  `xml:"link"`
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Title     string     `xml:"title"`
	Links     []atomLink `xml:"link"`
	ID        string     `xml:"id"`
	Updated   string     `xml:"updated"`
	Published string     `xml:"published"`
	Summary   string     `xml:"summary"`
	Content   string     `xml:"content"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: rproject-collector <package|youtube|community|community-digest|mastodon> [flags]"))
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "package":
		err = runPackage(ctx, os.Args[2:])
	case "youtube":
		err = runYouTube(ctx, os.Args[2:])
	case "community":
		err = runCommunity(ctx, os.Args[2:])
	case "community-digest":
		err = runCommunityDigest(ctx, os.Args[2:])
	case "mastodon":
		err = runMastodon(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatal(err)
	}
}

func runPackage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("package", flag.ExitOnError)
	job := fs.String("job", envString("RPKG_JOB", "all"), "all or comma-separated package jobs")
	topic := fs.String("topic", envString("RPKG_KAFKA_TOPIC", defaultPackageTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	metadataLimit := fs.Int("metadata-limit", envInt("RPKG_CRAN_METADATA_LIMIT", 0), "CRAN metadata limit")
	downloadTop := fs.Int("download-top", envInt("RPKG_DOWNLOAD_TOP", 100), "cranlogs top package count")
	reverseLimit := fs.Int("reverse-limit", envInt("RPKG_REVERSE_DEPENDENCY_LIMIT", 0), "reverse dependency edge limit")
	checkLimit := fs.Int("check-limit", envInt("RPKG_CRAN_CHECK_LIMIT", 0), "CRAN check row limit")
	archiveLimit := fs.Int("archive-limit", envInt("RPKG_CRAN_ARCHIVE_LIMIT", 0), "CRAN archive row limit")
	taskViewLimit := fs.Int("task-view-limit", envInt("RPKG_CRAN_TASK_VIEW_LIMIT", 0), "CRAN Task View page limit")
	newsLimit := fs.Int("package-news-limit", envInt("RPKG_PACKAGE_NEWS_LIMIT", 50), "package NEWS page limit")
	packagePageLimit := fs.Int("package-page-limit", envInt("RPKG_CRAN_PACKAGE_PAGE_LIMIT", 0), "CRAN package index.html page limit; 0 means all")
	packagePagePackages := fs.String("package-page-packages", envString("RPKG_CRAN_PACKAGE_PAGE_PACKAGES", ""), "comma-separated CRAN package names to always include in package page collection")
	packageArtifactLimit := fs.Int("package-artifact-limit", envInt("RPKG_CRAN_PACKAGE_ARTIFACT_LIMIT", -1), "CRAN package linked artifact fetch limit per package; negative disables, 0 means all")
	packageManualTopicLimit := fs.Int("package-manual-topic-limit", envInt("RPKG_CRAN_PACKAGE_MANUAL_TOPIC_LIMIT", -1), "CRAN package Rd manual topic limit per package; negative disables, 0 means all")
	bioconductorPackagePageLimit := fs.Int("bioconductor-package-page-limit", envInt("RPKG_BIOCONDUCTOR_PACKAGE_PAGE_LIMIT", 0), "Bioconductor package HTML page collection limit; 0 means all")
	bioconductorPackagePagePackages := fs.String("bioconductor-package-page-packages", envString("RPKG_BIOCONDUCTOR_PACKAGE_PAGE_PACKAGES", ""), "comma-separated Bioconductor package names to always include in package page collection")
	bioconductorPackageArtifactLimit := fs.Int("bioconductor-package-artifact-limit", envInt("RPKG_BIOCONDUCTOR_PACKAGE_ARTIFACT_LIMIT", -1), "Bioconductor package linked artifact fetch limit per package; negative disables, 0 means all")
	bioconductorPackageManualTopicLimit := fs.Int("bioconductor-package-manual-topic-limit", envInt("RPKG_BIOCONDUCTOR_PACKAGE_MANUAL_TOPIC_LIMIT", -1), "Bioconductor package Rd manual topic limit per package; negative disables, 0 means all")
	runiversePackagePageLimit := fs.Int("runiverse-package-page-limit", envInt("RPKG_RUNIVERSE_PACKAGE_PAGE_LIMIT", 0), "R-universe package HTML/API page collection limit; 0 means all")
	runiversePackagePagePackages := fs.String("runiverse-package-page-packages", envString("RPKG_RUNIVERSE_PACKAGE_PAGE_PACKAGES", ""), "comma-separated R-universe package names to always include in package page collection")
	runiversePackageArtifactLimit := fs.Int("runiverse-package-artifact-limit", envInt("RPKG_RUNIVERSE_PACKAGE_ARTIFACT_LIMIT", -1), "R-universe package linked artifact fetch limit per package; negative disables, 0 means all")
	runiversePackageManualTopicLimit := fs.Int("runiverse-package-manual-topic-limit", envInt("RPKG_RUNIVERSE_PACKAGE_MANUAL_TOPIC_LIMIT", -1), "R-universe package Rd manual topic limit per package; negative disables, 0 means all")
	websiteLimit := fs.Int("website-limit", envInt("RPKG_R_WEBSITE_LIMIT", 0), "website seed limit")
	websiteCandidateLimit := fs.Int("website-candidate-limit", envInt("RPKG_R_WEBSITE_CANDIDATE_LIMIT", 120), "CRAN DESCRIPTION website candidate limit")
	websiteFeedLimit := fs.Int("website-feed-limit", envInt("RPKG_R_WEBSITE_FEED_LIMIT", 40), "website feed item limit")
	websiteLinkLimit := fs.Int("website-link-limit", envInt("RPKG_R_WEBSITE_LINK_LIMIT", 120), "website link edge limit")
	websiteSitemapLimit := fs.Int("website-sitemap-limit", envInt("RPKG_R_WEBSITE_SITEMAP_LIMIT", 40), "website sitemap URL limit")
	githubLimit := fs.Int("github-limit", envInt("RPKG_GITHUB_LIMIT", 60), "GitHub repository metadata limit")
	osvLimit := fs.Int("osv-limit", envInt("RPKG_OSV_LIMIT", 100), "OSV package query limit")
	bibliometricLimit := fs.Int("bibliometric-limit", envInt("RPKG_BIBLIOMETRIC_LIMIT", 40), "OpenAlex bibliometric query limit")
	fs.Parse(args)

	pub := newPublisher(*topic, "statground-rpkg-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}

	jobs := expandJobs(*job, []string{
		"cran-metadata",
		"cran-downloads",
		"cran-reverse-dependencies",
		"cran-checks",
		"cran-archive",
		"cran-task-views",
		"r-core-news",
		"package-news",
		"cran-package-pages",
		"bioconductor",
		"bioconductor-package-pages",
		"runiverse",
		"runiverse-package-pages",
		"cran-website-discovery",
		"r-websites",
		"github-repos",
		"osv-security",
		"bibliometric-mentions",
	})
	recordsCache := []cranRecord(nil)
	getRecords := func() ([]cranRecord, error) {
		if recordsCache != nil {
			return recordsCache, nil
		}
		records, err := fetchCRANPackages()
		if err != nil {
			return nil, err
		}
		recordsCache = records
		return recordsCache, nil
	}

	total := 0
	deferred := 0
	for _, currentJob := range jobs {
		events, err := collectPackageJob(currentJob, getRecords, packageJobLimits{
			metadataLimit:                       *metadataLimit,
			downloadTop:                         *downloadTop,
			reverseLimit:                        *reverseLimit,
			checkLimit:                          *checkLimit,
			archiveLimit:                        *archiveLimit,
			taskViewLimit:                       *taskViewLimit,
			newsLimit:                           *newsLimit,
			packagePageLimit:                    *packagePageLimit,
			packagePagePackages:                 splitCSV(*packagePagePackages),
			packageArtifactLimit:                *packageArtifactLimit,
			packageManualTopicLimit:             *packageManualTopicLimit,
			bioconductorPackagePageLimit:        *bioconductorPackagePageLimit,
			bioconductorPackagePagePackages:     splitCSV(*bioconductorPackagePagePackages),
			bioconductorPackageArtifactLimit:    *bioconductorPackageArtifactLimit,
			bioconductorPackageManualTopicLimit: *bioconductorPackageManualTopicLimit,
			runiversePackagePageLimit:           *runiversePackagePageLimit,
			runiversePackagePagePackages:        splitCSV(*runiversePackagePagePackages),
			runiversePackageArtifactLimit:       *runiversePackageArtifactLimit,
			runiversePackageManualTopicLimit:    *runiversePackageManualTopicLimit,
			websiteLimit:                        *websiteLimit,
			websiteCandidateLimit:               *websiteCandidateLimit,
			websiteFeedLimit:                    *websiteFeedLimit,
			websiteLinkLimit:                    *websiteLinkLimit,
			websiteSitemapLimit:                 *websiteSitemapLimit,
			githubLimit:                         *githubLimit,
			osvLimit:                            *osvLimit,
			bibliometricLimit:                   *bibliometricLimit,
		})
		if err != nil {
			return fmt.Errorf("%s: %w", currentJob, err)
		}
		if err := pub.publishGeneric(ctx, events); err != nil {
			if shouldDeferPackagePublishFailure(err) {
				fmt.Printf("[package] publish_deferred job=%s events=%d reason=%s\n", currentJob, len(events), packagePublishFailureReason(err))
				deferred += len(events)
				continue
			}
			return fmt.Errorf("%s publish: %w", currentJob, err)
		}
		fmt.Printf("job=%s published=%d\n", currentJob, len(events))
		total += len(events)
	}
	fmt.Printf("published=%d\n", total)
	if deferred > 0 {
		fmt.Printf("publish_deferred=%d\n", deferred)
	}
	return nil
}

func runCommunity(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("community", flag.ExitOnError)
	jsonlPath := fs.String("jsonl", envString("R_COMMUNITY_JSONL", "data/collected/r/latest.jsonl"), "normalized R Community JSONL path")
	topic := fs.String("topic", envString("R_COMMUNITY_KAFKA_TOPIC", defaultCommunityTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	limit := fs.Int("limit", envInt("R_COMMUNITY_EVENT_LIMIT", 0), "max JSONL rows to publish; 0 means all")
	translate := fs.Bool("translate", envBool("R_COMMUNITY_TRANSLATE_ENABLED", true), "translate non-Korean source rows into title_ko/summary_ko/content_ko before publish")
	translationLimit := fs.Int("translation-limit", envInt("R_COMMUNITY_TRANSLATION_LIMIT", 0), "max rows to translate in this run; 0 means all rows that need translation")
	translationModel := fs.String("translation-model", envString("R_COMMUNITY_TRANSLATION_MODEL", envString("R_COMMUNITY_DIGEST_MODEL", "google/gemini-2.5-flash-lite")), "AI model for R Community source item Korean translation")
	failOnTranslationErr := fs.Bool("fail-on-translation-error", envBool("R_COMMUNITY_FAIL_ON_TRANSLATION_ERROR", false), "fail community publish when a source row translation fails")
	reportPath := fs.String("report", envString("R_COMMUNITY_REPORT", defaultCommunityReportPath(*jsonlPath)), "R Community collection report JSON path")
	maxSourceAgeDays := fs.Float64(
		"max-source-age-days",
		envFloat("R_COMMUNITY_MAX_SOURCE_AGE_DAYS", 8),
		"max age in days for existing current rows that satisfy a temporarily unavailable required source",
	)
	skipExistingCanonical := fs.Bool(
		"skip-existing-canonical",
		envBool("R_COMMUNITY_SKIP_EXISTING_CANONICAL", true),
		"skip source rows whose external_id or canonical URL already exists before AI translation and Kafka publish",
	)
	fs.Parse(args)
	requiredSources := mergeStringSlices(defaultCommunityRequiredSourceIDs, splitCSV(envString("R_COMMUNITY_REQUIRED_SOURCE_IDS", "")))
	prioritySources := mergeStringSlices(defaultCommunityPrioritySourceIDs, splitCSV(envString("R_COMMUNITY_PRIORITY_SOURCE_IDS", "")))
	selectionSources := mergeStringSlices(requiredSources, prioritySources)

	var ai *aiClient
	translated := 0
	translationErrors := make([]string, 0)
	var enrich func(map[string]any) error
	if *translate {
		ai = newAIClient(time.Duration(maxInt(30, envInt("AI_TIMEOUT", 300))) * time.Second)
		if !ai.enabled() {
			return errors.New("R Community source translation is enabled, but no AI provider key is configured")
		}
		enrich = func(row map[string]any) error {
			if *translationLimit > 0 && translated >= *translationLimit {
				return nil
			}
			changed, err := translateCommunitySourceRow(ai, *translationModel, row)
			if err != nil {
				message := fmt.Sprintf("%s: %v", firstNonEmpty(stringAny(row["canonical_url"]), stringAny(row["external_id"])), err)
				translationErrors = append(translationErrors, message)
				if *failOnTranslationErr {
					return errors.New(message)
				}
				return nil
			}
			if changed {
				translated++
			}
			return nil
		}
	}

	events, selectedCount, skippedExisting, err := readCommunityJSONLEvents(
		ctx,
		*jsonlPath,
		*limit,
		requiredSources,
		selectionSources,
		*reportPath,
		*maxSourceAgeDays,
		enrich,
		*skipExistingCanonical,
	)
	if err != nil {
		return err
	}
	sourceCounts := communityEventSourceCounts(events)
	if len(events) == 0 {
		fmt.Printf(
			"published=0 translated=%d skipped_existing=%d selected=%d topic=%s jsonl=%s required_sources=%s priority_sources=%s required_counts=%s limit=%d\n",
			translated,
			skippedExisting,
			selectedCount,
			*topic,
			*jsonlPath,
			strings.Join(requiredSources, ","),
			strings.Join(prioritySources, ","),
			communityRequiredCountsString(sourceCounts, requiredSources),
			*limit,
		)
		return nil
	}
	pub := newPublisher(*topic, "statground-rcommunity-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	if err := pub.publishGeneric(ctx, events); err != nil {
		return err
	}
	if len(translationErrors) > 0 {
		fmt.Printf("translation_errors=%d first_error=%s\n", len(translationErrors), translationErrors[0])
	}
	fmt.Printf(
		"published=%d translated=%d skipped_existing=%d selected=%d topic=%s jsonl=%s required_sources=%s priority_sources=%s required_counts=%s limit=%d\n",
		len(events),
		translated,
		skippedExisting,
		selectedCount,
		*topic,
		*jsonlPath,
		strings.Join(requiredSources, ","),
		strings.Join(prioritySources, ","),
		communityRequiredCountsString(sourceCounts, requiredSources),
		*limit,
	)
	return nil
}

type communityJSONLRow struct {
	lineNo int
	row    map[string]any
}

type communityCollectionReport struct {
	StartedAt                  string                     `json:"started_at"`
	SinceDays                  any                        `json:"since_days"`
	SourceObservedLatestItemAt map[string]string          `json:"source_observed_latest_item_at"`
	Errors                     []communityCollectionError `json:"errors"`
}

type communityCollectionError struct {
	SourceID  string `json:"source_id"`
	ErrorType string `json:"error_type"`
	Message   string `json:"message"`
}

func readCommunityJSONLEvents(
	ctx context.Context,
	path string,
	limit int,
	requiredSourceIDs []string,
	selectionSourceIDs []string,
	reportPath string,
	maxSourceAgeDays float64,
	enrich func(map[string]any) error,
	skipExistingCanonical bool,
) ([]genericEvent, int, int, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, 0, err
	}
	rows := make([]communityJSONLRow, 0)
	for lineNo, rawLine := range bytes.Split(body, []byte("\n")) {
		line := bytes.TrimSpace(rawLine)
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, 0, 0, fmt.Errorf("%s:%d: %w", path, lineNo+1, err)
		}
		rows = append(rows, communityJSONLRow{lineNo: lineNo + 1, row: row})
	}
	rows = selectCommunityJSONLRows(rows, limit, selectionSourceIDs)
	report, err := loadCommunityCollectionReport(reportPath)
	if err != nil {
		return nil, len(rows), 0, err
	}
	if err := validateRequiredCommunityRows(ctx, rows, requiredSourceIDs, report, maxSourceAgeDays); err != nil {
		return nil, len(rows), 0, err
	}
	selectedCount := len(rows)
	skippedExisting := 0
	if skipExistingCanonical && len(rows) > 0 {
		cfg, err := newClickHouseQueryConfig()
		if err != nil {
			return nil, selectedCount, 0, err
		}
		var filterErr error
		rows, skippedExisting, filterErr = filterExistingCommunityJSONLRows(ctx, cfg, rows)
		if filterErr != nil {
			return nil, selectedCount, skippedExisting, filterErr
		}
	}
	events := make([]genericEvent, 0, len(rows))
	for _, item := range rows {
		if enrich != nil {
			if err := enrich(item.row); err != nil {
				return nil, selectedCount, skippedExisting, fmt.Errorf("%s:%d: %w", path, item.lineNo, err)
			}
		}
		events = append(events, communityRowEvent(item.row))
	}
	return events, selectedCount, skippedExisting, nil
}

func selectCommunityJSONLRows(rows []communityJSONLRow, limit int, requiredSourceIDs []string) []communityJSONLRow {
	if limit <= 0 || len(rows) <= limit {
		return rows
	}
	required := make(map[string]bool, len(requiredSourceIDs))
	for _, sourceID := range requiredSourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID != "" {
			required[sourceID] = true
		}
	}
	if len(required) == 0 {
		return rows[:limit]
	}
	selected := make([]bool, len(rows))
	selectedCount := 0
	for idx, item := range rows {
		if required[strings.TrimSpace(stringAny(item.row["source_id"]))] {
			selected[idx] = true
			selectedCount++
		}
	}
	for idx := range rows {
		if selected[idx] {
			continue
		}
		if selectedCount >= limit {
			break
		}
		selected[idx] = true
		selectedCount++
	}
	out := make([]communityJSONLRow, 0, selectedCount)
	for idx, item := range rows {
		if selected[idx] {
			out = append(out, item)
		}
	}
	return out
}

func communityRowEvent(row map[string]any) genericEvent {
	payload := make(map[string]any, len(row)+6)
	for key, value := range row {
		payload[key] = value
	}
	payload["payload_schema"] = "r_community_item_jsonl_v1"
	payload["source_method"] = "r_community_sources_jsonl"
	payload["collection_status"] = "collected"
	payload["repository"] = "R-Community"
	payload["row_external_id"] = stringAny(row["external_id"])

	sourceURL := firstNonEmpty(stringAny(row["canonical_url"]), stringAny(row["source_url"]))
	source := firstNonEmpty(stringAny(row["source_id"]), stringAny(row["source_name"]), "r_community_sources_jsonl")
	observedAt := firstNonEmpty(stringAny(row["published_at"]), stringAny(row["collected_at"]))
	return newGenericEvent("r.community.item.v1", source, sourceURL, "R-Community", "", "", observedAt, payload)
}

func validateRequiredCommunityEvents(events []genericEvent, requiredSourceIDs []string) error {
	counts := communityEventSourceCounts(events)
	missing := make([]string, 0)
	for _, sourceID := range requiredSourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID != "" && counts[sourceID] == 0 {
			missing = append(missing, sourceID)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("R Community required source rows were not selected for publish: %s", strings.Join(missing, ","))
	}
	return nil
}

func validateRequiredCommunityRows(
	ctx context.Context,
	rows []communityJSONLRow,
	requiredSourceIDs []string,
	report *communityCollectionReport,
	maxSourceAgeDays float64,
) error {
	counts := make(map[string]int)
	for _, item := range rows {
		sourceID := strings.TrimSpace(stringAny(item.row["source_id"]))
		if sourceID != "" {
			counts[sourceID]++
		}
	}
	missing := make([]string, 0)
	for _, sourceID := range requiredSourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID == "" || counts[sourceID] > 0 {
			continue
		}
		if communityReportSourceInactiveForWindow(report, sourceID) {
			continue
		}
		if !communityReportSourceUnavailable(report, sourceID) {
			missing = append(missing, sourceID)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("R Community required source rows were not selected for publish: %s", strings.Join(missing, ","))
	}
	blockedMissing := make([]string, 0)
	for _, sourceID := range requiredSourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID != "" &&
			counts[sourceID] == 0 &&
			communityReportSourceUnavailable(report, sourceID) &&
			!communityReportSourceInactiveForWindow(report, sourceID) {
			blockedMissing = append(blockedMissing, sourceID)
		}
	}
	if len(blockedMissing) == 0 {
		return nil
	}
	satisfied, err := communitySourcesFreshInCurrent(ctx, blockedMissing, maxSourceAgeDays)
	if err != nil {
		return fmt.Errorf("R Community required source rows were blocked and live current freshness could not be verified: %w", err)
	}
	for _, sourceID := range blockedMissing {
		if !satisfied[sourceID] {
			missing = append(missing, sourceID)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("R Community required source rows were not selected for publish and live current was not fresh: %s", strings.Join(missing, ","))
	}
	return nil
}

func loadCommunityCollectionReport(path string) (*communityCollectionReport, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var report communityCollectionReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &report, nil
}

func defaultCommunityReportPath(jsonlPath string) string {
	jsonlPath = strings.TrimSpace(jsonlPath)
	if jsonlPath == "" {
		return ""
	}
	dir := filepath.Dir(jsonlPath)
	base := filepath.Base(jsonlPath)
	ext := filepath.Ext(base)
	if ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	return filepath.Join(dir, base+"_report.json")
}

func communityReportSourceInactiveForWindow(report *communityCollectionReport, sourceID string) bool {
	if report == nil {
		return false
	}
	observedRaw := strings.TrimSpace(report.SourceObservedLatestItemAt[sourceID])
	if observedRaw == "" {
		return false
	}
	observed := parseKSTTime(observedRaw, time.Time{})
	if observed.IsZero() {
		return false
	}
	sinceDays, ok := floatAny(report.SinceDays)
	if !ok || sinceDays < 0 {
		return false
	}
	started := parseKSTTime(report.StartedAt, time.Now())
	cutoff := started.UTC().Add(-time.Duration(sinceDays * float64(24*time.Hour)))
	return observed.UTC().Before(cutoff)
}

func communityReportSourceUnavailable(report *communityCollectionReport, sourceID string) bool {
	if report == nil {
		return false
	}
	for _, item := range report.Errors {
		if strings.TrimSpace(item.SourceID) != sourceID {
			continue
		}
		if communitySourceUnavailableError(item.ErrorType + "\n" + item.Message) {
			return true
		}
	}
	return false
}

func communitySourceUnavailableError(message string) bool {
	lowered := strings.ToLower(message)
	for _, token := range []string{"403", "blocked", "forbidden", "429", "too many", "503", "temporarily unavailable"} {
		if strings.Contains(lowered, token) {
			return true
		}
	}
	return false
}

func communitySourcesFreshInCurrent(ctx context.Context, sourceIDs []string, maxSourceAgeDays float64) (map[string]bool, error) {
	if maxSourceAgeDays <= 0 {
		maxSourceAgeDays = 8
	}
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
SELECT
    source_id,
    count() AS rows,
    ifNull(formatDateTime(max(coalesce(original_published_at, collected_at)), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul'), '') AS latest_source_at
FROM Data_R_Community_Service.r_community_item_read_current
WHERE source_id IN (%s)
  AND active = 1
  AND notEmpty(title)
  AND notEmpty(canonical_url)
GROUP BY source_id
FORMAT JSONEachRow`, clickHouseStringList(sourceIDs))
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	rows, err := cfg.queryJSONEachRow(query)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make(map[string]bool, len(sourceIDs))
	for _, row := range rows {
		sourceID := strings.TrimSpace(stringAny(row["source_id"]))
		if sourceID == "" || intAny(row["rows"]) <= 0 {
			continue
		}
		latest := parseKSTTime(stringAny(row["latest_source_at"]), time.Time{})
		if latest.IsZero() {
			continue
		}
		ageDays := now.Sub(latest).Hours() / 24
		out[sourceID] = ageDays <= maxSourceAgeDays
	}
	return out, nil
}

func filterExistingCommunityJSONLRows(
	ctx context.Context,
	cfg clickHouseQueryConfig,
	rows []communityJSONLRow,
) ([]communityJSONLRow, int, error) {
	existingExternal, existingCanonical, err := loadExistingCommunityRowKeys(ctx, cfg, rows)
	if err != nil {
		return nil, 0, err
	}
	out := make([]communityJSONLRow, 0, len(rows))
	skipped := 0
	for _, item := range rows {
		externalID := strings.TrimSpace(stringAny(item.row["external_id"]))
		canonicalKey := communityCanonicalDedupKey(stringAny(item.row["canonical_url"]))
		if (externalID != "" && existingExternal[externalID]) || (canonicalKey != "" && existingCanonical[canonicalKey]) {
			skipped++
			continue
		}
		out = append(out, item)
	}
	return out, skipped, nil
}

func loadExistingCommunityRowKeys(
	ctx context.Context,
	cfg clickHouseQueryConfig,
	rows []communityJSONLRow,
) (map[string]bool, map[string]bool, error) {
	externalIDs := make([]string, 0, len(rows))
	canonicalKeys := make([]string, 0, len(rows))
	seenExternal := make(map[string]bool)
	seenCanonical := make(map[string]bool)
	for _, item := range rows {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		externalID := strings.TrimSpace(stringAny(item.row["external_id"]))
		if externalID != "" && !seenExternal[externalID] {
			seenExternal[externalID] = true
			externalIDs = append(externalIDs, externalID)
		}
		canonicalKey := communityCanonicalDedupKey(stringAny(item.row["canonical_url"]))
		if canonicalKey != "" && !seenCanonical[canonicalKey] {
			seenCanonical[canonicalKey] = true
			canonicalKeys = append(canonicalKeys, canonicalKey)
		}
	}
	existingExternal := make(map[string]bool)
	existingCanonical := make(map[string]bool)
	const chunkSize = 200
	maxChunks := maxInt((len(externalIDs)+chunkSize-1)/chunkSize, (len(canonicalKeys)+chunkSize-1)/chunkSize)
	for i := 0; i < maxChunks; i++ {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		externalChunk := stringChunk(externalIDs, i*chunkSize, chunkSize)
		canonicalChunk := stringChunk(canonicalKeys, i*chunkSize, chunkSize)
		whereParts := make([]string, 0, 2)
		if len(externalChunk) > 0 {
			whereParts = append(whereParts, "external_id IN ("+clickHouseStringList(externalChunk)+")")
		}
		if len(canonicalChunk) > 0 {
			whereParts = append(whereParts, "canonical_key IN ("+clickHouseStringList(canonicalChunk)+")")
		}
		if len(whereParts) == 0 {
			continue
		}
		query := fmt.Sprintf(`
SELECT external_id, canonical_key
  FROM
  (
      SELECT external_id,
             lowerUTF8(replaceRegexpAll(canonical_url, '/+(#|$)', '\\1')) AS canonical_key
        FROM Data_R_Community_Raw.r_community_item_raw
       WHERE active = 1
         AND notEmpty(canonical_url)
  )
 WHERE %s
 FORMAT JSONEachRow`, strings.Join(whereParts, " OR "))
		resultRows, err := cfg.queryJSONEachRow(query)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range resultRows {
			if externalID := strings.TrimSpace(stringAny(row["external_id"])); externalID != "" {
				existingExternal[externalID] = true
			}
			if canonicalKey := strings.TrimSpace(stringAny(row["canonical_key"])); canonicalKey != "" {
				existingCanonical[canonicalKey] = true
			}
		}
	}
	return existingExternal, existingCanonical, nil
}

func clickHouseStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			quoted = append(quoted, clickHouseQuoteString(value))
		}
	}
	return strings.Join(quoted, ",")
}

func stringChunk(values []string, start, size int) []string {
	if start >= len(values) || size <= 0 {
		return nil
	}
	end := start + size
	if end > len(values) {
		end = len(values)
	}
	return values[start:end]
}

func communityCanonicalDedupKey(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err == nil && parsed.Scheme != "" && parsed.Host != "" {
		query := parsed.Query()
		for key := range query {
			keyLower := strings.ToLower(key)
			if keyLower == "fbclid" ||
				keyLower == "gclid" ||
				keyLower == "igshid" ||
				keyLower == "mc_cid" ||
				keyLower == "mc_eid" ||
				keyLower == "spm" ||
				keyLower == "ref_src" ||
				keyLower == "ref" ||
				keyLower == "source" ||
				strings.HasPrefix(keyLower, "utm_") {
				query.Del(key)
			}
		}
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		parsed.RawQuery = query.Encode()
		if parsed.Path != "/" {
			parsed.Path = strings.TrimRight(parsed.Path, "/")
		}
		rawURL = parsed.String()
	}
	if hashIndex := strings.Index(rawURL, "#"); hashIndex >= 0 {
		prefix := strings.TrimRight(rawURL[:hashIndex], "/")
		return strings.ToLower(prefix + rawURL[hashIndex:])
	}
	return strings.ToLower(strings.TrimRight(rawURL, "/"))
}

func communityEventSourceCounts(events []genericEvent) map[string]int {
	counts := make(map[string]int)
	for _, event := range events {
		sourceID := strings.TrimSpace(event.Source)
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Payload), &payload); err == nil {
			sourceID = firstNonEmpty(stringAny(payload["source_id"]), sourceID)
		}
		if sourceID != "" {
			counts[sourceID]++
		}
	}
	return counts
}

func communityRequiredCountsString(counts map[string]int, requiredSourceIDs []string) string {
	parts := make([]string, 0, len(requiredSourceIDs))
	for _, sourceID := range requiredSourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID != "" {
			parts = append(parts, fmt.Sprintf("%s=%d", sourceID, counts[sourceID]))
		}
	}
	return strings.Join(parts, ",")
}

func translateCommunitySourceRow(ai *aiClient, model string, row map[string]any) (bool, error) {
	if ai == nil || !ai.enabled() {
		return false, errors.New("AI client is not enabled")
	}
	title := stringAny(row["title"])
	summary := stringAny(row["summary"])
	content := stringAny(row["content"])
	evidence := communitySourceTranslationEvidence(row)
	changed := false
	titleKo := strings.TrimSpace(stringAny(row["title_ko"]))
	summaryKo := strings.TrimSpace(stringAny(row["summary_ko"]))
	contentKo := strings.TrimSpace(stringAny(row["content_ko"]))
	needsTitle := titleKo == "" && strings.TrimSpace(title) != "" && !looksKorean(title, 0.20)
	needsSummary := (summaryKo == "" || isWeakCommunitySourceKoreanText(row, summaryKo)) && strings.TrimSpace(summary) != "" && !looksKorean(summary, 0.25)
	if strings.TrimSpace(stringAny(row["title_ko"])) == "" && strings.TrimSpace(title) != "" && looksKorean(title, 0.20) {
		row["title_ko"] = title
		changed = true
	}
	if summaryKo == "" && strings.TrimSpace(summary) != "" && looksKorean(summary, 0.25) {
		row["summary_ko"] = summary
		changed = true
	}

	if strings.TrimSpace(content) == "" && (needsTitle || needsSummary) {
		response, err := ai.chat(communitySourceTitleSummaryPrompt(title, summary, evidence), model)
		if err != nil {
			return changed, err
		}
		translatedTitle, translatedSummary := parseCommunitySourceTranslationResponse(response)
		if needsTitle {
			translatedTitle = preserveCommunityAnniversaryNumbers(evidence, translatedTitle)
			translatedTitle = cleanBoardTitle(translatedTitle)
			if translatedTitle != "" {
				row["title_ko"] = translatedTitle
				changed = true
				needsTitle = false
			}
		}
		if needsSummary {
			translatedSummary = preserveCommunityAnniversaryNumbers(evidence, translatedSummary)
			translatedSummary = strings.TrimSpace(removeBoardURLs(stripTags(translatedSummary)))
			if translatedSummary != "" {
				row["summary_ko"] = translatedSummary
				changed = true
				needsSummary = false
			}
		}
	}

	if needsTitle {
		translatedTitle, err := ai.chat(communitySourceTitlePrompt(title, summary, evidence), model)
		if err != nil {
			return changed, err
		}
		translatedTitle = preserveCommunityAnniversaryNumbers(evidence, translatedTitle)
		translatedTitle = cleanBoardTitle(translatedTitle)
		if translatedTitle != "" {
			row["title_ko"] = translatedTitle
			changed = true
		}
	}
	if needsSummary {
		translatedSummary, err := ai.chat(communitySourceSummaryPrompt(title, summary, evidence), model)
		if err != nil {
			return changed, err
		}
		translatedSummary = preserveCommunityAnniversaryNumbers(evidence, translatedSummary)
		translatedSummary = strings.TrimSpace(removeBoardURLs(stripTags(translatedSummary)))
		if translatedSummary != "" {
			row["summary_ko"] = translatedSummary
			changed = true
		}
	}
	if (contentKo == "" || isWeakCommunitySourceKoreanText(row, contentKo)) && strings.TrimSpace(content) != "" && !looksKorean(content, 0.25) {
		translatedContent, err := ai.chat(communitySourceContentPrompt(title, content, evidence), model)
		if err != nil {
			return changed, err
		}
		translatedContent = preserveCommunityAnniversaryNumbers(evidence, translatedContent)
		sanitized, err := sanitizeBoardHTML(translatedContent)
		if err != nil {
			return changed, err
		}
		if strings.TrimSpace(stripTags(sanitized)) != "" {
			row["content_ko"] = sanitized
			changed = true
		}
	}
	if (strings.TrimSpace(stringAny(row["content_ko"])) == "" ||
		isWeakCommunitySourceKoreanText(row, stringAny(row["content_ko"]))) &&
		strings.TrimSpace(stringAny(row["summary_ko"])) != "" {
		row["content_ko"] = safeHTML(stringAny(row["summary_ko"]))
		changed = true
	}
	if changed {
		row["translation_status"] = "translated"
		row["translation_language"] = "ko"
		row["translation_model"] = model
		row["translation_updated_at"] = nowKST().Format(time.RFC3339)
	}
	return changed, nil
}

func communitySourceTranslationEvidence(row map[string]any) string {
	parts := make([]string, 0, 10)
	seen := map[string]bool{}
	add := func(label, value string) {
		value = strings.TrimSpace(removeBoardURLs(stripTags(value)))
		if value == "" {
			return
		}
		value = spaceRE.ReplaceAllString(value, " ")
		if seen[value] {
			return
		}
		seen[value] = true
		parts = append(parts, label+": "+truncateRunes(value, 1200))
	}
	add("title", stringField(row, "title"))
	add("summary", stringField(row, "summary"))
	add("content", stringField(row, "content"))
	addCommunitySourceEvidenceFromMap(add, "row", row, 0)
	if rawJSON := strings.TrimSpace(stringAny(row["raw_json"])); rawJSON != "" {
		var raw map[string]any
		if err := json.Unmarshal([]byte(rawJSON), &raw); err == nil {
			addCommunitySourceEvidenceFromMap(add, "raw_json", raw, 0)
		}
	}
	return truncateRunes(strings.Join(parts, "\n"), 5000)
}

func addCommunitySourceEvidenceFromMap(add func(string, string), label string, row map[string]any, depth int) {
	if len(row) == 0 || depth > 4 {
		return
	}
	for _, key := range []string{"title", "description", "image_description", "content_text", "text_excerpt", "summary", "content", "link_text", "link_context", "target_title", "target_html_title", "target_meta_description", "target_abstract"} {
		add(label+"."+key, stringField(row, key))
	}
	for _, key := range []string{"card", "status", "original_status", "reblog", "quote", "summary_detail"} {
		if nested := mapAny(row[key]); len(nested) > 0 {
			addCommunitySourceEvidenceFromMap(add, label+"."+key, nested, depth+1)
		}
	}
}

func stringField(row map[string]any, key string) string {
	value, ok := row[key]
	if !ok {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func isWeakCommunitySourceKoreanText(row map[string]any, value string) bool {
	text := strings.TrimSpace(value)
	if text == "" {
		return false
	}
	lower := strings.ToLower(text)
	for _, marker := range []string{
		"discovered from",
		"원문 링크에서 확인",
		"원문에는 패키지 배포 배경",
		"빌드 결과와 패키지 설명은 원문",
	} {
		if strings.Contains(lower, strings.ToLower(marker)) {
			return true
		}
	}
	sourceID := strings.ToLower(strings.TrimSpace(stringAny(row["source_id"])))
	if sourceID == "official:r-mail:r-packages" {
		return strings.Contains(text, "R-packages 메일링 리스트에 올라온") &&
			(strings.Contains(text, "관련 공지입니다") || strings.Contains(text, "신규 CRAN 패키지 공지입니다"))
	}
	return false
}

func preserveCommunityAnniversaryNumbers(evidence, translated string) string {
	numbers := communityAnniversaryNumbers(evidence)
	if len(numbers) == 0 || strings.TrimSpace(translated) == "" {
		return translated
	}
	allowed := make(map[string]bool, len(numbers))
	for _, number := range numbers {
		allowed[number] = true
	}
	preferred := numbers[0]
	return koAnniversaryRE.ReplaceAllStringFunc(translated, func(match string) string {
		parts := koAnniversaryRE.FindStringSubmatch(match)
		if len(parts) < 2 || allowed[parts[1]] {
			return match
		}
		return strings.Replace(match, parts[1], preferred, 1)
	})
}

func communityAnniversaryNumbers(value string) []string {
	out := make([]string, 0, 2)
	seen := map[string]bool{}
	add := func(number string) {
		number = strings.TrimSpace(number)
		if number == "" || seen[number] {
			return
		}
		seen[number] = true
		out = append(out, number)
	}
	for _, match := range enAnniversaryRE.FindAllStringSubmatch(value, -1) {
		if len(match) > 1 {
			add(match[1])
		}
	}
	for _, match := range koAnniversaryRE.FindAllStringSubmatch(value, -1) {
		if len(match) > 1 {
			add(match[1])
		}
	}
	return out
}

func communitySourceTitleSummaryPrompt(title, summary, evidence string) string {
	return fmt.Sprintf(`You are a professional Korean translator for the Web-R R ecosystem reader.

Translate the source title and abstract/summary into Korean.

Output rules:
- Return only JSON: {"title_ko":"...","summary_ko":"..."}
- Do not include Markdown fences, explanations, labels, HTML, URLs, or hyperlinks.
- title_ko must be one concise Korean title line.
- summary_ko must be one compact Korean paragraph in polite formal Korean ending in ~합니다 or ~입니다.
- Preserve R, CRAN, package names, function names, code, formulas, numbers, version strings, statistical terms, and proper nouns.
- Preserve factual numeric claims exactly. If the source evidence says "15 Years" or "15th anniversary", Korean must say "15주년"; never replace it with another number.
- Use only the source title, summary, and evidence below. Do not infer anniversaries, ages, counts, release numbers, dates, or durations that are not supported by the evidence.

Source title:
%s

Source abstract/summary:
%s

Source evidence/context:
%s`, title, truncateRunes(summary, 3000), evidence)
}

func parseCommunitySourceTranslationResponse(value string) (string, string) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```json")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	value = strings.TrimSpace(value)
	type response struct {
		TitleKo   string `json:"title_ko"`
		SummaryKo string `json:"summary_ko"`
		Title     string `json:"title"`
		Summary   string `json:"summary"`
	}
	var parsed response
	if err := json.Unmarshal([]byte(value), &parsed); err == nil {
		return firstNonEmpty(parsed.TitleKo, parsed.Title), firstNonEmpty(parsed.SummaryKo, parsed.Summary)
	}
	start := strings.Index(value, "{")
	end := strings.LastIndex(value, "}")
	if start >= 0 && end > start {
		if err := json.Unmarshal([]byte(value[start:end+1]), &parsed); err == nil {
			return firstNonEmpty(parsed.TitleKo, parsed.Title), firstNonEmpty(parsed.SummaryKo, parsed.Summary)
		}
	}
	return "", value
}

func communitySourceTitlePrompt(title, summary, evidence string) string {
	return fmt.Sprintf(`You are a professional Korean translator for the Web-R R ecosystem reader.

Output rules:
- Return exactly one Korean title line.
- Do not include explanations, labels, quotes, Markdown, HTML, URLs, or hyperlinks.
- Preserve R, CRAN, package names, function names, version numbers, statistical terms, and proper nouns.
- Preserve factual numeric claims exactly. If the source evidence says "15 Years" or "15th anniversary", Korean must say "15주년"; never replace it with another number.
- Use only the source title, summary, and evidence below. Do not infer anniversaries, ages, counts, release numbers, dates, or durations that are not supported by the evidence.
- Keep it concise and natural for Korean readers.

Source title:
%s

Source summary:
%s

Source evidence/context:
%s`, title, truncateRunes(summary, 1200), evidence)
}

func communitySourceSummaryPrompt(title, summary, evidence string) string {
	return fmt.Sprintf(`You are an editorial Korean translator for the Web-R R ecosystem reader.

Translate the source summary into Korean.

Output rules:
- Return one compact Korean paragraph as plain text.
- Do not include explanations, labels, Markdown, HTML, URLs, or hyperlinks.
- Use polite formal Korean ending in ~합니다 or ~입니다.
- Preserve R, CRAN, package names, function names, code, numbers, version strings, and proper nouns.
- Preserve factual numeric claims exactly. If the source evidence says "15 Years" or "15th anniversary", Korean must say "15주년"; never replace it with another number.
- Use only the source title, summary, and evidence below. Do not infer anniversaries, ages, counts, release numbers, dates, or durations that are not supported by the evidence.

Source title:
%s

Source summary:
%s

Source evidence/context:
%s`, title, truncateRunes(summary, 3000), evidence)
}

func communitySourceContentPrompt(title, content, evidence string) string {
	return fmt.Sprintf(`You are an editorial Korean translator for the Web-R R ecosystem reader.

Translate and lightly edit the source body for Korean Web-R readers.

Output rules:
- Return only a compact HTML fragment. The first character must be "<".
- Never use <html>, <head>, or <body>.
- Allowed tags only: <h2>, <h3>, <p>, <ul>, <ol>, <li>, <strong>, <em>, <code>, <pre>, <blockquote>.
- Do not output hyperlinks, URLs, Markdown links, HTML <a> tags, href attributes, citations, source links, or "read more" links.
- Use polite formal Korean ending in ~합니다 or ~입니다.
- Preserve R, CRAN, package names, function names, code, formulas, numbers, version strings, and proper nouns.
- Preserve factual numeric claims exactly. If the source evidence says "15 Years" or "15th anniversary", Korean must say "15주년"; never replace it with another number.
- Use only the source title, body, and evidence below. Do not infer anniversaries, ages, counts, release numbers, dates, or durations that are not supported by the evidence.
- Do not add an introduction, explanation, label, or meta-commentary.

Source title:
%s

Source body:
%s

Source evidence/context:
%s`, title, truncateRunes(stripTags(content), 9000), evidence)
}

func runCommunityDigest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("community-digest", flag.ExitOnError)
	topic := fs.String("topic", envString("R_COMMUNITY_KAFKA_TOPIC", defaultCommunityTopic), "Kafka topic for digest events")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print digest events instead of Kafka")
	insertClickHouse := fs.Bool("insert-clickhouse", envBool("R_COMMUNITY_DIGEST_INSERT_CLICKHOUSE", false), "insert digest rows directly into ClickHouse service table")
	sinceDays := fs.Int("since-days", envInt("R_COMMUNITY_DIGEST_SINCE_DAYS", 14), "digest source rows newer than N days; -1 means all")
	groupLimit := fs.Int("group-limit", envInt("R_COMMUNITY_DIGEST_GROUP_LIMIT", 0), "max digest groups to generate; 0 means all")
	itemLimit := fs.Int("items-per-digest", envInt("R_COMMUNITY_DIGEST_ITEMS_PER_DIGEST", 80), "max source links kept in each digest")
	model := fs.String("model", envString("R_COMMUNITY_DIGEST_MODEL", envString("MASTODON_TRANSLATION_MODEL", envString("RBLOGGER_TRANSLATION_MODEL", "google/gemini-2.5-flash-lite"))), "AI model for Korean daily digest")
	allowFallback := fs.Bool("allow-fallback", envBool("R_COMMUNITY_DIGEST_ALLOW_FALLBACK", false), "allow deterministic non-AI digest when no AI provider is configured")
	missingOnly := fs.Bool("missing-only", envBool("R_COMMUNITY_DIGEST_MISSING_ONLY", false), "only publish or insert digest groups not already present in the latest digest view")
	latestPerSource := fs.Bool("latest-per-source", envBool("R_COMMUNITY_DIGEST_LATEST_PER_SOURCE", true), "publish at most the newest missing digest group per source in one run")
	planOutput := fs.String("plan-output", envString("R_COMMUNITY_DIGEST_PLAN_OUTPUT", ""), "write planned digest ids to this JSON file for visibility wait")
	publishKafka := fs.Bool("publish-kafka", envBool("R_COMMUNITY_DIGEST_PUBLISH_KAFKA", false), "publish digest events to Kafka after optional direct ClickHouse insert")
	fs.Parse(args)

	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return err
	}
	records, err := buildCommunityDigestRecords(ctx, cfg, *sinceDays, *groupLimit, maxInt(1, *itemLimit), *model, *allowFallback, *missingOnly, *latestPerSource)
	if err != nil {
		return err
	}
	if err := writeCommunityDigestPlan(*planOutput, records); err != nil {
		return err
	}
	if *insertClickHouse {
		if err := insertCommunityDigestRecords(ctx, cfg, records); err != nil {
			return err
		}
		fmt.Printf("inserted=%d table=Data_R_Community_Service.r_community_daily_digest\n", len(records))
	}
	if !*publishKafka {
		fmt.Printf("published=0 topic=%s skipped=true\n", *topic)
		return nil
	}
	events := communityDigestEvents(records)
	pub := newPublisher(*topic, "statground-rcommunity-digest", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	if err := pub.publishGeneric(ctx, events); err != nil {
		return err
	}
	fmt.Printf("published=%d topic=%s\n", len(events), *topic)
	return nil
}

func buildCommunityDigestRecords(ctx context.Context, cfg clickHouseQueryConfig, sinceDays, groupLimit, itemLimit int, model string, allowFallback bool, missingOnly bool, latestPerSource bool) ([]communityDigestRecord, error) {
	rows, err := fetchCommunityDigestSourceRows(cfg, sinceDays)
	if err != nil {
		return nil, err
	}
	records := groupCommunityDigestRows(rows, itemLimit)
	if missingOnly {
		before := len(records)
		records, err = filterMissingCommunityDigestRecords(cfg, records)
		if err != nil {
			return nil, err
		}
		fmt.Printf("missing_only_before=%d missing_only_after=%d\n", before, len(records))
	}
	if latestPerSource {
		before := len(records)
		records = filterLatestCommunityDigestRecordsPerSource(records)
		fmt.Printf("latest_per_source_before=%d latest_per_source_after=%d\n", before, len(records))
	}
	if groupLimit > 0 && len(records) > groupLimit {
		records = records[:groupLimit]
	}
	ai := newAIClient(time.Duration(maxInt(30, envInt("AI_TIMEOUT", 300))) * time.Second)
	if !ai.enabled() && !allowFallback {
		return nil, errors.New("community digest requires an AI provider key; set OPENROUTER_API_KEY, GROQ_API_KEY, CEREBRAS_API_KEY, or GH_MODELS_API_KEY")
	}
	for i := range records {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		prompt := communityDigestPrompt(records[i])
		records[i].PromptHash = shaHex(prompt)
		if ai.enabled() {
			response, err := ai.chat(prompt, model)
			if err != nil {
				if !allowFallback {
					return nil, fmt.Errorf("digest %s AI summary: %w", records[i].DigestID, err)
				}
				records[i].Title = cleanCommunityDigestTitle(records[i].Title, records[i])
				records[i].Summary = fallbackCommunityDigestSummary(records[i])
				records[i].Status = "fallback_ai_error"
			} else {
				title, summary := parseCommunityDigestAIResponse(response)
				records[i].Summary = cleanCommunityDigestSummary(summary)
				records[i].Title = cleanCommunityDigestTitle(communityDigestTitleFromSummary(records[i].Summary, firstNonEmpty(title, records[i].Title), records[i]), records[i])
				records[i].Status = "generated"
			}
		} else {
			records[i].Title = cleanCommunityDigestTitle(records[i].Title, records[i])
			records[i].Summary = fallbackCommunityDigestSummary(records[i])
			records[i].Status = "fallback_no_ai"
		}
		records[i].Model = model
		records[i].GeneratedAt = nowKST().Format(time.RFC3339)
		records[i].Title = cleanCommunityDigestTitle(communityDigestTitleFromSummary(records[i].Summary, records[i].Title, records[i]), records[i])
		if strings.TrimSpace(records[i].Summary) == "" {
			records[i].Summary = fallbackCommunityDigestSummary(records[i])
			records[i].Status = firstNonEmpty(records[i].Status, "fallback_empty")
		}
		records[i].PayloadHash = communityDigestPayloadHash(records[i])
	}
	return records, nil
}

func filterMissingCommunityDigestRecords(cfg clickHouseQueryConfig, records []communityDigestRecord) ([]communityDigestRecord, error) {
	if len(records) == 0 {
		return records, nil
	}
	existing := make(map[string]bool, len(records))
	const chunkSize = 200
	for start := 0; start < len(records); start += chunkSize {
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		quotedIDs := make([]string, 0, end-start)
		for _, record := range records[start:end] {
			if strings.TrimSpace(record.DigestID) != "" {
				quotedIDs = append(quotedIDs, clickHouseQuoteString(record.DigestID))
			}
		}
		if len(quotedIDs) == 0 {
			continue
		}
		query := fmt.Sprintf(`
SELECT digest_id
  FROM Data_R_Community_Service.v_r_community_daily_digest_latest
 WHERE digest_id IN (%s)
 FORMAT JSONEachRow`, strings.Join(quotedIDs, ","))
		rows, err := cfg.queryJSONEachRow(query)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if digestID := strings.TrimSpace(stringAny(row["digest_id"])); digestID != "" {
				existing[digestID] = true
			}
		}
	}
	missing := make([]communityDigestRecord, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.DigestID) == "" || existing[record.DigestID] {
			continue
		}
		missing = append(missing, record)
	}
	return missing, nil
}

func filterLatestCommunityDigestRecordsPerSource(records []communityDigestRecord) []communityDigestRecord {
	out := make([]communityDigestRecord, 0, len(records))
	seen := make(map[string]bool)
	for _, record := range records {
		key := strings.Join([]string{record.SourceType, record.SourceID, record.SourceName, record.Platform}, "\x00")
		if key == "\x00\x00\x00" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, record)
	}
	return out
}

func writeCommunityDigestPlan(path string, records []communityDigestRecord) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	plan := communityDigestPlan{
		GeneratedAt: nowKST().Format(time.RFC3339),
		RecordCount: len(records),
		DigestIDs:   make([]string, 0, len(records)),
		Records:     make([]communityDigestPlanRecord, 0, len(records)),
	}
	for _, record := range records {
		plan.DigestIDs = append(plan.DigestIDs, record.DigestID)
		plan.Records = append(plan.Records, communityDigestPlanRecord{
			DigestID:   record.DigestID,
			DigestUUID: record.DigestUUID,
			DigestDate: record.DigestDate,
			SourceType: record.SourceType,
			SourceID:   record.SourceID,
			SourceName: record.SourceName,
			Platform:   record.Platform,
			ItemCount:  record.ItemCount,
			Status:     record.Status,
		})
	}
	body, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		return err
	}
	fmt.Printf("digest_plan=%s planned=%d\n", path, len(records))
	return nil
}

func fetchCommunityDigestSourceRows(cfg clickHouseQueryConfig, sinceDays int) ([]map[string]any, error) {
	allowed := splitCSV(envString("R_COMMUNITY_DIGEST_SOURCE_TYPES", "community_forum,qna_feed,social_tag,fediverse_group"))
	quoted := make([]string, 0, len(allowed))
	for _, value := range allowed {
		value = strings.TrimSpace(value)
		if value != "" {
			quoted = append(quoted, clickHouseQuoteString(value))
		}
	}
	if len(quoted) == 0 {
		return nil, errors.New("R_COMMUNITY_DIGEST_SOURCE_TYPES resolved to an empty set")
	}
	excludedIDs := splitCSV(envString("R_COMMUNITY_DIGEST_EXCLUDED_SOURCE_IDS", "mastodon:group:rstats"))
	excludedWhere := ""
	if len(excludedIDs) > 0 {
		quotedExcluded := make([]string, 0, len(excludedIDs))
		for _, value := range excludedIDs {
			value = strings.TrimSpace(value)
			if value != "" {
				quotedExcluded = append(quotedExcluded, clickHouseQuoteString(value))
			}
		}
		if len(quotedExcluded) > 0 {
			excludedWhere = fmt.Sprintf(" AND source_id NOT IN (%s)", strings.Join(quotedExcluded, ","))
		}
	}
	sinceWhere := ""
	if sinceDays >= 0 {
		cutoffDate := nowKST().AddDate(0, 0, -sinceDays).Format("2006-01-02")
		sinceWhere = fmt.Sprintf(" AND toDate(coalesce(original_published_at, collected_at)) >= toDate(%s)", clickHouseQuoteString(cutoffDate))
	}
	pubMedRedditWhere := `
  AND (
      (
        positionCaseInsensitiveUTF8(source_name, 'PubMed') = 0
        AND source_id NOT IN (
          'reddit:r/librarians',
          'reddit:r/research',
          'reddit:r/bioinformatics',
          'reddit:r/labrats',
          'reddit:r/AskAcademia',
          'reddit:r/medicine',
          'reddit:r/pharmacy',
          'reddit:r/DataHoarder',
          'reddit:r/healthIT'
        )
      )
      OR positionUTF8(concat(title, '\n', summary, '\n', canonical_url), 'MeSH') > 0
      OR match(concat(title, '\n', summary, '\n', canonical_url), '(?i)PubMed|MEDLINE|PubMed Central|\\bPMC\\b|\\bNCBI\\b|\\bNLM\\b|E[- ]utilities|\\bEntrez\\b|literature search|literature review|systematic review|scoping review|search strateg|evidence synthesis|biomedical literature|medical librarian|clinical literature|database search')
  )`
	query := fmt.Sprintf(`
SELECT
    toString(toDate(coalesce(original_published_at, collected_at))) AS digest_date,
    external_id,
    source_id,
    source_name,
    source_type,
    platform,
    source_url,
    canonical_url,
    title,
    summary,
    raw_json,
    author,
    language,
    ifNull(formatDateTime(original_published_at, '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul'), '') AS published_at_text,
    formatDateTime(collected_at, '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul') AS collected_at_text
FROM Data_R_Community_Service.v_r_community_latest
WHERE source_type IN (%s)
  AND notEmpty(title)
  AND notEmpty(canonical_url)
  %s
  %s
  %s
ORDER BY digest_date DESC, source_type ASC, source_id ASC, published_at_text DESC, collected_at_text DESC, title ASC
FORMAT JSONEachRow`, strings.Join(quoted, ","), excludedWhere, sinceWhere, pubMedRedditWhere)
	return cfg.queryJSONEachRow(query)
}

func groupCommunityDigestRows(rows []map[string]any, itemLimit int) []communityDigestRecord {
	type groupState struct {
		record communityDigestRecord
		raw    int
	}
	groups := map[string]*groupState{}
	order := make([]string, 0)
	seenCanonical := map[string]bool{}
	for _, row := range rows {
		digestDate := firstNonEmpty(stringAny(row["digest_date"]), nowKST().Format("2006-01-02"))
		sourceType := stringAny(row["source_type"])
		sourceID := stringAny(row["source_id"])
		sourceName := firstNonEmpty(stringAny(row["source_name"]), sourceID)
		platform := stringAny(row["platform"])
		key := strings.Join([]string{digestDate, sourceType, sourceID, sourceName, platform}, "\x00")
		state := groups[key]
		if state == nil {
			digestID := "sha256:" + shaHex(strings.Join([]string{digestDate, sourceType, sourceID, sourceName, platform}, "\n"))
			state = &groupState{record: communityDigestRecord{
				DigestID:   digestID,
				DigestUUID: communityDigestUUID(digestID),
				DigestDate: digestDate,
				SourceType: sourceType,
				SourceID:   sourceID,
				SourceName: sourceName,
				Platform:   platform,
				SourceURL:  stringAny(row["source_url"]),
			}}
			groups[key] = state
			order = append(order, key)
		}
		state.raw++
		canonical := stringAny(row["canonical_url"])
		canonicalKey := communityDigestCanonicalKey(canonical)
		if canonicalKey != "" {
			if seenCanonical[canonicalKey] {
				continue
			}
			seenCanonical[canonicalKey] = true
		}
		if len(state.record.Items) >= itemLimit {
			continue
		}
		state.record.Items = append(state.record.Items, communityDigestItem{
			ExternalID:   stringAny(row["external_id"]),
			Title:        stringAny(row["title"]),
			CanonicalURL: canonical,
			Author:       stringAny(row["author"]),
			PublishedAt:  firstNonEmpty(stringAny(row["published_at_text"]), stringAny(row["collected_at_text"])),
			SourceName:   sourceName,
			Context:      communityDigestItemContext(row),
		})
	}
	records := make([]communityDigestRecord, 0, len(order))
	for _, key := range order {
		state := groups[key]
		if state == nil || len(state.record.Items) == 0 {
			continue
		}
		state.record.ItemCount = state.raw
		state.record.DedupedItemCount = len(state.record.Items)
		state.record.Title = cleanCommunityDigestTitle(firstNonEmpty(state.record.Title, communityDigestFallbackTitle(state.record)), state.record)
		records = append(records, state.record)
	}
	return records
}

func communityDigestUUID(digestID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(digestID)))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

func communityDigestItemContext(row map[string]any) string {
	parts := make([]string, 0, 4)
	add := func(value string) {
		value = strings.TrimSpace(removeBoardURLs(stripTags(value)))
		if value == "" {
			return
		}
		for _, existing := range parts {
			if existing == value {
				return
			}
		}
		parts = append(parts, truncateRunes(value, 1800))
	}
	add(stringAny(row["summary"]))

	var raw map[string]any
	if err := json.Unmarshal([]byte(stringAny(row["raw_json"])), &raw); err == nil {
		add(stringAny(raw["text_excerpt"]))
		if detail, _ := raw["summary_detail"].(map[string]any); detail != nil {
			add(stringAny(detail["value"]))
		}
		if content, _ := raw["content"].([]any); len(content) > 0 {
			contentParts := make([]string, 0, minInt(3, len(content)))
			for _, entry := range content {
				if item, _ := entry.(map[string]any); item != nil {
					text := strings.TrimSpace(stringAny(item["value"]))
					if text != "" {
						contentParts = append(contentParts, text)
					}
				}
				if len(contentParts) >= 3 {
					break
				}
			}
			add(strings.Join(contentParts, "\n"))
		}
	}
	return truncateRunes(strings.Join(parts, "\n"), 3000)
}

func communityDigestPrompt(record communityDigestRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are a community activity analyst and meeting-minutes editor for the Web-R Korean website.\n")
	fmt.Fprintf(&b, "Analyze one day of community posts collected from a single source path. The output must be written in Korean.\n\n")
	fmt.Fprintf(&b, "Source path: %s\nDate: %s\nSource type: %s\nPlatform: %s\nItems after URL dedupe: %d\n\n", record.SourceName, record.DigestDate, record.SourceType, record.Platform, len(record.Items))
	b.WriteString("Goal:\n")
	b.WriteString("- Do not merely compress the items. Explain what happened during the day, preserving chronology, context, issues, mood, decisions, technical topics, jokes, conflicts, discoveries, and unresolved points when the evidence supports them.\n")
	b.WriteString("- Write so a reader can later understand the day of activity without reading the original posts.\n")
	b.WriteString("- Use only the provided metadata and excerpts. Do not invent missing replies, decisions, people, conflicts, or reactions.\n")
	b.WriteString("- Do not republish source bodies. Do not include URLs, Markdown links, HTML links, or long verbatim quotes. The original link list is stored separately.\n")
	b.WriteString("- Keep technical details specific when they are visible in the collected excerpts.\n\n")
	b.WriteString("Required output:\n")
	b.WriteString("- Return only one JSON object. Do not wrap it in Markdown fences.\n")
	b.WriteString("- JSON shape: {\"title\":\"Korean representative title\",\"html\":\"safe Korean HTML fragment\"}.\n")
	b.WriteString("- The title must come from the html section named '주요 토픽'. If there is one major topic, use that topic as the title. If there are several major topics, synthesize them into one concise Korean title.\n")
	b.WriteString("- Do not include the date, source path, platform name, or the phrase '일일 요약' in the title.\n")
	b.WriteString("- The title must be concrete and evidence-specific. Include visible package names, functions, errors, datasets, tools, events, or analysis methods when they are central.\n")
	b.WriteString("- Forbidden title patterns: '관련 논의', '이야기', '흐름', '주요 토픽', '질문과 답변 흐름', '해결 단서', '공유와 논의', or broad labels such as 'R 패키지, R 개발 도구'. Write what concrete thing was discussed instead.\n")
	b.WriteString("- Good title examples: '<code>lm</code>/<code>arima</code> SSE 계산 문의', 'Posit Assistant 정리 모드와 RStudio 실행 오류', 'phylotastic 계통수 패키지 업데이트', '반복측정 ANOVA partial eta squared 계산'.\n")
	b.WriteString("- Major topic list items should describe what people discussed. Do not use the source path or platform name itself as a topic.\n")
	b.WriteString("- The html value must be a safe HTML fragment in Korean. The first non-space character of html must be '<'.\n")
	b.WriteString("- Allowed tags inside html: <h2>, <h3>, <p>, <ul>, <ol>, <li>, <strong>, <em>, <mark>, <code>, <pre>, <blockquote>.\n")
	b.WriteString("- Never use Markdown headings, Markdown bullets, tables, HTML <a>, href attributes, images, scripts, styles, iframes, or raw URLs.\n")
	b.WriteString("- Include a <h2>주요 토픽</h2> section when there is any meaningful topic evidence. Do not force every other section template every day. Choose only useful sections and vary headings to match the evidence. The report may include overall flow, timeline, technical/operational points, action items, or mood when those are supported.\n")
	b.WriteString("- For sparse days, write concise natural observations. Do not include apology/limitation phrases such as '정보가 부족합니다', '정보가 매우 부족합니다', '분석하기 어렵습니다', '제공된 데이터만으로는', '알 수 없습니다', or '기록되어 있지 않습니다'. Omit unsupported details instead.\n")
	b.WriteString("- The result should read like a polished community activity report, not a checklist of missing evidence.\n\n")
	b.WriteString("Style:\n")
	b.WriteString("- Korean, readable analytical report style, not a dry bullet-only summary.\n")
	b.WriteString("- Use paragraphs and subheadings. Use <strong> for important actors, functions, package names, errors, and decisions. Use <mark> sparingly for one or two central themes. Use <blockquote> only for paraphrased notable moments, not verbatim long quotes.\n")
	b.WriteString("- It can be long enough to preserve context, but stay focused on evidence.\n\n")
	b.WriteString("Collected items:\n")
	for i, item := range record.Items {
		if i >= 80 {
			break
		}
		fmt.Fprintf(&b, "\nItem %02d\n", i+1)
		fmt.Fprintf(&b, "Title: %s\n", truncate(stripTags(item.Title), 260))
		if item.Author != "" {
			fmt.Fprintf(&b, "Author: %s\n", truncate(stripTags(item.Author), 100))
		}
		if item.PublishedAt != "" {
			fmt.Fprintf(&b, "Time: %s\n", item.PublishedAt)
		}
		if item.Context != "" {
			fmt.Fprintf(&b, "Collected excerpt: %s\n", truncate(stripTags(item.Context), 1200))
		}
	}
	return b.String()
}

func fallbackCommunityDigestSummary(record communityDigestRecord) string {
	titles := make([]string, 0, minInt(5, len(record.Items)))
	for _, item := range record.Items {
		title := strings.TrimSpace(removeBoardURLs(stripTags(item.Title)))
		if title != "" {
			titles = append(titles, title)
		}
		if len(titles) >= 5 {
			break
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<h2>커뮤니티 흐름</h2><p><strong>%s</strong> 경로에서는 중복 링크를 제외하고 <mark>%d건</mark>의 R 커뮤니티 게시물이 관찰되었습니다. ", html.EscapeString(record.SourceName), record.DedupedItemCount)
	if len(titles) > 0 {
		fmt.Fprintf(&b, "수집된 제목 기준으로는 %s 등이 눈에 띄었습니다. ", html.EscapeString(strings.Join(titles, ", ")))
	}
	b.WriteString("원문 본문을 재게시하지 않는 정책에 따라 자세한 내용은 별도 원문 링크 목록으로 분리했습니다.</p>")
	b.WriteString("<h2>주요 토픽</h2><ul>")
	for _, title := range titles {
		fmt.Fprintf(&b, "<li>%s</li>", html.EscapeString(title))
	}
	b.WriteString("</ul><h2>커뮤니티 분위기</h2><p>이날 수집된 흐름은 R 사용, 개발 도구, 시각화, 문제 해결을 중심으로 이어졌습니다.</p>")
	return b.String()
}

func parseCommunityDigestAIResponse(value string) (string, string) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```json")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	value = strings.TrimSpace(value)
	type response struct {
		Title   string `json:"title"`
		HTML    string `json:"html"`
		Summary string `json:"summary"`
		Content string `json:"content"`
	}
	var parsed response
	if err := json.Unmarshal([]byte(value), &parsed); err == nil {
		return parsed.Title, firstNonEmpty(parsed.HTML, parsed.Summary, parsed.Content)
	}
	start := strings.Index(value, "{")
	end := strings.LastIndex(value, "}")
	if start >= 0 && end > start {
		if err := json.Unmarshal([]byte(value[start:end+1]), &parsed); err == nil {
			return parsed.Title, firstNonEmpty(parsed.HTML, parsed.Summary, parsed.Content)
		}
	}
	return "", value
}

func communityDigestFallbackTitle(record communityDigestRecord) string {
	if title := specificCommunityDigestTitleFromRecord(record); title != "" && !isGenericCommunityDigestTitle(title) && !communityDigestTitleNeedsFallback(title) {
		return title
	}
	sourceType := strings.ToLower(strings.TrimSpace(record.SourceType))
	platform := strings.ToLower(strings.TrimSpace(record.Platform))
	switch {
	case sourceType == "qna_feed" || strings.Contains(platform, "stackoverflow"):
		return "R 질문 해결 사례"
	case sourceType == "community_forum" || strings.Contains(platform, "posit"):
		return "R 사용 문의 정리"
	case sourceType == "social_tag" || sourceType == "fediverse_group" || strings.Contains(platform, "mastodon"):
		return "R 커뮤니티 활동 정리"
	default:
		return "R 커뮤니티 주요 토픽"
	}
}

func communityDigestTitleFromSummary(summary, fallback string, record communityDigestRecord) string {
	topics := communityDigestMajorTopics(summary)
	if len(topics) == 1 {
		if title := communityDigestTitleCandidate(topics[0]); title != "" {
			return title
		}
		return cleanCommunityDigestTopicPhrase(topics[0])
	}
	if len(topics) > 1 {
		return summarizeCommunityDigestTopics(topics, record)
	}
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	return communityDigestFallbackTitle(record)
}

func communityDigestMajorTopics(summary string) []string {
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return nil
	}
	headingRE := regexp.MustCompile(`(?is)<h[23][^>]*>\s*주요\s*토픽\s*</h[23]>(.*?)(?:<h[23][^>]*>|$)`)
	match := headingRE.FindStringSubmatch(summary)
	if len(match) < 2 {
		return nil
	}
	itemRE := regexp.MustCompile(`(?is)<li[^>]*>(.*?)</li>`)
	itemMatches := itemRE.FindAllStringSubmatch(match[1], -1)
	strongRE := regexp.MustCompile(`(?is)<strong[^>]*>(.*?)</strong>`)
	topics := make([]string, 0, len(itemMatches))
	for _, item := range itemMatches {
		if len(item) < 2 {
			continue
		}
		topicSource := item[1]
		if strongMatch := strongRE.FindStringSubmatch(topicSource); len(strongMatch) >= 2 {
			topicSource = strongMatch[1]
		}
		topic := cleanCommunityDigestTopicPhrase(topicSource)
		if topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}

func summarizeCommunityDigestTopics(topics []string, record communityDigestRecord) string {
	cleaned := make([]string, 0, len(topics))
	seen := map[string]bool{}
	for _, topic := range topics {
		value := communityDigestTitleCandidate(topic)
		if value == "" || isGenericCommunityDigestTitle(value) || communityDigestTitleNeedsFallback(value) || seen[value] {
			continue
		}
		seen[value] = true
		cleaned = append(cleaned, value)
		if len(cleaned) >= 3 {
			break
		}
	}
	if len(cleaned) == 1 {
		return cleaned[0]
	}
	if len(cleaned) == 2 {
		return cleaned[0] + " / " + cleaned[1]
	}
	if len(cleaned) >= 3 {
		return cleaned[0] + " / " + cleaned[1] + " 외 " + strconv.Itoa(len(cleaned)-2) + "개 토픽"
	}
	if title := specificCommunityDigestTitleFromRecord(record); title != "" && !isGenericCommunityDigestTitle(title) && !communityDigestTitleNeedsFallback(title) {
		return title
	}
	return "R 커뮤니티 활동 정리"
}

func cleanCommunityDigestTitle(value string, record communityDigestRecord) string {
	value = normalizeCommunityDigestEscapedWhitespace(value)
	value = strings.TrimSpace(removeBoardURLs(stripTags(value)))
	value = regexp.MustCompile(`\b20[0-9]{2}-[0-9]{2}-[0-9]{2}\b`).ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "일일 요약", "")
	value = strings.ReplaceAll(value, "하루 요약", "")
	value = stripCommunityDigestTitleSourceTerms(value)
	value = cleanCommunityDigestTopicPhrase(value)
	value = strings.Trim(value, " \t\r\n-_:|·")
	if value == "" || isGenericCommunityDigestTitle(value) || communityDigestTitleNeedsFallback(value) {
		value = bestCommunityDigestTitleFallback(record)
	}
	return value
}

func bestCommunityDigestTitleFallback(record communityDigestRecord) string {
	if title := specificCommunityDigestTitleFromRecord(record); title != "" && !isGenericCommunityDigestTitle(title) && !communityDigestTitleNeedsFallback(title) {
		return title
	}
	return communityDigestFallbackTitle(record)
}

func cleanCommunityDigestTopicPhrase(value string) string {
	value = strings.TrimSpace(removeBoardURLs(stripTags(value)))
	value = strings.ReplaceAll(value, "\uFFFD", "")
	value = stripCommunityDigestTitleSourceTerms(value)
	replacements := []string{
		"관련 논의",
		"관련 이야기",
		"이야기",
		"논의 흐름",
		"질문과 답변 흐름",
		"해결 단서",
		"공유와 논의",
		"주요 토픽",
		"커뮤니티 흐름",
	}
	for _, phrase := range replacements {
		value = strings.ReplaceAll(value, phrase, "")
	}
	value = regexp.MustCompile(`(?i)\b(discussion|story|thread|topic)s?\b`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`([A-Za-z0-9_.+()/#:-]+)\s+(를|을|가|은|는|의|와|과)`).ReplaceAllString(value, "$1$2")
	value = regexp.MustCompile(`\s+`).ReplaceAllString(value, " ")
	value = strings.Trim(value, " \t\r\n-_:|·,，")
	return value
}

func communityDigestTitleCandidate(value string) string {
	value = cleanCommunityDigestTopicPhrase(value)
	if value == "" {
		return ""
	}
	value = simplifyCommunityDigestTitleSentence(value)
	if left, right, ok := cutCommunityDigestTopicColon(value); ok {
		if isGenericCommunityDigestTitle(left) || communityDigestGenericTopicLabel(left) {
			value = right
		} else if len([]rune(left)) >= 4 && len([]rune(left)) <= 55 {
			value = left
		}
	}
	value = simplifyCommunityDigestTitleSentence(value)
	return strings.Trim(value, " \t\r\n-_:|·,，")
}

func cutCommunityDigestTopicColon(value string) (string, string, bool) {
	match := regexp.MustCompile(`^(.{2,80})[:：]\s+(.+)$`).FindStringSubmatch(value)
	if len(match) == 3 {
		return strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), true
	}
	return "", "", false
}

func simplifyCommunityDigestTitleSentence(value string) string {
	value = regexp.MustCompile(`([A-Za-z][A-Za-z0-9_.+-]*) 패키지의 새 버전 출시와 ([A-Za-z][A-Za-z0-9_.+-]*) 패키지의 업데이트 소식이 공유되었습니다`).ReplaceAllString(value, "$1 패키지 새 버전과 $2 업데이트")
	value = regexp.MustCompile(`([A-Za-z][A-Za-z0-9_.+-]*) 패키지의 새 버전 출시`).ReplaceAllString(value, "$1 패키지 새 버전")
	value = regexp.MustCompile(`([A-Za-z][A-Za-z0-9_.+-]*) 패키지의 업데이트 소식`).ReplaceAllString(value, "$1 패키지 업데이트")
	value = strings.ReplaceAll(value, "Positron IDE의 새로운 패키지 관리 기능에 대한 탐색 및 소개가 있었습니다", "Positron IDE 패키지 관리 기능")
	value = strings.ReplaceAll(value, "R 및 Python 패키지 업데이트 확인", "R/Python 패키지 업데이트 확인")
	value = regexp.MustCompile(`Nuts 패키지가 최신 NUTS 버전을 지원하지 않는 문제.*`).ReplaceAllString(value, "Nuts 패키지 NUTS 버전 지원 문제")
	value = regexp.MustCompile(`계통수 분석 및 관련 데이터 처리에 유용한 여러 패키지들이 소개.*`).ReplaceAllString(value, "phylotastic 계통수 패키지 업데이트")
	value = regexp.MustCompile(`(?i)(?:#TidyTuesday\s+)?2026년 19주차 챌린지.*`).ReplaceAllString(value, "Twinned Cities 데이터 시각화 챌린지")
	value = regexp.MustCompile(`^.*게시물이 등록되었습니다\.?\s*주요 내용은\s*`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`^주제별 활발한 활동을 보였습니다\.?\s*`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`(에 대한 질문과 해결책 모색|에 대한 탐색 및 소개|에 대한 논의|에 대한 질문|이 공유되었습니다|가 공유되었습니다|이 있었습니다|가 있었습니다|이루어졌습니다|제기되었습니다|논의되었습니다)\.?$`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`\s+`).ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func communityDigestGenericTopicLabel(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	genericLabels := []string{
		"r 패키지 업데이트",
		"r 패키지 및 기술 관련 질문/정보",
		"r 시각화",
		"ide 기능 탐색",
		"기술/운영 포인트",
		"서비스 소개 및 홍보",
		"운영 관련 이야기",
		"프로그래밍 언어 및 컴파일러",
		"웹 기술 및 개발 도구",
		"웹 개발 및 인프라",
		"시스템 엔지니어링 및 데이터베이스",
		"데이터 처리 및 분석",
		"r 패키지 및 개발",
		"r 패키지 및 도구 소개",
		"데이터 시각화",
		"패키지 호환성 문제",
	}
	for _, label := range genericLabels {
		if normalized == label {
			return true
		}
	}
	return false
}

func specificCommunityDigestTitleFromRecord(record communityDigestRecord) string {
	titles := make([]string, 0, 3)
	seen := map[string]bool{}
	for _, item := range record.Items {
		title := communityDigestTitleCandidate(item.Title)
		if title == "" || isGenericCommunityDigestTitle(title) || seen[title] {
			continue
		}
		seen[title] = true
		titles = append(titles, title)
		if len(titles) >= 2 {
			break
		}
	}
	if len(titles) == 1 {
		return titles[0]
	}
	if len(titles) >= 2 {
		return titles[0] + " / " + titles[1]
	}
	return ""
}

func communityDigestTitleNeedsFallback(value string) bool {
	value = strings.TrimSpace(stripTags(value))
	if value == "" {
		return true
	}
	if strings.Contains(value, `\n`) || strings.Contains(value, `\r`) || strings.Contains(value, `\t`) {
		return true
	}
	runes := []rune(value)
	if len(runes) > 90 {
		return true
	}
	korean := 0
	latin := 0
	for _, r := range runes {
		if (r >= 0xAC00 && r <= 0xD7A3) || (r >= 0x1100 && r <= 0x11FF) || (r >= 0x3130 && r <= 0x318F) {
			korean++
			continue
		}
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			latin++
		}
	}
	if korean == 0 && latin >= 20 && len(runes) > 45 {
		return true
	}
	if korean == 0 && len(regexp.MustCompile(`[A-Za-z][A-Za-z0-9_+-]*`).FindAllString(value, -1)) >= 4 {
		return true
	}
	if strings.HasPrefix(strings.TrimSpace(value), "#") && korean == 0 && latin >= 8 {
		return true
	}
	return false
}

func isGenericCommunityDigestTitle(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(stripTags(value)))
	if normalized == "" {
		return true
	}
	genericPhrases := []string{
		"관련 논의",
		"이야기",
		"흐름",
		"주요 토픽",
		"질문과 답변 흐름",
		"해결 단서",
		"공유와 논의",
		"r 커뮤니티 활동 정리",
		"r 커뮤니티 주요 토픽",
	}
	for _, phrase := range genericPhrases {
		if strings.Contains(normalized, strings.ToLower(phrase)) {
			return true
		}
	}
	broadLabels := []string{"r 시각화", "r 통계 모델링", "r 패키지", "r 개발 도구", "r 오류 해결", "r 데이터 작업", "r 커뮤니티 공유"}
	broadCount := 0
	for _, label := range broadLabels {
		if strings.Contains(normalized, label) {
			broadCount++
		}
	}
	if broadCount >= 2 && len([]rune(normalized)) <= 40 {
		return true
	}
	for _, label := range broadLabels {
		if normalized == label {
			return true
		}
	}
	return false
}

func stripCommunityDigestTitleSourceTerms(value string) string {
	terms := []string{
		"Stack Overflow",
		"Posit Community",
		"Posit",
		"Mastodon #rstats",
		"#rstats",
		"Fediverse",
		"rstats@a.gup.pe",
		"rstats@fosstodon.org",
		"rstats@",
		"gup.pe",
		"r/rprogramming",
		"r/RStudio",
		"Reddit",
		"#TidyTuesday",
		"TidyTuesday",
	}
	for _, term := range terms {
		re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(term))
		value = re.ReplaceAllString(value, "")
	}
	value = regexp.MustCompile(`\s+`).ReplaceAllString(value, " ")
	value = regexp.MustCompile(`^\s*(의|에서|와|과|및|:|-|·)\s*`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`\s+(의|에서)\s*`).ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func cleanCommunityDigestSummary(value string) string {
	value = normalizeCommunityDigestEscapedWhitespace(value)
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```html")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	value = strings.TrimSpace(value)
	sanitized, err := sanitizeBoardHTML(value)
	if err != nil || strings.TrimSpace(stripTags(sanitized)) == "" {
		return safeHTML(truncateRunes(removeCommunityDigestLimitationPhrases(removeBoardURLs(stripTags(normalizeCommunityDigestEscapedWhitespace(value)))), 3000))
	}
	sanitized = normalizeCommunityDigestEscapedWhitespace(sanitized)
	sanitized = removeCommunityDigestLimitationPhrases(sanitized)
	if strings.TrimSpace(stripTags(sanitized)) == "" {
		return fallbackCommunityDigestSummary(communityDigestRecord{DigestDate: nowKST().Format("2006-01-02"), SourceName: "R Community"})
	}
	return truncateRunes(sanitized, 6000)
}

func normalizeCommunityDigestEscapedWhitespace(value string) string {
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, `\r\n`, "\n")
	value = strings.ReplaceAll(value, `\n`, "\n")
	value = strings.ReplaceAll(value, `\r`, "\n")
	value = strings.ReplaceAll(value, `\t`, " ")
	value = regexp.MustCompile(`[ \t]+\n`).ReplaceAllString(value, "\n")
	value = regexp.MustCompile(`\n[ \t]+`).ReplaceAllString(value, "\n")
	value = regexp.MustCompile(`\n{3,}`).ReplaceAllString(value, "\n\n")
	return strings.TrimSpace(value)
}

func removeCommunityDigestLimitationPhrases(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	phrases := []string{
		"제공된 데이터만으로는 명확한 액션 아이템을 도출하기 어렵습니다.",
		"제공된 데이터만으로는 커뮤니티의 전반적인 분위기, 관심사, 긴장감, 유머, 문화적 신호 등을 분석하기에는 정보가 매우 부족합니다.",
		"정보가 부족합니다",
		"정보가 매우 부족합니다",
		"데이터가 부족합니다",
		"분석하기에는 정보가 매우 부족합니다",
		"분석하기 어렵습니다",
		"제공된 데이터만으로는",
		"알 수 없습니다",
		"기록되어 있지 않습니다",
	}
	for _, phrase := range phrases {
		quoted := regexp.QuoteMeta(phrase)
		for _, tag := range []string{"p", "li", "blockquote"} {
			re := regexp.MustCompile(`(?is)<` + tag + `>[^<]*` + quoted + `.*?</` + tag + `>`)
			value = re.ReplaceAllString(value, "")
		}
		value = strings.ReplaceAll(value, phrase, "")
	}
	value = regexp.MustCompile(`(?is)<li>\s*</li>`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`(?is)<p>\s*</p>`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`(?is)<ul>\s*</ul>`).ReplaceAllString(value, "")
	value = regexp.MustCompile(`(?is)<ol>\s*</ol>`).ReplaceAllString(value, "")
	return strings.TrimSpace(value)
}

func communityDigestEvents(records []communityDigestRecord) []genericEvent {
	events := make([]genericEvent, 0, len(records))
	for _, record := range records {
		payload := communityDigestPayload(record)
		observedAt := record.DigestDate + "T00:00:00+09:00"
		events = append(events, newGenericEvent("r.community.daily_digest.v1", record.SourceID, record.SourceURL, "R-Community", "", "", observedAt, payload))
	}
	return events
}

func communityDigestPayload(record communityDigestRecord) map[string]any {
	return map[string]any{
		"payload_schema":        "r_community_daily_digest_v1",
		"source_method":         "ai_daily_source_path_digest",
		"collection_status":     record.Status,
		"digest_id":             record.DigestID,
		"digest_uuid":           record.DigestUUID,
		"digest_date":           record.DigestDate,
		"source_type":           record.SourceType,
		"source_id":             record.SourceID,
		"source_name":           record.SourceName,
		"platform":              record.Platform,
		"source_url":            record.SourceURL,
		"title":                 record.Title,
		"summary":               record.Summary,
		"item_count":            record.ItemCount,
		"deduped_item_count":    record.DedupedItemCount,
		"source_items":          record.Items,
		"model":                 record.Model,
		"prompt_hash":           record.PromptHash,
		"generated_at":          record.GeneratedAt,
		"payload_hash":          record.PayloadHash,
		"copyright_policy":      "digest_and_links_only",
		"dedupe_policy":         "canonical_url_global_per_run",
		"excluded_source_types": []string{"korean_community_site", "korean_community_forum", "community_support_index", "user_group_index", "community_archive"},
	}
}

func communityDigestPayloadHash(record communityDigestRecord) string {
	payload := communityDigestPayload(record)
	delete(payload, "payload_hash")
	body, _ := json.Marshal(payload)
	return shaHex(string(body))
}

func insertCommunityDigestRecords(ctx context.Context, cfg clickHouseQueryConfig, records []communityDigestRecord) error {
	if len(records) == 0 {
		return nil
	}
	rows, err := communityDigestDirectRows(records, nowKST())
	if err != nil {
		return err
	}
	return insertDirectRows(ctx, cfg, "Data_R_Community_Service.r_community_daily_digest", rows)
}

func communityDigestDirectRows(records []communityDigestRecord, now time.Time) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		itemsJSON, err := json.Marshal(record.Items)
		if err != nil {
			return nil, err
		}
		rows = append(rows, map[string]any{
			"digest_id":          record.DigestID,
			"digest_uuid":        record.DigestUUID,
			"digest_date":        record.DigestDate,
			"source_type":        record.SourceType,
			"source_id":          record.SourceID,
			"source_name":        record.SourceName,
			"platform":           record.Platform,
			"source_url":         record.SourceURL,
			"title":              record.Title,
			"summary":            record.Summary,
			"item_count":         record.ItemCount,
			"deduped_item_count": record.DedupedItemCount,
			"source_items_json":  string(itemsJSON),
			"model":              record.Model,
			"prompt_hash":        record.PromptHash,
			"generation_status":  record.Status,
			"created_at":         parseKSTTime(record.GeneratedAt, now).Format("2006-01-02 15:04:05"),
			"updated_at":         now.Format("2006-01-02 15:04:05"),
			"payload_hash":       record.PayloadHash,
			"active":             1,
			"version":            uint64(now.UnixMilli()),
		})
	}
	return rows, nil
}

func communityDigestCanonicalKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return strings.ToLower(raw)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Fragment = ""
	if parsed.Path != "/" && strings.HasSuffix(parsed.Path, "/") {
		parsed.Path = strings.TrimRight(parsed.Path, "/")
	}
	q := parsed.Query()
	for key := range q {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "utm_") || lower == "fbclid" || lower == "gclid" || lower == "ref" || lower == "source" {
			q.Del(key)
		}
	}
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

type packageJobLimits struct {
	metadataLimit                       int
	downloadTop                         int
	reverseLimit                        int
	checkLimit                          int
	archiveLimit                        int
	taskViewLimit                       int
	newsLimit                           int
	packagePageLimit                    int
	packagePagePackages                 []string
	packageArtifactLimit                int
	packageManualTopicLimit             int
	bioconductorPackagePageLimit        int
	bioconductorPackagePagePackages     []string
	bioconductorPackageArtifactLimit    int
	bioconductorPackageManualTopicLimit int
	runiversePackagePageLimit           int
	runiversePackagePagePackages        []string
	runiversePackageArtifactLimit       int
	runiversePackageManualTopicLimit    int
	websiteLimit                        int
	websiteCandidateLimit               int
	websiteFeedLimit                    int
	websiteLinkLimit                    int
	websiteSitemapLimit                 int
	githubLimit                         int
	osvLimit                            int
	bibliometricLimit                   int
}

func collectPackageJob(job string, records func() ([]cranRecord, error), limits packageJobLimits) ([]genericEvent, error) {
	switch job {
	case "cran-metadata":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectCRANMetadata(rows, limits.metadataLimit), nil
	case "cran-downloads":
		return collectCRANDownloads(limits.downloadTop)
	case "cran-reverse-dependencies":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectReverseDependencies(rows, limits.reverseLimit), nil
	case "cran-checks":
		return collectCRANChecks(limits.checkLimit)
	case "cran-archive":
		return collectCRANArchive(limits.archiveLimit)
	case "cran-task-views":
		return collectCRANTaskViews(limits.taskViewLimit)
	case "r-core-news":
		return collectRCoreNEWS()
	case "package-news":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectPackageNEWS(rows, limits.newsLimit)
	case "cran-package-pages":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectCRANPackagePages(rows, limits.packagePageLimit, limits.packagePagePackages, limits.packageArtifactLimit, limits.packageManualTopicLimit)
	case "bioconductor":
		return collectBioconductor()
	case "bioconductor-package-pages":
		return collectBioconductorPackagePages(limits.bioconductorPackagePageLimit, limits.bioconductorPackagePagePackages, limits.bioconductorPackageArtifactLimit, limits.bioconductorPackageManualTopicLimit)
	case "runiverse":
		return collectRUniverse()
	case "runiverse-package-pages":
		return collectRUniversePackagePages(limits.runiversePackagePageLimit, limits.runiversePackagePagePackages, limits.runiversePackageArtifactLimit, limits.runiversePackageManualTopicLimit)
	case "cran-website-discovery":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectCRANWebsiteDiscovery(rows, limits.websiteCandidateLimit), nil
	case "r-websites":
		return collectRWebsites(limits.websiteLimit, limits.websiteFeedLimit, limits.websiteLinkLimit, limits.websiteSitemapLimit)
	case "github-repos":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectGitHubRepositories(rows, limits.githubLimit)
	case "osv-security":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectOSVSecurity(rows, limits.osvLimit)
	case "bibliometric-mentions":
		rows, err := records()
		if err != nil {
			return nil, err
		}
		return collectBibliometricMentions(rows, limits.bibliometricLimit)
	default:
		return nil, fmt.Errorf("unknown package job %q", job)
	}
}

func runYouTube(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("youtube", flag.ExitOnError)
	job := fs.String("job", envString("R_YOUTUBE_JOB", "all"), "all, seeds, pages, search, links, videos, transcripts, comments, backfill-metadata")
	topic := fs.String("topic", envString("R_YOUTUBE_KAFKA_TOPIC", defaultYouTubeTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	seedLimit := fs.Int("seed-limit", envInt("R_YOUTUBE_SEED_LIMIT", 0), "seed limit")
	pageLimit := fs.Int("page-limit", envInt("R_YOUTUBE_PAGE_LIMIT", 30), "HTML page fetch limit")
	videoLimit := fs.Int("video-limit", envInt("R_YOUTUBE_VIDEO_LIMIT", 30), "YouTube video metadata enrichment limit")
	transcriptLimit := fs.Int("transcript-limit", envInt("R_YOUTUBE_TRANSCRIPT_VIDEO_LIMIT", 10), "YouTube transcript enrichment video limit")
	commentLimit := fs.Int("comment-limit", envInt("R_YOUTUBE_COMMENT_VIDEO_LIMIT", 10), "YouTube comment enrichment video limit")
	backfillLimit := fs.Int("backfill-limit", envInt("R_YOUTUBE_BACKFILL_LIMIT", 30), "existing weak current video metadata backfill limit")
	fs.Parse(args)

	pub := newPublisher(*topic, "statground-ryoutube-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	jobs := expandJobs(*job, []string{"seeds", "pages", "search", "links", "videos", "transcripts", "comments", "backfill-metadata"})
	total := 0
	for _, currentJob := range jobs {
		events, err := collectYouTubeJob(currentJob, *seedLimit, *pageLimit, *videoLimit, *backfillLimit, *transcriptLimit, *commentLimit)
		if err != nil {
			return fmt.Errorf("%s: %w", currentJob, err)
		}
		if err := pub.publishGeneric(ctx, events); err != nil {
			return fmt.Errorf("%s publish: %w", currentJob, err)
		}
		fmt.Printf("job=%s published=%d\n", currentJob, len(events))
		total += len(events)
	}
	fmt.Printf("published=%d\n", total)
	return nil
}

func runMastodon(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mastodon", flag.ExitOnError)
	topic := fs.String("topic", envString("KAFKA_TOPIC", defaultWebRTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	limit := fs.Int("limit", envInt("MASTODON_LIMIT", 40), "RSS item limit")
	instance := fs.String("instance", envString("MASTODON_INSTANCE", "https://fosstodon.org"), "Mastodon instance")
	acct := fs.String("acct", envString("MASTODON_ACCT", "R_Foundation"), "Mastodon account")
	translate := fs.Bool("translate", envBool("MASTODON_TRANSLATE_ENABLED", true), "translate Mastodon board payloads to Korean")
	staleLimit := fs.Int("stale-translation-limit", envInt("MASTODON_STALE_TRANSLATION_LIMIT", 20), "missing or fallback board rows to translate from ClickHouse")
	backfillOnly := fs.Bool("backfill-board", envBool("MASTODON_BACKFILL_BOARD", false), "only translate missing or fallback board rows from ClickHouse")
	translationModel := fs.String("translation-model", envString("MASTODON_TRANSLATION_MODEL", envString("RBLOGGER_TRANSLATION_MODEL", "google/gemini-2.0-flash-exp:free")), "AI model for Korean board translation")
	fs.Parse(args)

	var ai *aiClient
	if *translate || *staleLimit > 0 || *backfillOnly {
		ai = newAIClient(time.Duration(maxInt(30, envInt("AI_TIMEOUT", 300))) * time.Second)
		if !ai.enabled() {
			return errors.New("Mastodon board translation is enabled, but no AI provider key is configured")
		}
	}
	pub := newPublisher(*topic, "statground-mastodon-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	var chCfg *clickHouseQueryConfig
	if (*translate && !*dryRun) || *staleLimit > 0 || *backfillOnly {
		cfg, err := newClickHouseQueryConfig()
		if err != nil {
			return err
		}
		chCfg = &cfg
	}
	dedup := emptyMastodonDedupState()
	if chCfg != nil && !*backfillOnly {
		loaded, err := loadMastodonDedupState(ctx, *chCfg)
		if err != nil {
			return err
		}
		dedup = loaded
	}
	events := make([]webREvent, 0)
	if !*backfillOnly {
		currentEvents, err := collectMastodonRSS(*instance, *acct, *limit, ai, *translationModel, *translate, dedup)
		if err != nil {
			return err
		}
		events = append(events, currentEvents...)
	}
	if *staleLimit > 0 || *backfillOnly {
		backfillEvents, err := collectMastodonBoardBackfill(ctx, *chCfg, maxInt(1, *staleLimit), ai, *translationModel)
		if err != nil {
			return err
		}
		events = append(events, backfillEvents...)
	}
	if err := pub.publishWebR(ctx, events); err != nil {
		return err
	}
	fmt.Printf("published=%d\n", len(events))
	return nil
}

func collectCRANMetadata(records []cranRecord, limit int) []genericEvent {
	events := make([]genericEvent, 0, len(records))
	for _, record := range records {
		if limit > 0 && len(events) >= limit {
			break
		}
		packageName := record["Package"]
		if packageName == "" {
			continue
		}
		version := record["Version"]
		payload := map[string]any{
			"package":             packageName,
			"version":             version,
			"title":               record["Title"],
			"description":         record["Description"],
			"license":             record["License"],
			"maintainer":          record["Maintainer"],
			"author":              record["Author"],
			"authors_at_r":        record["Authors@R"],
			"depends":             record["Depends"],
			"imports":             record["Imports"],
			"suggests":            record["Suggests"],
			"linking_to":          record["LinkingTo"],
			"enhances":            record["Enhances"],
			"system_requirements": record["SystemRequirements"],
			"needs_compilation":   record["NeedsCompilation"],
			"date_publication":    record["Date/Publication"],
			"repository":          firstNonEmpty(record["Repository"], "CRAN"),
			"url":                 record["URL"],
			"bug_reports":         record["BugReports"],
			"md5sum":              record["MD5sum"],
			"source_method":       "cran_packages_gz",
			"collection_status":   "collected",
		}
		events = append(events, newGenericEvent("rpkg.cran.package_snapshot.v1", "cran_packages_gz", cranPackagesURL(), "CRAN", packageName, version, "", payload))
		for _, repoURL := range repositoryURLs(record) {
			events = append(events, newGenericEvent("rpkg.upstream.repository.detected.v1", "cran_description_url_fields", repoURL, "CRAN", packageName, version, "", map[string]any{
				"package":           packageName,
				"repository_url":    repoURL,
				"detection_source":  "DESCRIPTION URL/BugReports",
				"source_method":     "cran_packages_gz_no_api",
				"collection_status": "collected",
			}))
		}
	}
	return events
}

func collectCRANDownloads(top int) ([]genericEvent, error) {
	if top <= 0 {
		return nil, nil
	}
	packages, err := cranlogsTop(top)
	if err != nil {
		return nil, err
	}
	period := envString("RPKG_DOWNLOAD_PERIOD", "last-month")
	events := make([]genericEvent, 0, top*31)
	for _, packageName := range packages {
		sourceURL := fmt.Sprintf("%s/downloads/daily/%s/%s", cranlogsBaseURL(), url.PathEscape(period), url.PathEscape(packageName))
		var decoded map[string]any
		if err := fetchJSON(sourceURL, &decoded); err != nil {
			events = append(events, collectionFailureEvent("rpkg.cran.download.failure.v1", "cranlogs", sourceURL, "CRAN", packageName, err))
			continue
		}
		rows, _ := decoded["downloads"].([]any)
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			day := stringAny(firstPresent(row, "day", "date"))
			payload := map[string]any{
				"package":           firstNonEmpty(stringAny(row["package"]), packageName),
				"download_date":     day,
				"downloads":         intAny(row["downloads"]),
				"period":            period,
				"source":            "cranlogs",
				"source_method":     "cranlogs_public_json",
				"collection_status": "collected",
			}
			events = append(events, newGenericEvent("rpkg.cran.download_daily.v1", "cranlogs", sourceURL, "CRAN", packageName, "", dayToObserved(day), payload))
		}
	}
	return events, nil
}

func collectReverseDependencies(records []cranRecord, limit int) []genericEvent {
	fields := []string{"Depends", "Imports", "Suggests", "LinkingTo", "Enhances"}
	snapshotDate := time.Now().UTC().Format("2006-01-02")
	events := make([]genericEvent, 0)
	for _, record := range records {
		fromPackage := record["Package"]
		if fromPackage == "" {
			continue
		}
		for _, field := range fields {
			for _, dep := range parseDependencies(record[field]) {
				if limit > 0 && len(events) >= limit {
					return events
				}
				payload := map[string]any{
					"snapshot_date":     snapshotDate,
					"source":            "CRAN",
					"from_repository":   "CRAN",
					"from_package":      fromPackage,
					"from_version":      record["Version"],
					"to_package":        dep.name,
					"dependency_type":   field,
					"dependency_spec":   dep.spec,
					"source_method":     "cran_packages_gz_dependency_parser",
					"collection_status": "collected",
				}
				events = append(events, newGenericEvent("rpkg.cran.dependency_edge_snapshot.v1", "cran_packages_gz_dependency_parser", cranPackagesURL(), "CRAN", fromPackage, record["Version"], snapshotDate+"T00:00:00Z", payload))
			}
		}
	}
	return events
}

func collectCRANChecks(limit int) ([]genericEvent, error) {
	sourceURL := envString("RPKG_CRAN_CHECK_SUMMARY_URL", "https://cran.r-project.org/web/checks/check_summary.html")
	body, err := fetchBytes(sourceURL)
	if err != nil {
		return nil, err
	}
	observed := utcNow()
	events := make([]genericEvent, 0)
	for _, tr := range trRE.FindAllStringSubmatch(string(body), -1) {
		cells := htmlCells(tr[1])
		if len(cells) < 2 {
			continue
		}
		packageName := strings.TrimSpace(cells[0])
		if packageName == "" || strings.EqualFold(packageName, "Package") || strings.HasPrefix(strings.ToLower(packageName), "summary") {
			continue
		}
		payload := map[string]any{
			"package":           packageName,
			"version":           "",
			"flavor":            "summary",
			"status":            worstStatus(cells[1:]),
			"raw_cells":         cells,
			"checked_at":        observed,
			"source":            "CRAN check summary",
			"source_method":     "cran_check_summary_html",
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent("rpkg.cran.check_snapshot.v1", "cran_check_summary_html", sourceURL, "CRAN", packageName, "", observed, payload))
		if limit > 0 && len(events) >= limit {
			break
		}
	}
	return events, nil
}

func collectCRANArchive(limit int) ([]genericEvent, error) {
	sourceURL := envString("RPKG_CRAN_ARCHIVE_INDEX_URL", "https://cran.r-project.org/src/contrib/Archive/")
	body, err := fetchBytes(sourceURL)
	if err != nil {
		return nil, err
	}
	observed := utcNow()
	events := make([]genericEvent, 0)
	for _, match := range linkRE.FindAllStringSubmatch(string(body), -1) {
		href := strings.TrimSpace(match[1])
		if !strings.HasSuffix(href, "/") || strings.HasPrefix(href, "?") || href == "../" {
			continue
		}
		packageName := strings.Trim(href, "/")
		if packageName == "" {
			continue
		}
		archiveURL := sourceURL + url.PathEscape(packageName) + "/"
		payload := map[string]any{
			"package":           packageName,
			"archive_url":       archiveURL,
			"is_archived":       true,
			"source":            "CRAN Archive index",
			"source_method":     "cran_archive_index_html",
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent("rpkg.cran.archive_snapshot.v1", "cran_archive_index_html", sourceURL, "CRAN", packageName, "", observed, payload))
		if limit > 0 && len(events) >= limit {
			break
		}
	}
	return events, nil
}

func collectCRANTaskViews(limit int) ([]genericEvent, error) {
	indexURL := envString("RPKG_CRAN_TASK_VIEWS_URL", "https://cran.r-project.org/web/views/")
	body, err := fetchBytes(indexURL)
	if err != nil {
		return nil, err
	}
	viewURLs := cranTaskViewURLs(indexURL, string(body), limit)
	events := make([]genericEvent, 0)
	for _, viewURL := range viewURLs {
		viewBody, err := fetchBytes(viewURL)
		if err != nil {
			events = append(events, collectionFailureEvent("rpkg.cran.task_view.failure.v1", "cran_task_view_html", viewURL, "CRAN Task Views", "", err))
			continue
		}
		htmlText := string(viewBody)
		viewName := taskViewName(viewURL, htmlText)
		packages := taskViewPackages(htmlText)
		payload := map[string]any{
			"task_view":         viewName,
			"title":             firstNonEmpty(firstTitle(htmlText), viewName),
			"source_url":        viewURL,
			"package_count":     intString(len(packages)),
			"packages":          packages,
			"packages_json":     mustJSON(packages),
			"headings":          textMatches(headingRE, htmlText, 20),
			"source_method":     "cran_task_view_html",
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent("rpkg.cran.task_view.snapshot.v1", "cran_task_view_html", viewURL, "CRAN Task Views", "", "", "", payload))
		for _, packageName := range packages {
			edgePayload := map[string]any{
				"task_view":         viewName,
				"package":           packageName,
				"source_url":        viewURL,
				"source_method":     "cran_task_view_package_link_parser",
				"collection_status": "collected",
			}
			events = append(events, newGenericEvent("rpkg.cran.task_view.package_edge_snapshot.v1", "cran_task_view_package_link_parser", viewURL, "CRAN Task Views", packageName, "", "", edgePayload))
		}
	}
	return events, nil
}

func collectRCoreNEWS() ([]genericEvent, error) {
	sourceURL := envString("RPKG_R_CORE_NEWS_URL", "https://cran.r-project.org/doc/manuals/r-release/NEWS.html")
	body, err := fetchBytes(sourceURL)
	if err != nil {
		return nil, err
	}
	htmlText := string(body)
	headings := textMatches(headingRE, htmlText, 100)
	items := textMatches(listItemRE, htmlText, 200)
	title := firstNonEmpty(firstTitle(htmlText), "R NEWS")
	payload := map[string]any{
		"title":              title,
		"headings":           headings,
		"headings_json":      mustJSON(headings),
		"entries":            items,
		"entry_count":        len(items),
		"html_sha256_source": sourceURL,
		"content_length":     len(body),
		"source_method":      "r_core_news_html",
		"collection_status":  "collected",
	}
	return []genericEvent{newGenericEvent("rpkg.r_core.news_snapshot.v1", "r_core_news_html", sourceURL, "R-Core", "", "", "", payload)}, nil
}

func collectPackageNEWS(records []cranRecord, limit int) ([]genericEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	events := make([]genericEvent, 0)
	for _, record := range records {
		if len(events) >= limit {
			break
		}
		packageName := record["Package"]
		if packageName == "" {
			continue
		}
		for _, candidate := range newsCandidates {
			sourceURL := fmt.Sprintf("https://cran.r-project.org/web/packages/%s/%s", url.PathEscape(packageName), candidate)
			body, err := fetchBytes(sourceURL)
			if err != nil || len(body) == 0 {
				continue
			}
			text := stripTags(string(body))
			if len(strings.TrimSpace(text)) < 40 {
				continue
			}
			payload := map[string]any{
				"package":           packageName,
				"version":           record["Version"],
				"news_title":        firstNonEmpty(firstTitle(string(body)), packageName+" NEWS"),
				"entry_text":        truncate(text, 12000),
				"entry_html":        truncate(string(body), 20000),
				"source_url":        sourceURL,
				"source_method":     "cran_rendered_news_html",
				"parser_version":    1,
				"collection_status": "collected",
			}
			events = append(events, newGenericEvent("rpkg.package.news_snapshot.v1", "cran_rendered_news_html", sourceURL, "CRAN", packageName, record["Version"], "", payload))
			break
		}
	}
	return events, nil
}

type cranPageLink struct {
	Label   string `json:"label"`
	URL     string `json:"url"`
	Section string `json:"section,omitempty"`
	Type    string `json:"type,omitempty"`
}

type packagePageBatch struct {
	SourceKey        string
	CursorKey        string
	NextCursorKey    string
	Limit            int
	TotalCandidates  int
	ForcedCount      int
	SelectedCount    int
	SelectedPackages []string
	SelectedItemKeys []string
}

func collectCRANPackagePages(records []cranRecord, limit int, packageNames []string, artifactLimit, manualTopicLimit int) ([]genericEvent, error) {
	if len(records) == 0 {
		return nil, nil
	}
	sourceKey := "cran-package-pages|repository=CRAN"
	selected, batch := selectPackagePageRecords(records, limit, packageNames, sourceKey)
	events := make([]genericEvent, 0, len(selected))
	for _, record := range selected {
		packageName := strings.TrimSpace(record["Package"])
		if packageName == "" {
			continue
		}
		sourceURL := fmt.Sprintf("https://cran.r-project.org/web/packages/%s/index.html", url.PathEscape(packageName))
		body, err := fetchBytes(sourceURL)
		if err != nil {
			events = append(events, collectionFailureEvent("rpkg.cran.package_page.failure.v1", "cran_package_index_html", sourceURL, "CRAN", packageName, err))
			continue
		}
		htmlText := string(body)
		payload := cranPackagePagePayload(sourceURL, htmlText, record)
		payload["content_length"] = len(body)
		payload["html_sha256"] = shaHex(htmlText)
		events = append(events, newGenericEvent("rpkg.cran.package_page_snapshot.v1", "cran_package_index_html", sourceURL, "CRAN", packageName, record["Version"], "", payload))
		events = append(events, collectCRANPackageArtifacts(record, sourceURL, cranPackageArtifactLinks(payload), artifactLimit)...)
		events = append(events, collectCRANPackageManualTopics(record, sourceURL, stringAny(payload["package_source_url"]), manualTopicLimit)...)
	}
	if batch.SelectedCount > 0 {
		events = append(events, newPackagePageBatchCursorEvent("CRAN", "cran_package_page_batch_cursor", batch))
	}
	return events, nil
}

func selectPackagePageRecords(records []cranRecord, limit int, packageNames []string, sourceKey string) ([]cranRecord, packagePageBatch) {
	batch := packagePageBatch{SourceKey: sourceKey, Limit: limit}
	if len(records) == 0 {
		return nil, batch
	}
	include := packageNameIncludeSet(packageNames)
	selected := make([]cranRecord, 0)
	selectedKeys := map[string]bool{}
	for _, record := range records {
		key := cranPackageRecordBatchKey(record)
		if key == "" || !include[key] || selectedKeys[key] {
			continue
		}
		selected = append(selected, record)
		selectedKeys[key] = true
		batch.SelectedPackages = append(batch.SelectedPackages, strings.TrimSpace(record["Package"]))
		batch.SelectedItemKeys = append(batch.SelectedItemKeys, key)
	}
	batch.ForcedCount = len(selected)
	if limit > 0 && len(selected) >= limit {
		batch.SelectedCount = len(selected)
		batch.TotalCandidates = countUniqueCRANPackageRecords(records)
		batch.CursorKey = latestPackagePageBatchCursor(sourceKey)
		batch.NextCursorKey = batch.CursorKey
		logPackagePageBatch(batch)
		return selected, batch
	}
	type cranBatchCandidate struct {
		key    string
		record cranRecord
	}
	out := make([]cranBatchCandidate, 0, len(records))
	outSeen := map[string]bool{}
	for _, record := range records {
		key := cranPackageRecordBatchKey(record)
		if key == "" || selectedKeys[key] || outSeen[key] {
			continue
		}
		out = append(out, cranBatchCandidate{key: key, record: record})
		outSeen[key] = true
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].key < out[j].key
	})
	batch.TotalCandidates = len(selected) + len(out)
	outKeys := make([]string, 0, len(out))
	for _, candidate := range out {
		outKeys = append(outKeys, candidate.key)
	}
	batch.CursorKey = latestPackagePageBatchCursor(sourceKey)
	if limit <= 0 {
		for _, candidate := range out {
			record := candidate.record
			selected = append(selected, record)
			batch.SelectedPackages = append(batch.SelectedPackages, strings.TrimSpace(record["Package"]))
			batch.SelectedItemKeys = append(batch.SelectedItemKeys, candidate.key)
			batch.NextCursorKey = candidate.key
		}
		batch.SelectedCount = len(selected)
		logPackagePageBatch(batch)
		return selected, batch
	}
	remaining := limit - len(selected)
	if remaining <= 0 || len(out) == 0 {
		batch.SelectedCount = len(selected)
		batch.NextCursorKey = batch.CursorKey
		logPackagePageBatch(batch)
		return selected, batch
	}
	for _, idx := range packagePageBatchIndexes(outKeys, batch.CursorKey, remaining) {
		record := out[idx].record
		selected = append(selected, record)
		batch.SelectedPackages = append(batch.SelectedPackages, strings.TrimSpace(record["Package"]))
		batch.SelectedItemKeys = append(batch.SelectedItemKeys, out[idx].key)
		batch.NextCursorKey = out[idx].key
	}
	batch.SelectedCount = len(selected)
	logPackagePageBatch(batch)
	return selected, batch
}

func cranPackageRecordBatchKey(record cranRecord) string {
	return strings.ToLower(strings.TrimSpace(record["Package"]))
}

func countUniqueCRANPackageRecords(records []cranRecord) int {
	seen := map[string]bool{}
	for _, record := range records {
		key := cranPackageRecordBatchKey(record)
		if key != "" {
			seen[key] = true
		}
	}
	return len(seen)
}

func packageNameIncludeSet(packageNames []string) map[string]bool {
	include := map[string]bool{}
	for _, name := range packageNames {
		if key := strings.ToLower(strings.TrimSpace(name)); key != "" {
			include[key] = true
		}
	}
	return include
}

func packagePageBatchIndexes(keys []string, cursorKey string, limit int) []int {
	if len(keys) == 0 || limit == 0 {
		return nil
	}
	if limit < 0 || limit > len(keys) {
		limit = len(keys)
	}
	start := 0
	if cursorKey != "" {
		start = sort.Search(len(keys), func(i int) bool {
			return keys[i] > cursorKey
		})
		if start >= len(keys) {
			start = 0
		}
	}
	out := make([]int, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, (start+i)%len(keys))
	}
	return out
}

func packagePageCursorMode() string {
	return strings.ToLower(strings.TrimSpace(envString("RPKG_PACKAGE_PAGE_CURSOR_MODE", "clickhouse")))
}

func packagePageCursorEnabled() bool {
	switch packagePageCursorMode() {
	case "", "off", "none", "disabled", "false", "0":
		return false
	default:
		return true
	}
}

func latestPackagePageBatchCursor(sourceKey string) string {
	if sourceKey == "" || !packagePageCursorEnabled() {
		return ""
	}
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		fmt.Printf("[package-page-batch] cursor_unavailable source_key=%q err=%v\n", sourceKey, err)
		return ""
	}
	query := fmt.Sprintf(`
SELECT JSONExtractString(payload, 'next_cursor_key') AS cursor_key
FROM Data_R_Package_Raw.r_package_event_raw
WHERE event_type = 'rpkg.package_page_batch_cursor.v1'
  AND JSONExtractString(payload, 'batch_source_key') = %s
ORDER BY collected_at DESC
LIMIT 1
FORMAT JSONEachRow`, clickHouseQuoteString(sourceKey))
	rows, err := cfg.queryJSONEachRow(query)
	if err != nil {
		fmt.Printf("[package-page-batch] cursor_unavailable source_key=%q err=%v\n", sourceKey, err)
		return ""
	}
	if len(rows) > 0 {
		if cursor := stringAny(rows[0]["cursor_key"]); cursor != "" {
			return cursor
		}
	}
	return latestPackagePageSnapshotCursor(cfg, sourceKey)
}

func latestPackagePageSnapshotCursor(cfg clickHouseQueryConfig, sourceKey string) string {
	eventType := packagePageCursorFallbackEventType(sourceKey)
	if eventType == "" {
		return ""
	}
	query := fmt.Sprintf(`
SELECT lower(package_name) AS cursor_key
FROM Data_R_Package_Raw.r_package_event_raw
WHERE event_type = %s
  AND package_name != ''
ORDER BY collected_at DESC
LIMIT 1
FORMAT JSONEachRow`, clickHouseQuoteString(eventType))
	rows, err := cfg.queryJSONEachRow(query)
	if err != nil {
		fmt.Printf("[package-page-batch] cursor_bootstrap_unavailable source_key=%q err=%v\n", sourceKey, err)
		return ""
	}
	if len(rows) == 0 {
		return ""
	}
	cursor := stringAny(rows[0]["cursor_key"])
	if cursor != "" {
		fmt.Printf("[package-page-batch] cursor_bootstrap source_key=%q cursor=%q\n", sourceKey, cursor)
	}
	return cursor
}

func packagePageCursorFallbackEventType(sourceKey string) string {
	switch {
	case strings.HasPrefix(sourceKey, "cran-package-pages|"):
		return "rpkg.cran.package_page_snapshot.v1"
	case strings.HasPrefix(sourceKey, "bioconductor-package-pages|"):
		return "rpkg.bioconductor.package_page_snapshot.v1"
	case strings.HasPrefix(sourceKey, "runiverse-package-pages|"):
		return "rpkg.runiverse.package_page_snapshot.v1"
	default:
		return ""
	}
}

func clickHouseQuoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func logPackagePageBatch(batch packagePageBatch) {
	fmt.Printf("[package-page-batch] source_key=%q cursor=%q next_cursor=%q limit=%d selected=%d forced=%d total_candidates=%d\n",
		batch.SourceKey,
		batch.CursorKey,
		batch.NextCursorKey,
		batch.Limit,
		batch.SelectedCount,
		batch.ForcedCount,
		batch.TotalCandidates,
	)
}

func newPackagePageBatchCursorEvent(repository, sourceMethod string, batch packagePageBatch) genericEvent {
	payload := map[string]any{
		"payload_schema":      "r_package_page_batch_cursor_v1",
		"batch_source_key":    batch.SourceKey,
		"cursor_mode":         packagePageCursorMode(),
		"previous_cursor_key": batch.CursorKey,
		"next_cursor_key":     batch.NextCursorKey,
		"limit":               batch.Limit,
		"total_candidates":    batch.TotalCandidates,
		"forced_count":        batch.ForcedCount,
		"selected_count":      batch.SelectedCount,
		"selected_packages":   batch.SelectedPackages,
		"selected_item_keys":  batch.SelectedItemKeys,
	}
	return newGenericEvent("rpkg.package_page_batch_cursor.v1", sourceMethod, "clickhouse://Data_R_Package_Raw/r_package_event_raw", repository, "", "", "", payload)
}

func cranPackagePagePayload(pageURL, htmlText string, record cranRecord) map[string]any {
	fields := cranPackagePageFields(htmlText)
	fieldRows := cranPackagePageFieldRows(pageURL, htmlText)
	title := firstHeading(htmlText)
	description := firstParagraph(htmlText)
	materials := cranLinksForLabel(pageURL, htmlText, "Materials")
	inViews := cranLinksForLabel(pageURL, htmlText, "In views")
	checks := cranLinksForLabel(pageURL, htmlText, "CRAN checks")
	doiLinks := cranLinksForLabel(pageURL, htmlText, "DOI")
	urlLinks := cranLinksForLabel(pageURL, htmlText, "URL")
	bugLinks := cranLinksForLabel(pageURL, htmlText, "BugReports")
	citationLinks := cranLinksForLabel(pageURL, htmlText, "Citation")
	referenceLinks := append(cranLinksForLabel(pageURL, htmlText, "Reference manual"), cranLinksAfterTextLabel(pageURL, htmlText, "Reference manual:", []string{"Vignettes:", "Downloads:", "Reverse dependencies:", "Linking:"})...)
	documentationLinks := cranLinksInSection(pageURL, htmlText, "Documentation:")
	vignetteLinks := cranLinksAfterTextLabel(pageURL, htmlText, "Vignettes:", []string{"Downloads:", "Reverse dependencies:", "Linking:"})
	downloadLinks := cranLinksInSection(pageURL, htmlText, "Downloads:")
	reverseDepends := append(cranLinksForLabel(pageURL, htmlText, "Reverse depends"), cranLinksAfterTextLabel(pageURL, htmlText, "Reverse depends:", []string{"Reverse imports:", "Reverse suggests:", "Reverse LinkingTo:", "Reverse enhances:", "Linking:"})...)
	reverseImports := append(cranLinksForLabel(pageURL, htmlText, "Reverse imports"), cranLinksAfterTextLabel(pageURL, htmlText, "Reverse imports:", []string{"Reverse suggests:", "Reverse LinkingTo:", "Reverse enhances:", "Linking:"})...)
	reverseSuggests := append(cranLinksForLabel(pageURL, htmlText, "Reverse suggests"), cranLinksAfterTextLabel(pageURL, htmlText, "Reverse suggests:", []string{"Reverse LinkingTo:", "Reverse enhances:", "Linking:"})...)
	reverseLinkingTo := append(cranLinksForLabel(pageURL, htmlText, "Reverse LinkingTo"), cranLinksAfterTextLabel(pageURL, htmlText, "Reverse LinkingTo:", []string{"Reverse enhances:", "Linking:"})...)
	reverseEnhances := append(cranLinksForLabel(pageURL, htmlText, "Reverse enhances"), cranLinksAfterTextLabel(pageURL, htmlText, "Reverse enhances:", []string{"Linking:"})...)
	allLinks := cranAllPageLinks(pageURL, htmlText, fieldRows)

	referenceHTML := firstLinkWithSuffix(referenceLinks, ".html")
	referencePDF := firstLinkWithSuffix(referenceLinks, ".pdf")
	sourcePackage := firstLinkMatching(downloadLinks, ".tar.gz")
	archiveURL := firstLinkContaining(downloadLinks, "Archive/")
	doiURL := firstLinkContaining(doiLinks, "doi.org")
	checksURL := firstLinkURL(checks)
	citationURL := firstLinkURL(citationLinks)
	readmeURL := firstLinkLabelContaining(materials, "readme")
	newsURL := firstLinkLabelContaining(materials, "news")

	payload := recordPayload(record)
	payload["fields_json"] = mustJSON(fields)
	payload["field_rows_json"] = mustJSON(fieldRows)
	payload["links_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(allLinks, envInt("RPKG_CRAN_PAGE_LINK_LIMIT", 120))))
	payload["sections_json"] = mustJSON(cranPackagePageSections(pageURL, htmlText))
	payload["package"] = firstNonEmpty(record["Package"], fields["Package"])
	payload["version"] = firstNonEmpty(fields["Version"], record["Version"])
	payload["title"] = firstNonEmpty(title, record["Title"])
	payload["description"] = firstNonEmpty(description, record["Description"])
	payload["depends"] = firstNonEmpty(fields["Depends"], record["Depends"])
	payload["imports"] = firstNonEmpty(fields["Imports"], record["Imports"])
	payload["suggests"] = firstNonEmpty(fields["Suggests"], record["Suggests"])
	payload["linking_to"] = firstNonEmpty(fields["LinkingTo"], record["LinkingTo"])
	payload["enhances"] = firstNonEmpty(fields["Enhances"], record["Enhances"])
	payload["published"] = firstNonEmpty(fields["Published"], record["Date/Publication"])
	payload["doi"] = fields["DOI"]
	payload["doi_url"] = doiURL
	payload["citation_url"] = citationURL
	payload["author"] = firstNonEmpty(fields["Author"], record["Author"])
	payload["maintainer"] = firstNonEmpty(fields["Maintainer"], record["Maintainer"])
	payload["bug_reports"] = firstNonEmpty(fields["BugReports"], record["BugReports"])
	payload["bug_reports_url"] = firstLinkURL(bugLinks)
	payload["license"] = firstNonEmpty(fields["License"], record["License"])
	payload["url"] = firstNonEmpty(fields["URL"], record["URL"])
	payload["urls_json"] = mustJSON(cranLinksToMaps(urlLinks))
	payload["needs_compilation"] = firstNonEmpty(fields["NeedsCompilation"], record["NeedsCompilation"])
	payload["materials_json"] = mustJSON(cranLinksToMaps(materials))
	payload["readme_url"] = readmeURL
	payload["news_url"] = newsURL
	payload["in_views"] = cranLinkLabels(inViews)
	payload["in_views_json"] = mustJSON(cranLinkLabels(inViews))
	payload["cran_checks_url"] = checksURL
	payload["reference_manual_html_url"] = referenceHTML
	payload["reference_manual_pdf_url"] = referencePDF
	payload["documentation_json"] = mustJSON(cranLinksToMaps(documentationLinks))
	payload["vignettes_json"] = mustJSON(cranLinksToMaps(vignetteLinks))
	payload["downloads_json"] = mustJSON(cranLinksToMaps(downloadLinks))
	payload["package_source_url"] = sourcePackage
	payload["archive_url"] = archiveURL
	payload["reverse_depends_count"] = len(reverseDepends)
	payload["reverse_imports_count"] = len(reverseImports)
	payload["reverse_suggests_count"] = len(reverseSuggests)
	payload["reverse_linking_to_count"] = len(reverseLinkingTo)
	payload["reverse_enhances_count"] = len(reverseEnhances)
	reverseLinkLimit := envInt("RPKG_CRAN_PAGE_REVERSE_LINK_LIMIT", 20)
	payload["reverse_depends_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(reverseDepends, reverseLinkLimit)))
	payload["reverse_imports_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(reverseImports, reverseLinkLimit)))
	payload["reverse_suggests_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(reverseSuggests, reverseLinkLimit)))
	payload["reverse_linking_to_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(reverseLinkingTo, reverseLinkLimit)))
	payload["reverse_enhances_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(reverseEnhances, reverseLinkLimit)))
	payload["all_links_count"] = len(allLinks)
	payload["page_url"] = pageURL
	payload["source_method"] = "cran_package_index_html"
	payload["parser_version"] = 1
	payload["collection_status"] = "collected"
	return payload
}

func cranPackagePageFields(htmlText string) map[string]string {
	out := map[string]string{}
	for _, match := range trRE.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 2 {
			continue
		}
		cells := htmlCells(match[1])
		if len(cells) < 2 {
			continue
		}
		key := normalizeCRANLabel(cells[0])
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(cells[1])
	}
	return out
}

func cranPackagePageFieldRows(baseURL, htmlText string) []map[string]any {
	out := make([]map[string]any, 0)
	valueLimit := envInt("RPKG_CRAN_PAGE_FIELD_VALUE_LIMIT", 2000)
	linkLimit := envInt("RPKG_CRAN_PAGE_FIELD_LINK_LIMIT", 20)
	for _, match := range trRE.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 2 {
			continue
		}
		cells := htmlCells(match[1])
		if len(cells) < 2 {
			continue
		}
		key := normalizeCRANLabel(cells[0])
		if key == "" {
			continue
		}
		out = append(out, map[string]any{
			"key":   key,
			"value": truncate(strings.TrimSpace(cells[1]), valueLimit),
			"links": cranLinksToMaps(limitedCRANLinks(withLinkSection(cranLinksInHTML(baseURL, match[1]), key, "field"), linkLimit)),
		})
	}
	return out
}

func cranLinksForLabel(baseURL, htmlText string, labels ...string) []cranPageLink {
	wanted := map[string]bool{}
	for _, label := range labels {
		wanted[normalizeCRANLabel(label)] = true
	}
	out := make([]cranPageLink, 0)
	for _, match := range trRE.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 2 {
			continue
		}
		cells := htmlCells(match[1])
		if len(cells) == 0 || !wanted[normalizeCRANLabel(cells[0])] {
			continue
		}
		out = append(out, withLinkSection(cranLinksInHTML(baseURL, match[1]), normalizeCRANLabel(cells[0]), "field")...)
	}
	return uniqueCRANLinks(out)
}

func cranLinksInSection(baseURL, htmlText, heading string) []cranPageLink {
	section := htmlSectionAfter(htmlText, heading)
	if section == "" {
		return nil
	}
	return uniqueCRANLinks(withLinkSection(cranLinksInHTML(baseURL, section), strings.TrimSuffix(heading, ":"), "section"))
}

func cranLinksAfterTextLabel(baseURL, htmlText, label string, stopLabels []string) []cranPageLink {
	normalizedHTML := normalizeCRANHTMLText(htmlText)
	lower := strings.ToLower(normalizedHTML)
	lowerLabel := strings.ToLower(label)
	idx := strings.Index(lower, lowerLabel)
	if idx < 0 {
		return nil
	}
	start := idx + len(label)
	end := len(normalizedHTML)
	for _, stop := range stopLabels {
		next := strings.Index(lower[start:], strings.ToLower(stop))
		if next >= 0 && start+next < end {
			end = start + next
		}
	}
	for _, marker := range []string{"<h4", "<h3", "<h2"} {
		next := strings.Index(lower[start:], marker)
		if next >= 0 && start+next < end {
			end = start + next
		}
	}
	section := strings.TrimSuffix(label, ":")
	return uniqueCRANLinks(withLinkSection(cranLinksInHTML(baseURL, normalizedHTML[start:end]), section, "section"))
}

func htmlSectionAfter(htmlText, heading string) string {
	htmlText = normalizeCRANHTMLText(htmlText)
	lower := strings.ToLower(htmlText)
	idx := strings.Index(lower, strings.ToLower(heading))
	if idx < 0 {
		return ""
	}
	start := idx + len(heading)
	end := len(htmlText)
	for _, marker := range []string{"<h4", "<h3", "<h2"} {
		next := strings.Index(strings.ToLower(htmlText[start:]), marker)
		if next >= 0 && start+next < end {
			end = start + next
		}
	}
	return htmlText[start:end]
}

func normalizeCRANHTMLText(value string) string {
	value = strings.ReplaceAll(value, "&nbsp;", " ")
	value = strings.ReplaceAll(value, "&#160;", " ")
	value = strings.ReplaceAll(value, "\u00a0", " ")
	return value
}

func cranLinksInHTML(baseURL, htmlText string) []cranPageLink {
	out := make([]cranPageLink, 0)
	for _, match := range linkRE.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 3 {
			continue
		}
		label := stripTags(match[2])
		href := strings.TrimSpace(html.UnescapeString(match[1]))
		if href == "" {
			continue
		}
		out = append(out, cranPageLink{Label: label, URL: absoluteURL(baseURL, href)})
	}
	return out
}

func withLinkSection(rows []cranPageLink, section, linkType string) []cranPageLink {
	section = normalizeCRANLabel(section)
	for i := range rows {
		rows[i].Section = section
		rows[i].Type = linkType
	}
	return rows
}

func cranAllPageLinks(baseURL, htmlText string, fieldRows []map[string]any) []cranPageLink {
	out := make([]cranPageLink, 0)
	for _, row := range fieldRows {
		section := fmt.Sprint(row["key"])
		for _, link := range linksFromJSONValue(row["links"]) {
			link.Section = section
			link.Type = firstNonEmpty(link.Type, "field")
			out = append(out, link)
		}
	}
	for _, section := range []string{"Documentation:", "Downloads:", "Reverse dependencies:", "Linking:"} {
		out = append(out, cranLinksInSection(baseURL, htmlText, section)...)
	}
	return uniqueCRANLinks(out)
}

func cranPackagePageSections(baseURL, htmlText string) []map[string]any {
	out := make([]map[string]any, 0)
	textLimit := envInt("RPKG_CRAN_PAGE_SECTION_TEXT_LIMIT", 2000)
	linkLimit := envInt("RPKG_CRAN_PAGE_SECTION_LINK_LIMIT", 20)
	for _, heading := range []string{"Documentation:", "Downloads:", "Reverse dependencies:", "Linking:"} {
		sectionHTML := htmlSectionAfter(htmlText, heading)
		if sectionHTML == "" {
			continue
		}
		out = append(out, map[string]any{
			"heading": strings.TrimSuffix(heading, ":"),
			"text":    truncate(stripTags(sectionHTML), textLimit),
			"links":   cranLinksToMaps(limitedCRANLinks(cranLinksInHTML(baseURL, sectionHTML), linkLimit)),
		})
	}
	return out
}

func linksFromJSONValue(value any) []cranPageLink {
	rows, ok := value.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]cranPageLink, 0, len(rows))
	for _, row := range rows {
		out = append(out, cranPageLink{
			Label:   row["label"],
			URL:     row["url"],
			Section: row["section"],
			Type:    row["type"],
		})
	}
	return out
}

func uniqueCRANLinks(rows []cranPageLink) []cranPageLink {
	out := make([]cranPageLink, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		key := strings.TrimSpace(row.URL)
		if key == "" {
			key = strings.TrimSpace(row.Label)
		}
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, row)
	}
	return out
}

func limitedCRANLinks(rows []cranPageLink, limit int) []cranPageLink {
	if limit <= 0 || len(rows) <= limit {
		return rows
	}
	return rows[:limit]
}

func cranLinksToMaps(rows []cranPageLink) []map[string]string {
	out := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]string{
			"label":   truncate(row.Label, 240),
			"url":     truncate(row.URL, 500),
			"section": truncate(row.Section, 120),
			"type":    truncate(row.Type, 80),
		})
	}
	return out
}

func cranLinkLabels(rows []cranPageLink) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if label := strings.TrimSpace(row.Label); label != "" {
			out = append(out, label)
		}
	}
	return uniqueStrings(out)
}

func normalizeCRANLabel(value string) string {
	value = strings.ReplaceAll(value, "\u00a0", " ")
	value = strings.TrimSpace(strings.TrimSuffix(value, ":"))
	value = spaceRE.ReplaceAllString(value, " ")
	return value
}

func firstHeading(htmlText string) string {
	matches := headingRE.FindAllStringSubmatch(htmlText, 1)
	if len(matches) == 0 || len(matches[0]) < 2 {
		return ""
	}
	return stripTags(matches[0][1])
}

func firstParagraph(htmlText string) string {
	matches := paragraphRE.FindAllStringSubmatch(htmlText, 1)
	if len(matches) == 0 || len(matches[0]) < 2 {
		return ""
	}
	return stripTags(matches[0][1])
}

func firstLinkURL(rows []cranPageLink) string {
	for _, row := range rows {
		if strings.TrimSpace(row.URL) != "" {
			return row.URL
		}
	}
	return ""
}

func firstLinkWithSuffix(rows []cranPageLink, suffix string) string {
	suffix = strings.ToLower(suffix)
	for _, row := range rows {
		if strings.HasSuffix(strings.ToLower(row.URL), suffix) {
			return row.URL
		}
	}
	return ""
}

func firstLinkContaining(rows []cranPageLink, needle string) string {
	needle = strings.ToLower(needle)
	for _, row := range rows {
		if strings.Contains(strings.ToLower(row.URL), needle) {
			return row.URL
		}
	}
	return ""
}

func firstLinkMatching(rows []cranPageLink, needle string) string {
	needle = strings.ToLower(needle)
	for _, row := range rows {
		if strings.Contains(strings.ToLower(row.URL), needle) || strings.Contains(strings.ToLower(row.Label), needle) {
			return row.URL
		}
	}
	return ""
}

func firstLinkLabelContaining(rows []cranPageLink, needle string) string {
	needle = strings.ToLower(needle)
	for _, row := range rows {
		if strings.Contains(strings.ToLower(row.Label), needle) && strings.TrimSpace(row.URL) != "" {
			return row.URL
		}
	}
	return ""
}

func cranPackageArtifactLinks(payload map[string]any) []cranPageLink {
	out := make([]cranPageLink, 0)
	for _, raw := range []string{
		stringAny(payload["materials_json"]),
		stringAny(payload["documentation_json"]),
	} {
		var rows []map[string]string
		if err := json.Unmarshal([]byte(raw), &rows); err != nil {
			continue
		}
		for _, row := range rows {
			link := cranPageLink{
				Label:   row["label"],
				URL:     row["url"],
				Section: row["section"],
				Type:    row["type"],
			}
			if isCRANPackageArtifact(link) {
				link.Type = firstNonEmpty(link.Type, cranArtifactType(link))
				out = append(out, link)
			}
		}
	}
	for _, item := range []cranPageLink{
		{Label: "Citation", URL: stringAny(payload["citation_url"]), Section: "Citation", Type: "citation"},
		{Label: "Reference manual PDF", URL: stringAny(payload["reference_manual_pdf_url"]), Section: "Documentation", Type: "reference_manual_pdf"},
		{Label: "Reference manual HTML", URL: stringAny(payload["reference_manual_html_url"]), Section: "Documentation", Type: "reference_manual_html"},
	} {
		if strings.TrimSpace(item.URL) != "" {
			out = append(out, item)
		}
	}
	return uniqueCRANLinks(out)
}

func isCRANPackageArtifact(link cranPageLink) bool {
	label := strings.ToLower(link.Label)
	rawURL := strings.ToLower(link.URL)
	return strings.Contains(label, "readme") ||
		strings.Contains(label, "news") ||
		strings.Contains(label, "citation") ||
		strings.Contains(label, "vignette") ||
		strings.Contains(rawURL, "/doc/") ||
		strings.Contains(rawURL, "/readme/") ||
		strings.Contains(rawURL, "/news/") ||
		strings.HasSuffix(rawURL, ".pdf")
}

func cranArtifactType(link cranPageLink) string {
	label := strings.ToLower(link.Label)
	rawURL := strings.ToLower(link.URL)
	switch {
	case strings.Contains(label, "readme") || strings.Contains(rawURL, "/readme/"):
		return "readme"
	case strings.Contains(label, "news") || strings.Contains(rawURL, "/news/"):
		return "news"
	case strings.Contains(label, "citation"):
		return "citation"
	case strings.Contains(label, "reference") && strings.HasSuffix(rawURL, ".pdf"):
		return "reference_manual_pdf"
	case strings.Contains(label, "reference"):
		return "reference_manual"
	case strings.Contains(rawURL, "/doc/"):
		return "vignette"
	case strings.HasSuffix(rawURL, ".pdf"):
		return "pdf"
	}
	return "artifact"
}

func collectCRANPackageArtifacts(record cranRecord, pageURL string, links []cranPageLink, limit int) []genericEvent {
	return collectPackageArtifacts(record, "CRAN", "cran_package_artifact", "rpkg.cran.package_artifact_snapshot.v1", "rpkg.cran.package_artifact.failure.v1", pageURL, links, limit)
}

func collectPackageArtifacts(record cranRecord, repository, sourceMethod, eventType, failureEventType, pageURL string, links []cranPageLink, limit int) []genericEvent {
	if len(links) == 0 || limit < 0 {
		return nil
	}
	if limit > 0 && limit < len(links) {
		links = links[:limit]
	}
	packageName := strings.TrimSpace(record["Package"])
	events := make([]genericEvent, 0, len(links))
	for _, link := range links {
		if strings.TrimSpace(link.URL) == "" {
			continue
		}
		body, contentType, err := fetchBytesWithContentType(link.URL)
		if err != nil {
			events = append(events, collectionFailureEvent(failureEventType, sourceMethod, link.URL, repository, packageName, err))
			continue
		}
		textContent, htmlContent := artifactContent(body, contentType, link.URL)
		payload := map[string]any{
			"package":           packageName,
			"version":           record["Version"],
			"page_url":          pageURL,
			"artifact_label":    link.Label,
			"artifact_url":      link.URL,
			"artifact_section":  link.Section,
			"artifact_type":     firstNonEmpty(link.Type, cranArtifactType(link)),
			"content_type":      contentType,
			"content_length":    len(body),
			"content_sha256":    shaHex(string(body)),
			"title":             firstNonEmpty(firstTitle(string(body)), link.Label),
			"text_content":      truncate(textContent, envInt("RPKG_CRAN_ARTIFACT_TEXT_LIMIT", 8000)),
			"html_content":      truncate(htmlContent, envInt("RPKG_CRAN_ARTIFACT_HTML_LIMIT", 8000)),
			"source_method":     sourceMethod,
			"parser_version":    1,
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent(eventType, sourceMethod, link.URL, repository, packageName, record["Version"], "", payload))
	}
	return events
}

func artifactContent(body []byte, contentType, artifactURL string) (string, string) {
	lowerType := strings.ToLower(contentType)
	lowerURL := strings.ToLower(artifactURL)
	if strings.Contains(lowerType, "pdf") || strings.HasSuffix(lowerURL, ".pdf") {
		return "", ""
	}
	raw := string(body)
	if strings.Contains(lowerType, "html") || strings.Contains(raw, "<html") {
		return stripTags(raw), raw
	}
	return raw, ""
}

func collectCRANPackageManualTopics(record cranRecord, pageURL, sourcePackageURL string, limit int) []genericEvent {
	return collectPackageManualTopics(record, "CRAN", "cran_source_rd_manual", "rpkg.cran.package_manual_topic_snapshot.v1", "rpkg.cran.package_manual.failure.v1", pageURL, sourcePackageURL, limit)
}

func collectPackageManualTopics(record cranRecord, repository, sourceMethod, eventType, failureEventType, pageURL, sourcePackageURL string, limit int) []genericEvent {
	sourcePackageURL = strings.TrimSpace(sourcePackageURL)
	packageName := strings.TrimSpace(record["Package"])
	if limit < 0 || sourcePackageURL == "" || packageName == "" {
		return nil
	}
	body, err := fetchBytes(sourcePackageURL)
	if err != nil {
		return []genericEvent{collectionFailureEvent(failureEventType, sourceMethod, sourcePackageURL, repository, packageName, err)}
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return []genericEvent{collectionFailureEvent(failureEventType, sourceMethod, sourcePackageURL, repository, packageName, err)}
	}
	defer reader.Close()

	tarReader := tar.NewReader(reader)
	events := make([]genericEvent, 0)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			events = append(events, collectionFailureEvent(failureEventType, sourceMethod, sourcePackageURL, repository, packageName, err))
			break
		}
		if header == nil || header.FileInfo().IsDir() || !strings.HasSuffix(header.Name, ".Rd") || !strings.Contains(header.Name, "/man/") {
			continue
		}
		if limit > 0 && len(events) >= limit {
			break
		}
		rdBody, err := io.ReadAll(io.LimitReader(tarReader, int64(envInt("RPKG_CRAN_RD_MAX_BYTES", 2*1024*1024))))
		if err != nil {
			events = append(events, collectionFailureEvent(failureEventType, sourceMethod, sourcePackageURL, repository, packageName, err))
			continue
		}
		payload := cranManualTopicPayload(record, pageURL, sourcePackageURL, header.Name, string(rdBody))
		payload["source_method"] = sourceMethod
		events = append(events, newGenericEvent(eventType, sourceMethod, sourcePackageURL, repository, packageName, record["Version"], "", payload))
	}
	return events
}

func cranManualTopicPayload(record cranRecord, pageURL, sourcePackageURL, rdPath, rdText string) map[string]any {
	aliases := rdSectionValues(rdText, "alias")
	keywords := rdSectionValues(rdText, "keyword")
	concepts := rdSectionValues(rdText, "concept")
	topicName := firstNonEmpty(rdFirstSection(rdText, "name"), firstString(aliases), rdBaseName(rdPath))
	fieldLimit := envInt("RPKG_CRAN_RD_FIELD_LIMIT", 12000)
	argumentLimit := envInt("RPKG_CRAN_RD_ARGUMENT_LIMIT", 80)
	argumentTextLimit := envInt("RPKG_CRAN_RD_ARGUMENT_TEXT_LIMIT", 2000)
	customSectionLimit := envInt("RPKG_CRAN_RD_CUSTOM_SECTION_LIMIT", 20)
	payload := map[string]any{
		"package":              record["Package"],
		"version":              record["Version"],
		"page_url":             pageURL,
		"source_package_url":   sourcePackageURL,
		"rd_path":              rdPath,
		"topic_name":           rdPlainText(topicName),
		"title":                rdPlainText(rdFirstSection(rdText, "title")),
		"description":          truncate(rdPlainText(rdFirstSection(rdText, "description")), fieldLimit),
		"usage":                truncate(rdPlainText(rdFirstSection(rdText, "usage")), fieldLimit),
		"arguments_json":       mustJSON(limitRDMapRows(rdArguments(rdFirstSection(rdText, "arguments")), argumentLimit, argumentTextLimit)),
		"details":              truncate(rdPlainText(rdFirstSection(rdText, "details")), fieldLimit),
		"value":                truncate(rdPlainText(rdFirstSection(rdText, "value")), fieldLimit),
		"format_text":          truncate(rdPlainText(rdFirstSection(rdText, "format")), fieldLimit),
		"source_text":          truncate(rdPlainText(rdFirstSection(rdText, "source")), fieldLimit),
		"examples":             truncate(rdPlainText(rdFirstSection(rdText, "examples")), fieldLimit),
		"seealso":              truncate(rdPlainText(rdFirstSection(rdText, "seealso")), fieldLimit),
		"keywords_json":        mustJSON(rdPlainTextList(keywords)),
		"aliases_json":         mustJSON(rdPlainTextList(aliases)),
		"concepts_json":        mustJSON(rdPlainTextList(concepts)),
		"doc_type":             rdPlainText(rdFirstSection(rdText, "docType")),
		"encoding":             rdPlainText(rdFirstSection(rdText, "encoding")),
		"custom_sections_json": mustJSON(limitRDMapRows(rdCustomSections(rdText), customSectionLimit, fieldLimit)),
		"note":                 truncate(rdPlainText(rdFirstSection(rdText, "note")), fieldLimit),
		"author":               truncate(rdPlainText(rdFirstSection(rdText, "author")), fieldLimit),
		"references_text":      truncate(rdPlainText(rdFirstSection(rdText, "references")), fieldLimit),
		"raw_rd":               truncate(rdText, envInt("RPKG_CRAN_RD_RAW_LIMIT", 12000)),
		"source_method":        "cran_source_rd_manual",
		"parser_version":       1,
		"collection_status":    "collected",
	}
	return payload
}

func limitRDMapRows(rows []map[string]string, rowLimit, valueLimit int) []map[string]string {
	if rowLimit > 0 && len(rows) > rowLimit {
		rows = rows[:rowLimit]
	}
	out := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		next := make(map[string]string, len(row))
		for key, value := range row {
			next[key] = truncate(value, valueLimit)
		}
		out = append(out, next)
	}
	return out
}

func rdBaseName(rdPath string) string {
	base := rdPath
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	return strings.TrimSuffix(base, ".Rd")
}

func rdFirstSection(rdText, name string) string {
	values := rdSectionValues(rdText, name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func rdSectionValues(rdText, name string) []string {
	marker := `\` + name
	values := []string{}
	offset := 0
	for {
		idx := strings.Index(rdText[offset:], marker)
		if idx < 0 {
			break
		}
		start := offset + idx + len(marker)
		for start < len(rdText) && (rdText[start] == ' ' || rdText[start] == '\n' || rdText[start] == '\r' || rdText[start] == '\t') {
			start++
		}
		if start >= len(rdText) || rdText[start] != '{' {
			offset = start
			continue
		}
		body, end := rdBraceBody(rdText, start)
		if end <= start {
			offset = start + 1
			continue
		}
		values = append(values, body)
		offset = end
	}
	return values
}

func rdBraceBody(value string, open int) (string, int) {
	if open < 0 || open >= len(value) || value[open] != '{' {
		return "", open
	}
	depth := 0
	start := open + 1
	for i := open; i < len(value); i++ {
		if value[i] == '\\' {
			i++
			continue
		}
		switch value[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return value[start:i], i + 1
			}
		}
	}
	return "", open
}

func rdCustomSections(rdText string) []map[string]string {
	out := []map[string]string{}
	offset := 0
	for {
		idx := strings.Index(rdText[offset:], `\section`)
		if idx < 0 {
			break
		}
		start := offset + idx + len(`\section`)
		for start < len(rdText) && isSpaceByte(rdText[start]) {
			start++
		}
		if start >= len(rdText) || rdText[start] != '{' {
			offset = start
			continue
		}
		title, end := rdBraceBody(rdText, start)
		for end < len(rdText) && isSpaceByte(rdText[end]) {
			end++
		}
		if end >= len(rdText) || rdText[end] != '{' {
			offset = end
			continue
		}
		body, next := rdBraceBody(rdText, end)
		out = append(out, map[string]string{"title": rdPlainText(title), "text": rdPlainText(body)})
		offset = next
	}
	return out
}

func rdArguments(value string) []map[string]string {
	out := []map[string]string{}
	offset := 0
	for {
		idx := strings.Index(value[offset:], `\item`)
		if idx < 0 {
			break
		}
		start := offset + idx + len(`\item`)
		for start < len(value) && isSpaceByte(value[start]) {
			start++
		}
		if start >= len(value) || value[start] != '{' {
			offset = start
			continue
		}
		name, end := rdBraceBody(value, start)
		for end < len(value) && isSpaceByte(value[end]) {
			end++
		}
		if end >= len(value) || value[end] != '{' {
			offset = end
			continue
		}
		desc, next := rdBraceBody(value, end)
		out = append(out, map[string]string{"name": rdPlainText(name), "description": rdPlainText(desc)})
		offset = next
	}
	return out
}

func rdPlainTextList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if text := rdPlainText(value); text != "" {
			out = append(out, text)
		}
	}
	return uniqueStrings(out)
}

func rdPlainText(value string) string {
	value = strings.ReplaceAll(value, "\\%", "%")
	value = strings.ReplaceAll(value, "\\_", "_")
	value = strings.ReplaceAll(value, "\\&", "&")
	value = rdCommandRE.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "{", "")
	value = strings.ReplaceAll(value, "}", "")
	return strings.TrimSpace(spaceRE.ReplaceAllString(value, " "))
}

func isSpaceByte(value byte) bool {
	return value == ' ' || value == '\n' || value == '\r' || value == '\t'
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func collectBioconductor() ([]genericEvent, error) {
	branches := splitCSV(envString("RPKG_BIOCONDUCTOR_BRANCHES", "release,devel"))
	repos := splitCSV(envString("RPKG_BIOCONDUCTOR_REPOS", "bioc,data/annotation,data/experiment,workflows"))
	events := make([]genericEvent, 0)
	for _, branch := range branches {
		for _, repo := range repos {
			sourceURL := fmt.Sprintf("https://bioconductor.org/packages/%s/%s/src/contrib/PACKAGES.gz", branch, repo)
			records, err := fetchDCF(sourceURL)
			if err != nil {
				events = append(events, collectionFailureEvent("rpkg.bioconductor.collection.failure.v1", "bioconductor_packages_gz", sourceURL, "Bioconductor", "", err))
				continue
			}
			for _, record := range records {
				packageName := record["Package"]
				if packageName == "" {
					continue
				}
				payload := recordPayload(record)
				payload["branch"] = branch
				payload["bioc_repository"] = repo
				payload["source_method"] = "bioconductor_packages_gz"
				payload["collection_status"] = "collected"
				events = append(events, newGenericEvent("rpkg.bioconductor.package_snapshot.v1", "bioconductor_packages_gz", sourceURL, "Bioconductor", packageName, record["Version"], "", payload))
			}
		}
	}
	return events, nil
}

func collectRUniverse() ([]genericEvent, error) {
	universes := splitCSV(envString("RPKG_RUNIVERSE_UNIVERSES", "ropensci,tidyverse"))
	events := make([]genericEvent, 0)
	for _, universe := range universes {
		sourceURL := fmt.Sprintf("https://%s.r-universe.dev/src/contrib/PACKAGES", universe)
		records, err := fetchDCF(sourceURL)
		if err != nil {
			events = append(events, collectionFailureEvent("rpkg.runiverse.collection.failure.v1", "runiverse_packages_file", sourceURL, "R-universe", "", err))
			continue
		}
		for _, record := range records {
			packageName := record["Package"]
			if packageName == "" {
				continue
			}
			payload := recordPayload(record)
			payload["universe"] = universe
			payload["source_method"] = "runiverse_packages_file_no_api"
			payload["collection_status"] = "collected"
			events = append(events, newGenericEvent("rpkg.runiverse.package_snapshot.v1", "runiverse_packages_file", sourceURL, "R-universe", packageName, record["Version"], "", payload))
		}
	}
	return events, nil
}

type repositoryPackageRecord struct {
	record            cranRecord
	repository        string
	sourceURL         string
	pageURL           string
	sourcePackageURL  string
	artifactLinks     []cranPageLink
	pageMethod        string
	artifactMethod    string
	manualMethod      string
	pageEventType     string
	artifactEventType string
	manualEventType   string
	failurePrefix     string
	extra             map[string]string
}

func collectBioconductorPackagePages(limit int, packageNames []string, artifactLimit, manualTopicLimit int) ([]genericEvent, error) {
	branches := splitCSV(envString("RPKG_BIOCONDUCTOR_PACKAGE_PAGE_BRANCHES", envString("RPKG_BIOCONDUCTOR_BRANCHES", "release")))
	repos := splitCSV(envString("RPKG_BIOCONDUCTOR_PACKAGE_PAGE_REPOS", envString("RPKG_BIOCONDUCTOR_REPOS", "bioc,data/annotation,data/experiment,workflows")))
	candidates := make([]repositoryPackageRecord, 0)
	for _, branch := range branches {
		for _, repo := range repos {
			sourceURL := fmt.Sprintf("https://bioconductor.org/packages/%s/%s/src/contrib/PACKAGES.gz", branch, repo)
			records, err := fetchDCF(sourceURL)
			if err != nil {
				candidates = append(candidates, repositoryPackageRecord{
					repository:    "Bioconductor",
					sourceURL:     sourceURL,
					pageMethod:    "bioconductor_package_html",
					failurePrefix: "rpkg.bioconductor.package_page",
					extra:         map[string]string{"branch": branch, "bioc_repository": repo, "fetch_error": err.Error()},
				})
				continue
			}
			for _, record := range records {
				packageName := strings.TrimSpace(record["Package"])
				version := strings.TrimSpace(record["Version"])
				if packageName == "" || version == "" {
					continue
				}
				pageURL := fmt.Sprintf("https://bioconductor.org/packages/%s/%s/html/%s.html", branch, repo, url.PathEscape(packageName))
				sourcePackageURL := fmt.Sprintf("https://bioconductor.org/packages/%s/%s/src/contrib/%s_%s.tar.gz", branch, repo, url.PathEscape(packageName), url.PathEscape(version))
				candidates = append(candidates, repositoryPackageRecord{
					record:            record,
					repository:        "Bioconductor",
					sourceURL:         sourceURL,
					pageURL:           pageURL,
					sourcePackageURL:  sourcePackageURL,
					pageMethod:        "bioconductor_package_html",
					artifactMethod:    "bioconductor_package_artifact",
					manualMethod:      "bioconductor_source_rd_manual",
					pageEventType:     "rpkg.bioconductor.package_page_snapshot.v1",
					artifactEventType: "rpkg.bioconductor.package_artifact_snapshot.v1",
					manualEventType:   "rpkg.bioconductor.package_manual_topic_snapshot.v1",
					failurePrefix:     "rpkg.bioconductor.package_page",
					extra:             map[string]string{"branch": branch, "bioc_repository": repo},
				})
			}
		}
	}
	sourceKey := fmt.Sprintf("bioconductor-package-pages|branches=%s|repos=%s", strings.Join(branches, ","), strings.Join(repos, ","))
	return collectRepositoryPackagePages(candidates, limit, packageNames, artifactLimit, manualTopicLimit, sourceKey, "bioconductor_package_page_batch_cursor")
}

func collectRUniversePackagePages(limit int, packageNames []string, artifactLimit, manualTopicLimit int) ([]genericEvent, error) {
	universes := splitCSV(envString("RPKG_RUNIVERSE_UNIVERSES", "ropensci,tidyverse"))
	candidates := make([]repositoryPackageRecord, 0)
	for _, universe := range universes {
		sourceURL := fmt.Sprintf("https://%s.r-universe.dev/src/contrib/PACKAGES", universe)
		records, err := fetchDCF(sourceURL)
		if err != nil {
			candidates = append(candidates, repositoryPackageRecord{
				repository:    "R-universe",
				sourceURL:     sourceURL,
				pageMethod:    "runiverse_package_html",
				failurePrefix: "rpkg.runiverse.package_page",
				extra:         map[string]string{"universe": universe, "fetch_error": err.Error()},
			})
			continue
		}
		for _, record := range records {
			packageName := strings.TrimSpace(record["Package"])
			version := strings.TrimSpace(record["Version"])
			if packageName == "" || version == "" {
				continue
			}
			baseURL := fmt.Sprintf("https://%s.r-universe.dev", universe)
			pageURL := fmt.Sprintf("%s/%s", baseURL, url.PathEscape(packageName))
			sourcePackageURL := fmt.Sprintf("%s/src/contrib/%s_%s.tar.gz", baseURL, url.PathEscape(packageName), url.PathEscape(version))
			candidates = append(candidates, repositoryPackageRecord{
				record:            record,
				repository:        "R-universe",
				sourceURL:         sourceURL,
				pageURL:           pageURL,
				sourcePackageURL:  sourcePackageURL,
				pageMethod:        "runiverse_package_html",
				artifactMethod:    "runiverse_package_artifact",
				manualMethod:      "runiverse_source_rd_manual",
				pageEventType:     "rpkg.runiverse.package_page_snapshot.v1",
				artifactEventType: "rpkg.runiverse.package_artifact_snapshot.v1",
				manualEventType:   "rpkg.runiverse.package_manual_topic_snapshot.v1",
				failurePrefix:     "rpkg.runiverse.package_page",
				extra:             map[string]string{"universe": universe, "api_url": fmt.Sprintf("%s/api/packages/%s", baseURL, url.PathEscape(packageName))},
			})
		}
	}
	sourceKey := fmt.Sprintf("runiverse-package-pages|universes=%s", strings.Join(universes, ","))
	return collectRepositoryPackagePages(candidates, limit, packageNames, artifactLimit, manualTopicLimit, sourceKey, "runiverse_package_page_batch_cursor")
}

func collectRepositoryPackagePages(candidates []repositoryPackageRecord, limit int, packageNames []string, artifactLimit, manualTopicLimit int, sourceKey, cursorMethod string) ([]genericEvent, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	events := make([]genericEvent, 0)
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.record["Package"]) == "" && stringAny(candidate.extra["fetch_error"]) != "" {
			events = append(events, collectionFailureEvent(candidate.failurePrefix+".failure.v1", candidate.pageMethod, candidate.sourceURL, candidate.repository, "", errors.New(candidate.extra["fetch_error"])))
		}
	}
	selected, batch := selectRepositoryPackageRecords(candidates, limit, packageNames, sourceKey)
	repository := firstRepositoryPackageRecordRepository(candidates)
	for _, candidate := range selected {
		packageName := strings.TrimSpace(candidate.record["Package"])
		if packageName == "" {
			continue
		}
		body, err := fetchBytes(candidate.pageURL)
		if err != nil {
			events = append(events, collectionFailureEvent(candidate.failurePrefix+".failure.v1", candidate.pageMethod, candidate.pageURL, candidate.repository, packageName, err))
			continue
		}
		htmlText := string(body)
		payload := repositoryPackagePagePayload(candidate, htmlText)
		payload["content_length"] = len(body)
		payload["html_sha256"] = shaHex(htmlText)
		events = append(events, newGenericEvent(candidate.pageEventType, candidate.pageMethod, candidate.pageURL, candidate.repository, packageName, candidate.record["Version"], "", payload))
		events = append(events, collectPackageArtifacts(candidate.record, candidate.repository, candidate.artifactMethod, candidate.artifactEventType, candidate.failurePrefix+".artifact_failure.v1", candidate.pageURL, packageDocumentArtifactLinks(payload), artifactLimit)...)
		events = append(events, collectPackageManualTopics(candidate.record, candidate.repository, candidate.manualMethod, candidate.manualEventType, candidate.failurePrefix+".manual_failure.v1", candidate.pageURL, stringAny(payload["package_source_url"]), manualTopicLimit)...)
	}
	if batch.SelectedCount > 0 {
		events = append(events, newPackagePageBatchCursorEvent(repository, cursorMethod, batch))
	}
	return events, nil
}

func selectRepositoryPackageRecords(records []repositoryPackageRecord, limit int, packageNames []string, sourceKey string) ([]repositoryPackageRecord, packagePageBatch) {
	batch := packagePageBatch{SourceKey: sourceKey, Limit: limit}
	include := packageNameIncludeSet(packageNames)
	selected := make([]repositoryPackageRecord, 0)
	selectedKeys := map[string]bool{}
	for _, record := range records {
		key := repositoryPackageRecordBatchKey(record)
		if key == "" || !include[key] || selectedKeys[key] {
			continue
		}
		selected = append(selected, record)
		selectedKeys[key] = true
		batch.SelectedPackages = append(batch.SelectedPackages, strings.TrimSpace(record.record["Package"]))
		batch.SelectedItemKeys = append(batch.SelectedItemKeys, key)
	}
	batch.ForcedCount = len(selected)
	if limit > 0 && len(selected) >= limit {
		batch.SelectedCount = len(selected)
		batch.TotalCandidates = countUniqueRepositoryPackageRecords(records)
		batch.CursorKey = latestPackagePageBatchCursor(sourceKey)
		batch.NextCursorKey = batch.CursorKey
		logPackagePageBatch(batch)
		return selected, batch
	}
	type repositoryBatchCandidate struct {
		key    string
		record repositoryPackageRecord
	}
	out := make([]repositoryBatchCandidate, 0, len(records))
	outSeen := map[string]bool{}
	for _, record := range records {
		key := repositoryPackageRecordBatchKey(record)
		if key == "" || selectedKeys[key] || outSeen[key] {
			continue
		}
		out = append(out, repositoryBatchCandidate{key: key, record: record})
		outSeen[key] = true
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].key < out[j].key
	})
	batch.TotalCandidates = len(selected) + len(out)
	outKeys := make([]string, 0, len(out))
	for _, candidate := range out {
		outKeys = append(outKeys, candidate.key)
	}
	batch.CursorKey = latestPackagePageBatchCursor(sourceKey)
	if limit <= 0 {
		for _, candidate := range out {
			record := candidate.record
			selected = append(selected, record)
			batch.SelectedPackages = append(batch.SelectedPackages, strings.TrimSpace(record.record["Package"]))
			batch.SelectedItemKeys = append(batch.SelectedItemKeys, candidate.key)
			batch.NextCursorKey = candidate.key
		}
		batch.SelectedCount = len(selected)
		logPackagePageBatch(batch)
		return selected, batch
	}
	remaining := limit - len(selected)
	if remaining <= 0 || len(out) == 0 {
		batch.SelectedCount = len(selected)
		batch.NextCursorKey = batch.CursorKey
		logPackagePageBatch(batch)
		return selected, batch
	}
	for _, idx := range packagePageBatchIndexes(outKeys, batch.CursorKey, remaining) {
		record := out[idx].record
		selected = append(selected, record)
		batch.SelectedPackages = append(batch.SelectedPackages, strings.TrimSpace(record.record["Package"]))
		batch.SelectedItemKeys = append(batch.SelectedItemKeys, out[idx].key)
		batch.NextCursorKey = out[idx].key
	}
	batch.SelectedCount = len(selected)
	logPackagePageBatch(batch)
	return selected, batch
}

func repositoryPackageRecordBatchKey(record repositoryPackageRecord) string {
	return strings.ToLower(strings.TrimSpace(record.record["Package"]))
}

func countUniqueRepositoryPackageRecords(records []repositoryPackageRecord) int {
	seen := map[string]bool{}
	for _, record := range records {
		key := repositoryPackageRecordBatchKey(record)
		if key != "" {
			seen[key] = true
		}
	}
	return len(seen)
}

func firstRepositoryPackageRecordRepository(records []repositoryPackageRecord) string {
	for _, record := range records {
		if record.repository != "" {
			return record.repository
		}
	}
	return ""
}

func repositoryPackagePagePayload(candidate repositoryPackageRecord, htmlText string) map[string]any {
	record := candidate.record
	fields := cranPackagePageFields(htmlText)
	fieldRows := cranPackagePageFieldRows(candidate.pageURL, htmlText)
	meta := metaValues(htmlText)
	allLinks := cranAllPageLinks(candidate.pageURL, htmlText, fieldRows)
	allLinks = append(allLinks, cranLinksInHTML(candidate.pageURL, htmlText)...)
	allLinks = uniqueCRANLinks(allLinks)
	documentationLinks := uniqueCRANLinks(append(cranLinksInSection(candidate.pageURL, htmlText, "Documentation"), cranLinksInSection(candidate.pageURL, htmlText, "Readme and manuals")...))
	downloadLinks := uniqueCRANLinks(append(cranLinksInSection(candidate.pageURL, htmlText, "Package Archives"), cranLinksInSection(candidate.pageURL, htmlText, "Downloads")...))
	vignetteLinks := uniqueCRANLinks(append(cranLinksInSection(candidate.pageURL, htmlText, "Vignettes"), linksContaining(allLinks, "/vignettes/", "/articles/", "/doc/")...))
	materialLinks := linksContaining(allLinks, "README", "NEWS", "citation", "manual", "card.")
	referenceHTML := firstNonEmpty(firstLinkContaining(allLinks, "/doc/manual.html"), firstLinkContaining(allLinks, "manual.html"))
	referencePDF := firstNonEmpty(firstLinkContaining(allLinks, "/manuals/"), firstLinkContaining(allLinks, "/"+strings.TrimSpace(record["Package"])+".pdf"), firstLinkWithSuffix(allLinks, ".pdf"))
	sourcePackageURL := firstNonEmpty(candidate.sourcePackageURL, firstLinkMatching(downloadLinks, ".tar.gz"), firstLinkMatching(allLinks, ".tar.gz"))
	archiveURL := firstLinkContaining(allLinks, "Archive/")
	doiLinks := cranLinksForLabel(candidate.pageURL, htmlText, "DOI")
	urlLinks := cranLinksForLabel(candidate.pageURL, htmlText, "URL")
	bugLinks := cranLinksForLabel(candidate.pageURL, htmlText, "BugReports")
	readmeURL := firstNonEmpty(firstLinkLabelContaining(materialLinks, "readme"), firstLinkContaining(allLinks, "/readme"), firstLinkContaining(allLinks, "#readme"))
	newsURL := firstNonEmpty(firstLinkLabelContaining(materialLinks, "news"), firstLinkContaining(allLinks, "/NEWS"))
	payload := recordPayload(record)
	for key, value := range candidate.extra {
		payload[key] = value
	}
	payload["fields_json"] = mustJSON(fields)
	payload["field_rows_json"] = mustJSON(fieldRows)
	payload["links_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(allLinks, envInt("RPKG_CRAN_PAGE_LINK_LIMIT", 120))))
	payload["sections_json"] = mustJSON(genericPackagePageSections(candidate.pageURL, htmlText, []string{"Documentation", "Readme and manuals", "Details", "Package Archives", "Downloads"}))
	payload["package"] = firstNonEmpty(record["Package"], fields["Package"])
	payload["version"] = firstNonEmpty(fields["Version"], record["Version"])
	payload["title"] = firstNonEmpty(meta["og:title"], firstHeading(htmlText), record["Title"])
	payload["description"] = firstNonEmpty(meta["og:description"], meta["description"], firstParagraph(htmlText), record["Description"])
	payload["depends"] = firstNonEmpty(fields["Depends"], record["Depends"])
	payload["imports"] = firstNonEmpty(fields["Imports"], record["Imports"])
	payload["suggests"] = firstNonEmpty(fields["Suggests"], record["Suggests"])
	payload["linking_to"] = firstNonEmpty(fields["LinkingTo"], record["LinkingTo"])
	payload["enhances"] = firstNonEmpty(fields["Enhances"], record["Enhances"])
	payload["published"] = firstNonEmpty(fields["Published"], record["Date/Publication"], stringAny(record["Packaged"]))
	payload["doi"] = fields["DOI"]
	payload["doi_url"] = firstLinkContaining(doiLinks, "doi.org")
	payload["citation_url"] = firstNonEmpty(firstLinkContaining(allLinks, "citation"), firstLinkContaining(allLinks, "CITATION"))
	payload["author"] = firstNonEmpty(fields["Author"], record["Author"])
	payload["maintainer"] = firstNonEmpty(fields["Maintainer"], record["Maintainer"])
	payload["bug_reports"] = firstNonEmpty(fields["BugReports"], record["BugReports"])
	payload["bug_reports_url"] = firstNonEmpty(firstLinkURL(bugLinks), record["BugReports"])
	payload["license"] = firstNonEmpty(fields["License"], record["License"])
	payload["url"] = firstNonEmpty(fields["URL"], record["URL"], record["RemoteUrl"])
	payload["urls_json"] = mustJSON(cranLinksToMaps(urlLinks))
	payload["needs_compilation"] = firstNonEmpty(fields["NeedsCompilation"], record["NeedsCompilation"])
	payload["materials_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(materialLinks, envInt("RPKG_CRAN_PAGE_LINK_LIMIT", 120))))
	payload["readme_url"] = readmeURL
	payload["news_url"] = newsURL
	payload["in_views"] = firstNonEmpty(fields["biocViews"], record["biocViews"])
	payload["in_views_json"] = mustJSON(splitCSV(firstNonEmpty(fields["biocViews"], record["biocViews"])))
	payload["cran_checks_url"] = ""
	payload["reference_manual_html_url"] = referenceHTML
	payload["reference_manual_pdf_url"] = referencePDF
	payload["documentation_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(documentationLinks, envInt("RPKG_CRAN_PAGE_LINK_LIMIT", 120))))
	payload["vignettes_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(vignetteLinks, envInt("RPKG_CRAN_PAGE_LINK_LIMIT", 120))))
	payload["downloads_json"] = mustJSON(cranLinksToMaps(limitedCRANLinks(downloadLinks, envInt("RPKG_CRAN_PAGE_LINK_LIMIT", 120))))
	payload["package_source_url"] = sourcePackageURL
	payload["archive_url"] = archiveURL
	payload["reverse_depends_count"] = 0
	payload["reverse_imports_count"] = 0
	payload["reverse_suggests_count"] = 0
	payload["reverse_linking_to_count"] = 0
	payload["reverse_enhances_count"] = 0
	payload["reverse_depends_json"] = "[]"
	payload["reverse_imports_json"] = "[]"
	payload["reverse_suggests_json"] = "[]"
	payload["reverse_linking_to_json"] = "[]"
	payload["reverse_enhances_json"] = "[]"
	payload["all_links_count"] = len(allLinks)
	payload["page_url"] = candidate.pageURL
	payload["source_method"] = candidate.pageMethod
	payload["parser_version"] = 1
	payload["collection_status"] = "collected"
	return payload
}

func genericPackagePageSections(baseURL, htmlText string, headings []string) []map[string]any {
	out := make([]map[string]any, 0)
	textLimit := envInt("RPKG_CRAN_PAGE_SECTION_TEXT_LIMIT", 2000)
	linkLimit := envInt("RPKG_CRAN_PAGE_SECTION_LINK_LIMIT", 20)
	for _, heading := range headings {
		sectionHTML := htmlSectionAfter(htmlText, heading)
		if sectionHTML == "" {
			continue
		}
		out = append(out, map[string]any{
			"heading": strings.TrimSuffix(heading, ":"),
			"text":    truncate(stripTags(sectionHTML), textLimit),
			"links":   cranLinksToMaps(limitedCRANLinks(cranLinksInHTML(baseURL, sectionHTML), linkLimit)),
		})
	}
	return out
}

func linksContaining(rows []cranPageLink, needles ...string) []cranPageLink {
	out := make([]cranPageLink, 0)
	for _, row := range rows {
		haystack := strings.ToLower(row.Label + " " + row.URL)
		for _, needle := range needles {
			if strings.Contains(haystack, strings.ToLower(needle)) {
				out = append(out, row)
				break
			}
		}
	}
	return uniqueCRANLinks(out)
}

func packageDocumentArtifactLinks(payload map[string]any) []cranPageLink {
	out := cranPackageArtifactLinks(payload)
	for _, raw := range []string{
		stringAny(payload["vignettes_json"]),
		stringAny(payload["downloads_json"]),
		stringAny(payload["links_json"]),
	} {
		var rows []map[string]string
		if err := json.Unmarshal([]byte(raw), &rows); err != nil {
			continue
		}
		for _, row := range rows {
			link := cranPageLink{
				Label:   row["label"],
				URL:     row["url"],
				Section: row["section"],
				Type:    row["type"],
			}
			if isCRANPackageArtifact(link) || strings.Contains(strings.ToLower(link.URL), "/manual") || strings.Contains(strings.ToLower(link.URL), "/citation") {
				link.Type = firstNonEmpty(link.Type, cranArtifactType(link))
				out = append(out, link)
			}
		}
	}
	return uniqueCRANLinks(out)
}

func collectCRANWebsiteDiscovery(records []cranRecord, limit int) []genericEvent {
	events := make([]genericEvent, 0)
	seen := map[string]bool{}
	for _, record := range records {
		packageName := record["Package"]
		if packageName == "" {
			continue
		}
		for _, candidateURL := range descriptionURLs(record) {
			if limit > 0 && len(events) >= limit {
				return events
			}
			key := packageName + "|" + candidateURL
			if seen[key] {
				continue
			}
			seen[key] = true
			parsed := parseYouTubeRef(candidateURL)
			payload := map[string]any{
				"package":           packageName,
				"version":           record["Version"],
				"candidate_url":     candidateURL,
				"url_hash":          shaHex(candidateURL),
				"host":              hostFromURL(candidateURL),
				"source_fields":     "URL/BugReports",
				"parsed_ref_type":   parsed["parsed_ref_type"],
				"is_youtube":        boolOrString(strings.Contains(strings.ToLower(hostFromURL(candidateURL)), "youtube.") || strings.Contains(strings.ToLower(hostFromURL(candidateURL)), "youtu.be")),
				"source_method":     "cran_description_url_bugreports_discovery",
				"collection_status": "collected",
			}
			events = append(events, newGenericEvent("rpkg.r_website.candidate_snapshot.v1", "cran_description_url_bugreports_discovery", candidateURL, "R-Web", packageName, record["Version"], "", payload))
		}
	}
	return events
}

func collectRWebsites(limit, feedLimit, linkLimit, sitemapLimit int) ([]genericEvent, error) {
	urls := configuredURLs("RPKG_R_WEBSITE_URLS", defaultWebsites)
	mentions := splitCSV(envString("RPKG_WEBSITE_MENTION_PACKAGES", "ggplot2,dplyr,shiny,tidymodels,quarto,data.table,tidyverse"))
	events := make([]genericEvent, 0)
	feedCount := 0
	linkCount := 0
	sitemapCount := 0
	for idx, targetURL := range urls {
		if limit > 0 && idx >= limit {
			break
		}
		payload := fetchWebsitePayload(targetURL)
		events = append(events, newGenericEvent("rpkg.r_website.fetch_snapshot.v1", "r_website_seed_fetcher", targetURL, "R-Web", "", "", "", payload))
		for _, packageName := range packageMentionsInText(stringAny(payload["page_text"]), mentions) {
			events = append(events, newGenericEvent("rpkg.r_website.package_mention_snapshot.v1", "r_website_seed_fetcher", targetURL, "R-Web", packageName, "", "", map[string]any{
				"source_url":        targetURL,
				"package":           packageName,
				"repository":        "CRAN",
				"mention_context":   packageName,
				"confidence":        0.4,
				"detected_at":       utcNow(),
				"source":            "r_website_seed_fetcher",
				"source_method":     "html_text_scan_no_api",
				"collection_status": "collected",
			}))
		}
		for _, linkURL := range anyStringSlice(payload["link_urls"]) {
			if linkLimit > 0 && linkCount >= linkLimit {
				break
			}
			linkPayload := map[string]any{
				"source_url":        targetURL,
				"target_url":        linkURL,
				"target_host":       hostFromURL(linkURL),
				"url_hash":          shaHex(targetURL + ">" + linkURL),
				"source_method":     "r_website_html_link_scan",
				"collection_status": "collected",
			}
			events = append(events, newGenericEvent("rpkg.r_website.link_edge_snapshot.v1", "r_website_html_link_scan", linkURL, "R-Web", "", "", "", linkPayload))
			linkCount++
		}
		for _, feedURL := range anyStringSlice(payload["feed_urls"]) {
			if feedLimit > 0 && feedCount >= feedLimit {
				break
			}
			events = append(events, newGenericEvent("rpkg.r_website.feed.discovered.v1", "r_website_feed_discovery", feedURL, "R-Web", "", "", "", map[string]any{
				"source_url":        targetURL,
				"feed_url":          feedURL,
				"feed_host":         hostFromURL(feedURL),
				"source_method":     "html_feed_link_discovery",
				"collection_status": "collected",
			}))
			feedEvents, consumed := collectRWebsiteFeedItems(feedURL, mentions, maxInt(1, envInt("RPKG_R_WEBSITE_FEED_ITEMS_PER_FEED", 10)), maxInt(0, feedLimit-feedCount))
			events = append(events, feedEvents...)
			feedCount += consumed
		}
		if sitemapLimit <= 0 || sitemapCount < sitemapLimit {
			sitemapEvents, consumed := collectRWebsiteSitemap(targetURL, maxInt(0, sitemapLimit-sitemapCount))
			events = append(events, sitemapEvents...)
			sitemapCount += consumed
		}
	}
	return events, nil
}

func collectRWebsiteFeedItems(feedURL string, mentionPackages []string, perFeedLimit, remainingLimit int) ([]genericEvent, int) {
	if remainingLimit == 0 {
		remainingLimit = perFeedLimit
	}
	body, err := fetchBytes(feedURL)
	if err != nil {
		return []genericEvent{collectionFailureEvent("rpkg.r_website.feed_item.failure.v1", "r_website_feed_fetch", feedURL, "R-Web", "", err)}, 0
	}
	items := parseFeedItems(feedURL, body)
	if perFeedLimit > 0 && len(items) > perFeedLimit {
		items = items[:perFeedLimit]
	}
	if remainingLimit > 0 && len(items) > remainingLimit {
		items = items[:remainingLimit]
	}
	events := make([]genericEvent, 0, len(items)*2)
	for _, item := range items {
		itemURL := firstNonEmpty(item["item_url"], feedURL)
		observed := item["published_at"]
		payload := map[string]any{
			"feed_url":          feedURL,
			"feed_host":         hostFromURL(feedURL),
			"item_id":           item["item_id"],
			"item_title":        item["item_title"],
			"item_url":          itemURL,
			"published_at":      observed,
			"summary_text":      item["summary_text"],
			"summary_html":      item["summary_html"],
			"source_method":     item["source_method"],
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent("rpkg.r_website.feed_item_snapshot.v1", item["source_method"], itemURL, "R-Web", "", "", observed, payload))
		mentionText := item["item_title"] + " " + item["summary_text"]
		for _, packageName := range packageMentionsInText(mentionText, mentionPackages) {
			events = append(events, newGenericEvent("rpkg.r_website.package_mention_snapshot.v1", "r_website_feed_item_scan", itemURL, "R-Web", packageName, "", observed, map[string]any{
				"source_url":        itemURL,
				"feed_url":          feedURL,
				"package":           packageName,
				"repository":        "CRAN",
				"mention_context":   packageName,
				"confidence":        0.55,
				"detected_at":       utcNow(),
				"source":            "r_website_feed_item_scan",
				"source_method":     "feed_item_text_scan_no_api",
				"collection_status": "collected",
			}))
		}
	}
	return events, len(items)
}

func collectRWebsiteSitemap(targetURL string, remainingLimit int) ([]genericEvent, int) {
	sitemapURL := sitemapURLFor(targetURL)
	if sitemapURL == "" {
		return nil, 0
	}
	body, err := fetchBytes(sitemapURL)
	if err != nil {
		return nil, 0
	}
	urls := sitemapURLs(body)
	if remainingLimit > 0 && len(urls) > remainingLimit {
		urls = urls[:remainingLimit]
	}
	events := make([]genericEvent, 0, len(urls))
	for _, pageURL := range urls {
		payload := map[string]any{
			"sitemap_url":       sitemapURL,
			"page_url":          pageURL,
			"page_host":         hostFromURL(pageURL),
			"url_hash":          shaHex(pageURL),
			"source_method":     "sitemap_xml_url_parser",
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent("rpkg.r_website.sitemap_url_snapshot.v1", "sitemap_xml_url_parser", pageURL, "R-Web", "", "", "", payload))
	}
	return events, len(urls)
}

type packageRepoRef struct {
	packageName string
	version     string
	repoURL     string
	owner       string
	repo        string
}

func collectGitHubRepositories(records []cranRecord, limit int) ([]genericEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	token := firstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GH_TOKEN"))
	refs := githubRepoRefs(records, limit)
	events := make([]genericEvent, 0, len(refs))
	for _, ref := range refs {
		apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s", url.PathEscape(ref.owner), url.PathEscape(ref.repo))
		var decoded map[string]any
		headers := map[string]string{
			"Accept":               "application/vnd.github+json",
			"X-GitHub-Api-Version": "2022-11-28",
		}
		if token != "" {
			headers["Authorization"] = "Bearer " + token
		}
		if err := fetchJSONWithHeaders(apiURL, headers, &decoded); err != nil {
			events = append(events, collectionFailureEvent("rpkg.github.collection.failure.v1", "github_rest_repos", apiURL, "GitHub", ref.packageName, err))
			continue
		}
		license := mapAny(decoded["license"])
		payload := map[string]any{
			"package":           ref.packageName,
			"version":           ref.version,
			"repository_url":    ref.repoURL,
			"repo_host":         "github",
			"repo_owner":        ref.owner,
			"repo_name":         ref.repo,
			"full_name":         stringAny(decoded["full_name"]),
			"html_url":          stringAny(decoded["html_url"]),
			"description":       stringAny(decoded["description"]),
			"homepage":          stringAny(decoded["homepage"]),
			"default_branch":    stringAny(decoded["default_branch"]),
			"language":          stringAny(decoded["language"]),
			"license_spdx":      stringAny(license["spdx_id"]),
			"topics_json":       mustJSON(anySlice(decoded["topics"])),
			"stargazers_count":  intString(decoded["stargazers_count"]),
			"forks_count":       intString(decoded["forks_count"]),
			"watchers_count":    intString(decoded["watchers_count"]),
			"open_issues_count": intString(decoded["open_issues_count"]),
			"subscribers_count": intString(decoded["subscribers_count"]),
			"created_at":        stringAny(decoded["created_at"]),
			"updated_at":        stringAny(decoded["updated_at"]),
			"pushed_at":         stringAny(decoded["pushed_at"]),
			"archived":          boolString(decoded["archived"]),
			"disabled":          boolString(decoded["disabled"]),
			"source_method":     "github_rest_repos",
			"collection_status": "collected",
		}
		events = append(events, newGenericEvent("rpkg.github.repo_snapshot.v1", "github_rest_repos", apiURL, "GitHub", ref.packageName, ref.version, stringAny(decoded["updated_at"]), payload))
	}
	return events, nil
}

func collectOSVSecurity(records []cranRecord, limit int) ([]genericEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	ecosystems := splitCSV(envString("RPKG_OSV_ECOSYSTEMS", "CRAN,Bioconductor"))
	if len(ecosystems) == 0 {
		ecosystems = []string{"CRAN"}
	}
	events := make([]genericEvent, 0, limit*len(ecosystems))
	count := 0
	for _, record := range records {
		packageName := record["Package"]
		if packageName == "" {
			continue
		}
		if count >= limit {
			break
		}
		count++
		for _, ecosystem := range ecosystems {
			requestPayload := map[string]any{"package": map[string]any{"name": packageName, "ecosystem": ecosystem}}
			var decoded map[string]any
			sourceURL := "https://api.osv.dev/v1/query"
			if err := postJSON(sourceURL, requestPayload, &decoded); err != nil {
				events = append(events, collectionFailureEvent("rpkg.security.osv.failure.v1", "osv_query", sourceURL, ecosystem, packageName, err))
				continue
			}
			vulns := anySlice(decoded["vulns"])
			vulnIDs := make([]string, 0, len(vulns))
			for _, item := range vulns {
				row := mapAny(item)
				if id := stringAny(row["id"]); id != "" {
					vulnIDs = append(vulnIDs, id)
				}
			}
			payload := map[string]any{
				"package":             packageName,
				"version":             record["Version"],
				"ecosystem":           ecosystem,
				"vulnerability_count": intString(len(vulnIDs)),
				"vuln_ids":            vulnIDs,
				"vuln_ids_json":       mustJSON(vulnIDs),
				"osv_response_json":   truncate(mustJSON(decoded), 50000),
				"source_method":       "osv_query_api",
				"collection_status":   "collected",
			}
			events = append(events, newGenericEvent("rpkg.security.osv_snapshot.v1", "osv_query", sourceURL, ecosystem, packageName, record["Version"], "", payload))
		}
	}
	return events, nil
}

func collectBibliometricMentions(records []cranRecord, limit int) ([]genericEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	events := make([]genericEvent, 0, limit)
	count := 0
	for _, record := range records {
		packageName := record["Package"]
		if packageName == "" {
			continue
		}
		if count >= limit {
			break
		}
		query := fmt.Sprintf("\"R package %s\"", packageName)
		sourceURL := "https://api.openalex.org/works?search=" + url.QueryEscape(query) + "&per-page=1"
		var decoded map[string]any
		if err := fetchJSON(sourceURL, &decoded); err != nil {
			events = append(events, collectionFailureEvent("rpkg.bibliometric.openalex.failure.v1", "openalex_works_search", sourceURL, "OpenAlex", packageName, err))
			count++
			continue
		}
		meta := mapAny(decoded["meta"])
		results := anySlice(decoded["results"])
		top := map[string]any{}
		if len(results) > 0 {
			top = mapAny(results[0])
		}
		payload := map[string]any{
			"package":           packageName,
			"version":           record["Version"],
			"query":             query,
			"result_count":      intString(meta["count"]),
			"top_work_id":       stringAny(top["id"]),
			"top_work_title":    stringAny(top["title"]),
			"top_work_year":     intString(top["publication_year"]),
			"top_work_cited_by": intString(top["cited_by_count"]),
			"source_method":     "openalex_works_search",
			"collection_status": "collected",
			"confidence":        "phrase_search",
		}
		events = append(events, newGenericEvent("rpkg.bibliometric.mention_snapshot.v1", "openalex_works_search", sourceURL, "OpenAlex", packageName, record["Version"], "", payload))
		count++
	}
	return events, nil
}

func collectYouTubeJob(job string, seedLimit, pageLimit, videoLimit, backfillLimit, transcriptLimit, commentLimit int) ([]genericEvent, error) {
	var seeds []map[string]any
	var err error
	if job != "backfill-metadata" {
		seeds, err = loadYouTubeSeedsFromClickHouse(seedLimit)
		if err != nil {
			return nil, err
		}
	}
	switch job {
	case "seeds":
		return youtubeSeedEvents(seeds, seedLimit), nil
	case "pages":
		return youtubePageEvents(seeds, pageLimit), nil
	case "search":
		return youtubeSearchEvents(seeds, envInt("R_YOUTUBE_SEARCH_EVENT_LIMIT", 0)), nil
	case "links":
		return youtubeLinkEvents(pageLimit), nil
	case "videos":
		return youtubeVideoEvents(seeds, videoLimit), nil
	case "transcripts":
		return youtubeTranscriptEvents(seeds, transcriptLimit), nil
	case "comments":
		return youtubeCommentEvents(seeds, commentLimit), nil
	case "backfill-metadata":
		return youtubeMetadataBackfillEvents(backfillLimit)
	default:
		return nil, fmt.Errorf("unknown youtube job %q", job)
	}
}

func youtubeSeedEvents(seeds []map[string]any, limit int) []genericEvent {
	events := make([]genericEvent, 0, len(seeds))
	for _, seed := range seeds {
		if limit > 0 && len(events) >= limit {
			break
		}
		payload := normalizeYouTubeSeed(seed)
		events = append(events, newGenericEvent("r.youtube.source.seed.v1", "r_youtube_seed_loader_go", stringAny(payload["url"]), "R-YouTube", "", "", "", payload))
	}
	return events
}

func loadYouTubeSeedsFromClickHouse(limit int) ([]map[string]any, error) {
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return nil, err
	}
	queryLimit := limit
	if queryLimit <= 0 {
		queryLimit = envInt("R_YOUTUBE_DB_SEED_QUERY_LIMIT", 1000)
	}
	queryLimit = maxInt(1, queryLimit)
	queries := []string{
		fmt.Sprintf(`SELECT
    source_code,
    source_type,
    title,
    url,
    category,
    language_hint,
    source_confidence,
    priority,
    parsed_ref_type,
    parsed_video_id,
    parsed_playlist_id,
    parsed_channel_id,
    parsed_handle,
    notes,
    'clickhouse_source_seed_current' AS source_method
FROM Data_R_Community_Service.r_youtube_source_seed_current
WHERE active = 1
ORDER BY priority, source_code
LIMIT %d
FORMAT JSONEachRow`, queryLimit),
		fmt.Sprintf(`SELECT
    concat('web_r_official_', toString(uuid)) AS source_code,
    'video' AS source_type,
    title,
    url,
    source_category AS category,
    language_code AS language_hint,
    'admin_migrated_legacy' AS source_confidence,
    'P0' AS priority,
    'video' AS parsed_ref_type,
    ifNull(nullIf(extract(ifNull(url, ''), '(?:v=|youtu\\.be/|shorts/|live/)([A-Za-z0-9_-]{6,})'), ''), '') AS parsed_video_id,
    '' AS parsed_playlist_id,
    '' AS parsed_channel_id,
    '' AS parsed_handle,
    'Migrated from Web-R official YouTube records' AS notes,
    'clickhouse_webr_official_youtube' AS source_method
FROM Data_R_Community_Service.v_webr_official_youtube
WHERE active = 1
ORDER BY updated_at DESC
LIMIT %d
FORMAT JSONEachRow`, queryLimit),
	}
	var rows []map[string]any
	var lastErr error
	for _, query := range queries {
		currentRows, err := cfg.queryJSONEachRow(query)
		if err != nil {
			lastErr = err
			continue
		}
		rows = append(rows, currentRows...)
	}
	rows = uniqueSeedRows(rows)
	if len(rows) == 0 && lastErr != nil && envBool("R_YOUTUBE_DB_SEEDS_REQUIRED", true) {
		return nil, fmt.Errorf("youtube seed DB query failed: %w", lastErr)
	}
	return firstNAnyMaps(rows, queryLimit), nil
}

func loadYouTubeMetadataBackfillRows(limit int) ([]map[string]any, error) {
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return nil, err
	}
	queryLimit := limit
	if queryLimit <= 0 {
		queryLimit = envInt("R_YOUTUBE_BACKFILL_QUERY_LIMIT", 100)
	}
	queryLimit = maxInt(1, queryLimit)
	query := fmt.Sprintf(`SELECT
    youtube_video_id,
    source_tag,
    uuid_article,
    stable_uuid,
    canonical_url,
    video_title,
    video_description,
    thumbnail_url,
    youtube_channel_id,
    channel_title,
    published_at,
    duration_seconds,
    view_count,
    like_count,
    comment_count,
    caption_available,
    default_audio_language,
    default_language,
    tags_json,
    source_method,
    source_category,
    source_confidence,
    language_code,
    payload_json,
    '' AS payload_hash,
    last_collected_at
FROM
(
    SELECT
        *,
        (
            if(trim(video_title) = '' OR lowerUTF8(trim(video_title)) IN ('youtube', '- youtube') OR endsWith(video_title, ' - YouTube') OR startsWith(video_title, 'YouTube video '), 40, 0) +
            if(trim(video_description) = '' OR video_description = '%s' OR startsWith(video_description, 'Discovered from YouTube search query:') OR video_description = video_title, 20, 0) +
            if(trim(thumbnail_url) = '' OR positionCaseInsensitive(thumbnail_url, 'ytimg.com') = 0, 10, 0) +
            if(trim(youtube_channel_id) = '', 8, 0) +
            if(trim(channel_title) = '' OR channel_title = video_title, 8, 0) +
            if(published_at = '', 8, 0) +
            if(toUInt64OrZero(duration_seconds) = 0, 8, 0) +
            if(toUInt64OrZero(view_count) = 0, 6, 0) +
            if(trim(default_audio_language) = '' AND trim(default_language) = '' AND language_code IN ('', 'und'), 4, 0) +
            if(tags_json IN ('', '[]', '{}'), 3, 0) +
            if(positionCaseInsensitive(source_method, 'metadata_unavailable') > 0 OR positionCaseInsensitive(source_method, 'unenriched') > 0 OR positionCaseInsensitive(source_method, 'public_html_no_data_api') > 0 OR positionCaseInsensitive(source_method, 'legacy_webr_board_youtube') > 0, 12, 0)
        ) AS metadata_quality_score
    FROM
    (
        SELECT
            youtube_video_id,
            source_tag,
            uuid_article,
            argMax(toString(uuid), collected_at) AS stable_uuid,
            argMax(canonical_url, collected_at) AS canonical_url,
            argMax(video_title, collected_at) AS video_title,
            argMax(video_description, collected_at) AS video_description,
            argMax(thumbnail_url, collected_at) AS thumbnail_url,
            argMax(youtube_channel_id, collected_at) AS youtube_channel_id,
            argMax(channel_title, collected_at) AS channel_title,
            ifNull(toString(argMax(published_at, collected_at)), '') AS published_at,
            toString(ifNull(argMax(duration_seconds, collected_at), 0)) AS duration_seconds,
            toString(argMax(view_count, collected_at)) AS view_count,
            toString(argMax(like_count, collected_at)) AS like_count,
            toString(argMax(comment_count, collected_at)) AS comment_count,
            toString(argMax(caption_available, collected_at)) AS caption_available,
            argMax(default_audio_language, collected_at) AS default_audio_language,
            argMax(default_language, collected_at) AS default_language,
            argMax(tags_json, collected_at) AS tags_json,
            argMax(source_method, collected_at) AS source_method,
            argMax(source_category, collected_at) AS source_category,
            argMax(source_confidence, collected_at) AS source_confidence,
            argMax(language_code, collected_at) AS language_code,
            argMax(payload_json, collected_at) AS payload_json,
            max(collected_at) AS last_collected_at
        FROM
        (
            SELECT
                youtube_video_id,
                source_tag,
                ifNull(toString(uuid_article), '') AS uuid_article,
                uuid,
                canonical_url,
                video_title,
                video_description,
                thumbnail_url,
                youtube_channel_id,
                channel_title,
                published_at,
                duration_seconds,
                view_count,
                like_count,
                comment_count,
                caption_available,
                default_audio_language,
                default_language,
                tags_json,
                source_method,
                source_category,
                source_confidence,
                language_code,
                payload_json,
                collected_at
            FROM Data_R_Community_Service.r_youtube_video_current
            WHERE active = 1
              AND notEmpty(youtube_video_id)
        )
        GROUP BY youtube_video_id, source_tag, uuid_article
    )
)
WHERE metadata_quality_score > 0
ORDER BY metadata_quality_score DESC, last_collected_at ASC, youtube_video_id
LIMIT %d
FORMAT JSONEachRow`, youtubeBoilerplateDescription, queryLimit)
	return cfg.queryJSONEachRow(query)
}

func youtubePageEvents(seeds []map[string]any, limit int) []genericEvent {
	events := make([]genericEvent, 0)
	for _, seed := range seeds {
		if limit > 0 && len(events) >= limit {
			break
		}
		seedPayload := normalizeYouTubeSeed(seed)
		targetURL := stringAny(seedPayload["url"])
		if targetURL == "" || strings.Contains(targetURL, "/results?") {
			continue
		}
		page := fetchWebsitePayload(targetURL)
		page["seed_source_code"] = seedPayload["source_code"]
		page["source_method"] = "youtube_public_html_no_data_api"
		events = append(events, newGenericEvent("r.youtube.page.snapshot.v1", "youtube_public_html", targetURL, "R-YouTube", "", "", "", page))
		ref := parseYouTubeRef(targetURL)
		if ref["parsed_video_id"] != "" {
			payload := map[string]any{
				"youtube_video_id":       ref["parsed_video_id"],
				"youtube_channel_id":     "",
				"playlist_ids_json":      "[]",
				"video_title":            stringAny(page["title"]),
				"video_description":      stringAny(page["description"]),
				"canonical_url":          targetURL,
				"thumbnail_url":          stringAny(page["og_image"]),
				"published_at":           "",
				"duration_seconds":       "0",
				"view_count":             "0",
				"like_count":             "0",
				"comment_count":          "0",
				"favorite_count":         "0",
				"caption_available":      "0",
				"default_audio_language": "",
				"default_language":       "",
				"language_code":          firstNonEmpty(stringAny(seedPayload["language_hint"]), "und"),
				"tags_json":              "[]",
				"thumbnail_urls_json":    "{}",
				"channel_title":          stringAny(seedPayload["title"]),
				"privacy_status":         "",
				"source_method":          "youtube_public_html_no_data_api",
				"source_tag":             "r_project_ecosystem_youtube",
				"source_category":        stringAny(seedPayload["category"]),
				"source_confidence":      firstNonEmpty(stringAny(seedPayload["source_confidence"]), "html_discovered"),
				"collection_status":      "collected",
			}
			finalizeYouTubeVideoPayload(payload)
			events = append(events, newGenericEvent("r.youtube.video.snapshot.v1", "youtube_public_html", targetURL, "R-YouTube", "", "", "", payload))
		}
	}
	return events
}

func youtubeLinkEvents(limit int) []genericEvent {
	urls := configuredURLs("R_YOUTUBE_DISCOVERY_URLS", defaultWebsites)
	events := make([]genericEvent, 0)
	for idx, targetURL := range urls {
		if limit > 0 && idx >= limit {
			break
		}
		payload := fetchWebsitePayload(targetURL)
		for _, link := range anyStringSlice(payload["youtube_urls"]) {
			ref := parseYouTubeRef(link)
			ref["source_url"] = targetURL
			ref["target_url"] = link
			ref["source_method"] = "r_website_youtube_link_scan_no_api"
			ref["collection_status"] = "collected"
			events = append(events, newGenericEvent("r.youtube.link.edge.v1", "r_website_youtube_link_scan", link, "R-YouTube", "", "", "", mapStringAny(ref)))
		}
	}
	return events
}

type youtubeVideoCandidate struct {
	videoID string
	url     string
	seed    map[string]any
}

func youtubeVideoEvents(seeds []map[string]any, limit int) []genericEvent {
	candidates := youtubeVideoCandidates(seeds, limit)
	events := make([]genericEvent, 0, len(candidates)*2)
	for _, candidate := range candidates {
		payload, err := fetchYouTubeVideoSnapshotPayload(candidate.videoID, candidate.url, candidate.seed)
		if err != nil {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "youtube_video_metadata_enrichment", candidate.url, "R-YouTube", "", err))
			continue
		}
		events = append(events, newGenericEvent("r.youtube.video.snapshot.v1", stringAny(payload["source_method"]), candidate.url, "R-YouTube", "", "", stringAny(payload["published_at"]), payload))
		if strings.Contains(stringAny(payload["source_method"]), "youtube_data_api") {
			events = append(events, youtubeQuotaUsageEvent(candidate.url))
		}
		events = append(events, youtubeMetadataPackageMentionEvents(candidate.videoID, payload)...)
	}
	return events
}

type youtubeCaptionTrack struct {
	key      string
	lang     string
	name     string
	ext      string
	url      string
	auto     bool
	provider string
}

type youtubeTranscriptSegment struct {
	index   int
	startMS int64
	endMS   int64
	textRaw string
}

func youtubeTranscriptEvents(seeds []map[string]any, limit int) []genericEvent {
	candidates := youtubeVideoCandidates(seeds, limit)
	events := make([]genericEvent, 0)
	for _, candidate := range candidates {
		segmentEvents, err := fetchYouTubeTranscriptSegmentEvents(candidate.videoID, candidate.url)
		if err != nil {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "yt_dlp_public_transcript_no_api", candidate.url, "R-YouTube", "", err))
			continue
		}
		events = append(events, segmentEvents...)
	}
	return events
}

func fetchYouTubeTranscriptSegmentEvents(videoID, canonicalURL string) ([]genericEvent, error) {
	decoded, err := fetchYTDLPJSON(canonicalURL)
	if err != nil {
		return nil, err
	}
	tracks := captionTracksFromYTDLP(decoded)
	if len(tracks) == 0 {
		return nil, errors.New("yt-dlp returned no public subtitle or automatic caption tracks")
	}
	maxTracks := maxInt(1, envInt("R_YOUTUBE_TRANSCRIPT_TRACK_LIMIT", 2))
	if len(tracks) > maxTracks {
		tracks = tracks[:maxTracks]
	}
	videoID = firstNonEmpty(videoID, stringAny(decoded["id"]), parseYouTubeRef(canonicalURL)["parsed_video_id"])
	if videoID == "" {
		return nil, errors.New("youtube video id is required for transcript collection")
	}
	events := make([]genericEvent, 0)
	for _, track := range tracks {
		body, err := fetchBytes(track.url)
		if err != nil {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "yt_dlp_caption_track_fetch", track.url, "R-YouTube", "", err))
			continue
		}
		segments := parseCaptionSegments(body, track.ext)
		if len(segments) == 0 {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "yt_dlp_caption_track_parse", track.url, "R-YouTube", "", errors.New("caption track produced zero VTT segments")))
			continue
		}
		for _, segment := range segments {
			textNormalized := stripTags(segment.textRaw)
			if textNormalized == "" {
				continue
			}
			payload := map[string]any{
				"youtube_video_id":       videoID,
				"caption_track_key":      firstNonEmpty(track.key, track.lang),
				"language_code":          firstNonEmpty(track.lang, "und"),
				"segment_index":          intString(segment.index),
				"start_ms":               intString(segment.startMS),
				"end_ms":                 intString(segment.endMS),
				"duration_ms":            intString(maxInt64(0, segment.endMS-segment.startMS)),
				"text_raw":               truncate(segment.textRaw, 4000),
				"text_normalized":        truncate(textNormalized, 4000),
				"is_auto_generated":      boolOrString(track.auto),
				"source_method":          "yt_dlp_public_caption_track_no_api",
				"collection_status":      "collected",
				"retention_policy_code":  "retain_public_caption_best_effort",
				"caption_track_name":     track.name,
				"caption_track_provider": track.provider,
				"caption_track_ext":      track.ext,
			}
			events = append(events, newGenericEvent("r.youtube.transcript.segment.v1", "yt_dlp_public_caption_track_no_api", canonicalURL, "R-YouTube", "", "", "", payload))
			events = append(events, youtubeTextPackageMentionEvents(
				videoID,
				canonicalURL,
				"transcript",
				firstNonEmpty(track.lang, "und"),
				textNormalized,
				segment.startMS,
				segment.endMS,
				"",
				"transcript_text_scan_no_api",
				"rpkg-youtube-transcript-mention-v1",
				"0.70",
			)...)
		}
	}
	if len(events) == 0 {
		return nil, errors.New("no transcript events were produced")
	}
	return events, nil
}

func youtubeCommentEvents(seeds []map[string]any, limit int) []genericEvent {
	apiKey := firstNonEmpty(os.Getenv("YOUTUBE_API_KEY"), os.Getenv("GOOGLE_YOUTUBE_API_KEY"))
	if apiKey == "" || envBool("R_YOUTUBE_DISABLE_COMMENT_API", false) {
		return []genericEvent{collectionFailureEvent("r.youtube.collection.failure.v1", "youtube_data_api_v3_commentThreads_list", "https://www.googleapis.com/youtube/v3/commentThreads", "R-YouTube", "", errors.New("YouTube comments require YOUTUBE_API_KEY or GOOGLE_YOUTUBE_API_KEY"))}
	}
	candidates := youtubeVideoCandidates(seeds, limit)
	events := make([]genericEvent, 0)
	for _, candidate := range candidates {
		commentEvents, err := fetchYouTubeCommentThreadEvents(candidate.videoID, candidate.url, apiKey)
		if err != nil {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "youtube_data_api_v3_commentThreads_list", candidate.url, "R-YouTube", "", err))
			continue
		}
		events = append(events, commentEvents...)
	}
	return events
}

func fetchYouTubeCommentThreadEvents(videoID, canonicalURL, apiKey string) ([]genericEvent, error) {
	videoID = firstNonEmpty(videoID, parseYouTubeRef(canonicalURL)["parsed_video_id"])
	if videoID == "" {
		return nil, errors.New("youtube video id is required for comment collection")
	}
	maxResults := maxInt(1, minInt(envInt("R_YOUTUBE_COMMENT_MAX_RESULTS", 50), 100))
	pageLimit := maxInt(1, envInt("R_YOUTUBE_COMMENT_PAGE_LIMIT", 1))
	events := make([]genericEvent, 0)
	pageToken := ""
	for page := 0; page < pageLimit; page++ {
		q := url.Values{}
		if envBool("R_YOUTUBE_INCLUDE_COMMENT_REPLIES", true) {
			q.Set("part", "snippet,replies")
		} else {
			q.Set("part", "snippet")
		}
		q.Set("videoId", videoID)
		q.Set("maxResults", strconv.Itoa(maxResults))
		q.Set("textFormat", "plainText")
		q.Set("key", apiKey)
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}
		sourceURL := "https://www.googleapis.com/youtube/v3/commentThreads?" + q.Encode()
		var decoded map[string]any
		if err := fetchJSON(sourceURL, &decoded); err != nil {
			return events, err
		}
		for _, item := range anySlice(decoded["items"]) {
			thread := mapAny(item)
			threadID := stringAny(thread["id"])
			snippet := mapAny(thread["snippet"])
			totalReplyCount := intAny(snippet["totalReplyCount"])
			topComment := mapAny(snippet["topLevelComment"])
			if len(topComment) > 0 {
				payload := youtubeCommentPayload(videoID, threadID, "", topComment, totalReplyCount)
				events = append(events, newGenericEvent("r.youtube.comment.thread.v1", "youtube_data_api_v3_commentThreads_list", canonicalURL, "R-YouTube", "", "", stringAny(payload["published_at"]), payload))
				events = append(events, youtubeTextPackageMentionEvents(videoID, canonicalURL, "comment", "und", stringAny(payload["text_normalized"]), 0, 0, stringAny(payload["published_at"]), "comment_text_scan_api", "rpkg-youtube-comment-mention-v1", "0.55")...)
			}
			if envBool("R_YOUTUBE_INCLUDE_COMMENT_REPLIES", true) {
				replies := mapAny(thread["replies"])
				for _, reply := range anySlice(replies["comments"]) {
					parentID := stringAny(mapAny(topComment)["id"])
					payload := youtubeCommentPayload(videoID, threadID, parentID, mapAny(reply), 0)
					events = append(events, newGenericEvent("r.youtube.comment.thread.v1", "youtube_data_api_v3_commentThreads_list", canonicalURL, "R-YouTube", "", "", stringAny(payload["published_at"]), payload))
					events = append(events, youtubeTextPackageMentionEvents(videoID, canonicalURL, "comment", "und", stringAny(payload["text_normalized"]), 0, 0, stringAny(payload["published_at"]), "comment_text_scan_api", "rpkg-youtube-comment-mention-v1", "0.55")...)
				}
			}
		}
		events = append(events, youtubeQuotaUsageEventFor(canonicalURL, "commentThreads.list", 1))
		pageToken = stringAny(decoded["nextPageToken"])
		if pageToken == "" {
			break
		}
	}
	if len(events) == 0 {
		return nil, errors.New("commentThreads.list returned no comment rows")
	}
	return events, nil
}

func youtubeCommentPayload(videoID, threadID, parentID string, comment map[string]any, totalReplyCount int64) map[string]any {
	snippet := mapAny(comment["snippet"])
	commentID := stringAny(comment["id"])
	textOriginal := firstNonEmpty(stringAny(snippet["textOriginal"]), stripTags(stringAny(snippet["textDisplay"])))
	authorChannel := mapAny(snippet["authorChannelId"])
	salt := envString("YOUTUBE_COMMENT_HASH_SALT", "statground-r-youtube")
	authorChannelID := stringAny(authorChannel["value"])
	authorDisplayName := stringAny(snippet["authorDisplayName"])
	return map[string]any{
		"youtube_video_id":         videoID,
		"comment_thread_id":        firstNonEmpty(threadID, commentID),
		"comment_id":               commentID,
		"parent_comment_id":        parentID,
		"author_channel_id_hash":   hashMaybe(salt, authorChannelID),
		"author_display_name_hash": hashMaybe(salt, authorDisplayName),
		"text_original":            truncate(textOriginal, envInt("R_YOUTUBE_COMMENT_TEXT_LIMIT", 2000)),
		"text_normalized":          truncate(stripTags(textOriginal), envInt("R_YOUTUBE_COMMENT_TEXT_LIMIT", 2000)),
		"like_count":               intString(snippet["likeCount"]),
		"published_at":             stringAny(snippet["publishedAt"]),
		"updated_at":               stringAny(snippet["updatedAt"]),
		"total_reply_count":        intString(totalReplyCount),
		"source_method":            "youtube_data_api_v3_commentThreads_list",
		"retention_policy_code":    "public_comment_aggregate_only",
	}
}

func youtubeMetadataBackfillEvents(limit int) ([]genericEvent, error) {
	rows, err := loadYouTubeMetadataBackfillRows(limit)
	if err != nil {
		return nil, err
	}
	events := make([]genericEvent, 0, len(rows)*3)
	for _, row := range rows {
		videoID := stringAny(row["youtube_video_id"])
		if videoID == "" {
			continue
		}
		stableUUID := firstNonEmpty(stringAny(row["stable_uuid"]), stableYouTubeVideoUUID(videoID, stringAny(row["source_tag"]), stringAny(row["uuid_article"])))
		currentPayload := currentYouTubeSnapshotPayload(row, stableUUID, "0")
		deactivate := newGenericEvent("r.youtube.video.snapshot.v1", "youtube_metadata_refresh_backfill", stringAny(currentPayload["canonical_url"]), "R-YouTube", "", "", stringAny(currentPayload["published_at"]), currentPayload)
		deactivate.CollectedAt = time.Now().UTC().Add(-2 * time.Millisecond).Format("2006-01-02T15:04:05.000Z")
		events = append(events, deactivate)

		seed := map[string]any{
			"title":             stringAny(row["video_title"]),
			"url":               firstNonEmpty(stringAny(row["canonical_url"]), "https://www.youtube.com/watch?v="+videoID),
			"category":          stringAny(row["source_category"]),
			"source_type":       "video",
			"source_tag":        firstNonEmpty(stringAny(row["source_tag"]), "r_project_ecosystem_youtube"),
			"source_confidence": firstNonEmpty(stringAny(row["source_confidence"]), "metadata_refresh"),
			"language_hint":     firstNonEmpty(stringAny(row["language_code"]), "und"),
			"stable_uuid":       stableUUID,
			"uuid_article":      stringAny(row["uuid_article"]),
			"source_code":       "current_video_refresh",
		}
		payload, err := fetchYouTubeVideoSnapshotPayload(videoID, stringAny(seed["url"]), seed)
		if err != nil {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "youtube_metadata_refresh_backfill", stringAny(seed["url"]), "R-YouTube", "", err))
			continue
		}
		payload["stable_uuid"] = stableUUID
		payload["uuid_article"] = stringAny(row["uuid_article"])
		payload["active"] = "1"
		payload["source_tag"] = firstNonEmpty(stringAny(row["source_tag"]), stringAny(payload["source_tag"]), "r_project_ecosystem_youtube")
		payload["source_category"] = firstNonEmpty(stringAny(row["source_category"]), stringAny(payload["source_category"]), "metadata_refresh")
		payload["source_confidence"] = "metadata_refreshed"
		payload["previous_payload_hash"] = stringAny(row["payload_hash"])
		payload["refresh_reason"] = "existing_video_metadata_may_change"
		payload["source_method"] = firstNonEmpty(stringAny(payload["source_method"]), "youtube_public_metadata") + "+current_metadata_refresh"
		event := newGenericEvent("r.youtube.video.snapshot.v1", stringAny(payload["source_method"]), stringAny(payload["canonical_url"]), "R-YouTube", "", "", stringAny(payload["published_at"]), payload)
		events = append(events, event)
		if strings.Contains(stringAny(payload["source_method"]), "youtube_data_api") {
			events = append(events, youtubeQuotaUsageEvent(stringAny(payload["canonical_url"])))
		}
		events = append(events, youtubeMetadataPackageMentionEvents(videoID, payload)...)
	}
	return events, nil
}

func youtubeQuotaUsageEvent(sourceURL string) genericEvent {
	return youtubeQuotaUsageEventFor(sourceURL, "videos.list", 1)
}

func youtubeQuotaUsageEventFor(sourceURL, methodName string, quotaCost int) genericEvent {
	return newGenericEvent("r.youtube.quota.usage.v1", "youtube_data_api_v3", sourceURL, "R-YouTube", "", "", "", map[string]any{
		"quota_date":        time.Now().UTC().Format("2006-01-02"),
		"api_key_alias":     envString("YOUTUBE_API_KEY_ALIAS", "default"),
		"method_name":       methodName,
		"quota_cost":        strconv.Itoa(quotaCost),
		"request_count":     "1",
		"quota_units_used":  strconv.Itoa(quotaCost),
		"source_method":     "youtube_data_api_v3_" + strings.ReplaceAll(methodName, ".", "_"),
		"collection_status": "collected",
	})
}

func youtubeVideoCandidates(seeds []map[string]any, limit int) []youtubeVideoCandidate {
	seen := map[string]bool{}
	out := make([]youtubeVideoCandidate, 0)
	add := func(videoID, rawURL string, seed map[string]any) {
		videoID = strings.TrimSpace(videoID)
		if videoID == "" || seen[videoID] {
			return
		}
		seen[videoID] = true
		if rawURL == "" {
			rawURL = "https://www.youtube.com/watch?v=" + videoID
		}
		out = append(out, youtubeVideoCandidate{videoID: videoID, url: rawURL, seed: seed})
	}
	for _, rawID := range splitCSV(envString("R_YOUTUBE_VIDEO_IDS", "")) {
		add(rawID, "https://www.youtube.com/watch?v="+rawID, map[string]any{
			"source_code":       "env_video_ids",
			"source_confidence": "manual_env",
			"category":          "manual",
		})
	}
	for _, rawURL := range splitCSV(envString("R_YOUTUBE_VIDEO_URLS", "")) {
		ref := parseYouTubeRef(rawURL)
		add(ref["parsed_video_id"], rawURL, map[string]any{
			"source_code":       "env_video_urls",
			"source_confidence": "manual_env",
			"category":          "manual",
		})
	}
	for _, seed := range seeds {
		payload := normalizeYouTubeSeed(seed)
		videoID := stringAny(payload["parsed_video_id"])
		add(videoID, stringAny(payload["url"]), payload)
	}
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func fetchYouTubeVideoSnapshotPayload(videoID, canonicalURL string, seed map[string]any) (map[string]any, error) {
	videoID = firstNonEmpty(videoID, parseYouTubeRef(canonicalURL)["parsed_video_id"])
	if videoID == "" {
		return nil, errors.New("youtube video id is required")
	}
	canonicalURL = firstNonEmpty(canonicalURL, "https://www.youtube.com/watch?v="+videoID)
	payload := baseYouTubeVideoPayload(videoID, canonicalURL, seed)
	methods := make([]string, 0, 3)
	errs := make([]string, 0, 3)
	if apiKey := firstNonEmpty(os.Getenv("YOUTUBE_API_KEY"), os.Getenv("GOOGLE_YOUTUBE_API_KEY")); apiKey != "" && !envBool("R_YOUTUBE_DISABLE_DATA_API", false) {
		if apiPayload, err := fetchYouTubeAPIVideoPayload(videoID, apiKey, seed); err == nil {
			mergePayload(payload, apiPayload)
			methods = append(methods, "youtube_data_api_v3_videos_list")
		} else {
			errs = append(errs, "youtube_data_api: "+err.Error())
		}
	}
	if needsYouTubeMetadataFill(payload) && !envBool("R_YOUTUBE_DISABLE_YTDLP", false) {
		if dlPayload, err := fetchYTDLPVideoPayload(canonicalURL, seed); err == nil {
			mergePayload(payload, dlPayload)
			methods = append(methods, "yt_dlp_public_metadata_no_api")
		} else {
			errs = append(errs, "yt_dlp: "+err.Error())
		}
	}
	if needsYouTubeMetadataFill(payload) {
		if embedPayload, err := fetchYouTubeOEmbedPayload(canonicalURL); err == nil {
			mergePayload(payload, embedPayload)
			methods = append(methods, "youtube_oembed_public_metadata")
		} else {
			errs = append(errs, "oembed: "+err.Error())
		}
	}
	if needsYouTubeMetadataFill(payload) {
		page := fetchWebsitePayload(canonicalURL)
		if stringAny(page["collection_status"]) == "collected" {
			mergePayload(payload, map[string]any{
				"video_title":       stringAny(page["title"]),
				"video_description": stringAny(page["description"]),
				"thumbnail_url":     stringAny(page["og_image"]),
			})
			methods = append(methods, "youtube_public_html_meta")
		} else if errCode := stringAny(page["error_code"]); errCode != "" {
			errs = append(errs, "html: "+errCode)
		}
	}
	if len(methods) == 0 {
		payload["source_method"] = "youtube_metadata_unavailable"
		payload["collection_status"] = "failed"
		payload["metadata_errors_json"] = mustJSON(errs)
		return payload, errors.New(strings.Join(errs, " | "))
	}
	finalizeYouTubeVideoPayload(payload)
	payload["source_method"] = strings.Join(uniqueStrings(methods), "+")
	payload["collection_status"] = "collected"
	if len(errs) > 0 {
		payload["metadata_errors_json"] = mustJSON(errs)
	}
	return payload, nil
}

func baseYouTubeVideoPayload(videoID, canonicalURL string, seed map[string]any) map[string]any {
	seed = normalizeYouTubeSeed(seed)
	sourceTag := firstNonEmpty(stringAny(seed["source_tag"]), "r_project_ecosystem_youtube")
	uuidArticle := stringAny(seed["uuid_article"])
	stableUUID := firstNonEmpty(stringAny(seed["stable_uuid"]), stableYouTubeVideoUUID(videoID, sourceTag, uuidArticle))
	return map[string]any{
		"youtube_video_id":       videoID,
		"youtube_channel_id":     "",
		"playlist_ids_json":      "[]",
		"video_title":            stringAny(seed["title"]),
		"video_description":      "",
		"canonical_url":          firstNonEmpty(canonicalURL, "https://www.youtube.com/watch?v="+videoID),
		"thumbnail_url":          "",
		"published_at":           "",
		"duration_seconds":       "0",
		"view_count":             "0",
		"like_count":             "0",
		"comment_count":          "0",
		"favorite_count":         "0",
		"caption_available":      "0",
		"default_audio_language": "",
		"default_language":       "",
		"language_code":          firstNonEmpty(stringAny(seed["language_hint"]), "und"),
		"tags_json":              "[]",
		"thumbnail_urls_json":    "{}",
		"channel_title":          stringAny(seed["title"]),
		"privacy_status":         "",
		"source_method":          "youtube_video_metadata_enrichment",
		"source_tag":             sourceTag,
		"source_category":        firstNonEmpty(stringAny(seed["category"]), stringAny(seed["source_type"]), "video"),
		"source_confidence":      firstNonEmpty(stringAny(seed["source_confidence"]), "seed_or_discovered"),
		"seed_source_code":       stringAny(seed["source_code"]),
		"stable_uuid":            stableUUID,
		"uuid_article":           uuidArticle,
		"active":                 "1",
		"collection_status":      "pending",
	}
}

func currentYouTubeSnapshotPayload(row map[string]any, stableUUID, active string) map[string]any {
	videoID := stringAny(row["youtube_video_id"])
	sourceTag := firstNonEmpty(stringAny(row["source_tag"]), "r_project_ecosystem_youtube")
	uuidArticle := stringAny(row["uuid_article"])
	stableUUID = firstNonEmpty(stableUUID, stableYouTubeVideoUUID(videoID, sourceTag, uuidArticle))
	return map[string]any{
		"youtube_video_id":       videoID,
		"youtube_channel_id":     stringAny(row["youtube_channel_id"]),
		"playlist_ids_json":      "[]",
		"video_title":            stringAny(row["video_title"]),
		"video_description":      stringAny(row["video_description"]),
		"canonical_url":          firstNonEmpty(stringAny(row["canonical_url"]), "https://www.youtube.com/watch?v="+videoID),
		"thumbnail_url":          stringAny(row["thumbnail_url"]),
		"published_at":           stringAny(row["published_at"]),
		"duration_seconds":       stringAny(row["duration_seconds"]),
		"view_count":             stringAny(row["view_count"]),
		"like_count":             stringAny(row["like_count"]),
		"comment_count":          stringAny(row["comment_count"]),
		"favorite_count":         "0",
		"caption_available":      stringAny(row["caption_available"]),
		"default_audio_language": stringAny(row["default_audio_language"]),
		"default_language":       stringAny(row["default_language"]),
		"language_code":          firstNonEmpty(stringAny(row["language_code"]), "und"),
		"tags_json":              firstNonEmpty(stringAny(row["tags_json"]), "[]"),
		"thumbnail_urls_json":    "{}",
		"channel_title":          stringAny(row["channel_title"]),
		"privacy_status":         "",
		"source_method":          "youtube_metadata_refresh_previous_inactive",
		"source_tag":             sourceTag,
		"source_category":        stringAny(row["source_category"]),
		"source_confidence":      firstNonEmpty(stringAny(row["source_confidence"]), "previous_current"),
		"stable_uuid":            stableUUID,
		"uuid_article":           uuidArticle,
		"active":                 active,
		"collection_status":      "superseded",
		"previous_payload_hash":  stringAny(row["payload_hash"]),
	}
}

func fetchYouTubeAPIVideoPayload(videoID, apiKey string, seed map[string]any) (map[string]any, error) {
	endpoint := "https://www.googleapis.com/youtube/v3/videos"
	q := url.Values{}
	q.Set("part", "snippet,contentDetails,statistics,status")
	q.Set("id", videoID)
	q.Set("key", apiKey)
	var decoded map[string]any
	if err := fetchJSON(endpoint+"?"+q.Encode(), &decoded); err != nil {
		return nil, err
	}
	items := anySlice(decoded["items"])
	if len(items) == 0 {
		return nil, errors.New("videos.list returned no items")
	}
	item := mapAny(items[0])
	snippet := mapAny(item["snippet"])
	content := mapAny(item["contentDetails"])
	stats := mapAny(item["statistics"])
	status := mapAny(item["status"])
	thumbnails := mapAny(snippet["thumbnails"])
	bestThumb, thumbJSON := bestYouTubeAPIThumbnail(thumbnails)
	return map[string]any{
		"youtube_video_id":       firstNonEmpty(stringAny(item["id"]), videoID),
		"youtube_channel_id":     stringAny(snippet["channelId"]),
		"video_title":            stringAny(snippet["title"]),
		"video_description":      stringAny(snippet["description"]),
		"canonical_url":          "https://www.youtube.com/watch?v=" + videoID,
		"thumbnail_url":          bestThumb,
		"published_at":           stringAny(snippet["publishedAt"]),
		"duration_seconds":       intString(parseISO8601DurationSeconds(stringAny(content["duration"]))),
		"view_count":             intString(stats["viewCount"]),
		"like_count":             intString(stats["likeCount"]),
		"comment_count":          intString(stats["commentCount"]),
		"favorite_count":         intString(stats["favoriteCount"]),
		"caption_available":      boolOrString(stringAny(content["caption"]) == "true"),
		"default_audio_language": stringAny(content["defaultAudioLanguage"]),
		"default_language":       stringAny(snippet["defaultLanguage"]),
		"language_code":          firstNonEmpty(stringAny(snippet["defaultAudioLanguage"]), stringAny(snippet["defaultLanguage"]), stringAny(seed["language_hint"]), "und"),
		"tags_json":              mustJSON(anySlice(snippet["tags"])),
		"thumbnail_urls_json":    thumbJSON,
		"channel_title":          stringAny(snippet["channelTitle"]),
		"privacy_status":         stringAny(status["privacyStatus"]),
	}, nil
}

func fetchYTDLPVideoPayload(canonicalURL string, seed map[string]any) (map[string]any, error) {
	bin, err := youtubeDLBinary()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxInt(10, envInt("YOUTUBE_DL_TIMEOUT", 90)))*time.Second)
	defer cancel()
	template := strings.Join([]string{
		"%(id)j",
		"%(title)j",
		"%(description)j",
		"%(channel_id)j",
		"%(channel)j",
		"%(timestamp)j",
		"%(upload_date)j",
		"%(duration)j",
		"%(view_count)j",
		"%(like_count)j",
		"%(comment_count)j",
		"%(language)j",
		"%(tags)j",
		"%(thumbnails)j",
		"%(availability)j",
		"%(webpage_url)j",
	}, "\t")
	args := []string{"--skip-download", "--no-playlist", "--no-warnings", "--ignore-no-formats-error", "--print", template, canonicalURL}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, truncate(string(out), 800))
	}
	decoded, err := parseYTDLPPrintedMetadata(string(out))
	if err != nil {
		return nil, err
	}
	videoID := firstNonEmpty(stringAny(decoded["id"]), parseYouTubeRef(canonicalURL)["parsed_video_id"])
	thumbnails := anySlice(decoded["thumbnails"])
	language := firstNonEmpty(stringAny(decoded["language"]), stringAny(seed["language_hint"]))
	return map[string]any{
		"youtube_video_id":       videoID,
		"youtube_channel_id":     stringAny(decoded["channel_id"]),
		"playlist_ids_json":      "[]",
		"video_title":            stringAny(decoded["title"]),
		"video_description":      stringAny(decoded["description"]),
		"canonical_url":          firstNonEmpty(stringAny(decoded["webpage_url"]), canonicalURL),
		"thumbnail_url":          firstNonEmpty(stringAny(decoded["thumbnail"]), bestYTDLPThumbnail(thumbnails)),
		"published_at":           ytdlpPrintedPublishedAt(decoded),
		"duration_seconds":       intString(decoded["duration"]),
		"view_count":             intString(decoded["view_count"]),
		"like_count":             intString(decoded["like_count"]),
		"comment_count":          intString(decoded["comment_count"]),
		"favorite_count":         "0",
		"caption_available":      "0",
		"default_audio_language": language,
		"default_language":       language,
		"language_code":          firstNonEmpty(language, "und"),
		"tags_json":              mustJSON(anySlice(decoded["tags"])),
		"thumbnail_urls_json":    mustJSON(thumbnails),
		"channel_title":          stringAny(decoded["channel"]),
		"privacy_status":         stringAny(decoded["availability"]),
	}, nil
}

func fetchYTDLPJSON(canonicalURL string) (map[string]any, error) {
	bin, err := youtubeDLBinary()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxInt(10, envInt("YOUTUBE_DL_TIMEOUT", 120)))*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--skip-download", "--no-playlist", "--no-warnings", "--ignore-no-formats-error", "-J", canonicalURL)
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, truncate(string(out), 800))
	}
	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func captionTracksFromYTDLP(decoded map[string]any) []youtubeCaptionTrack {
	out := make([]youtubeCaptionTrack, 0)
	seen := map[string]bool{}
	addContainer := func(key string, auto bool) {
		container := mapAny(decoded[key])
		langs := prioritizedCaptionLanguages(container)
		for _, lang := range langs {
			for _, item := range anySlice(container[lang]) {
				row := mapAny(item)
				trackURL := stringAny(row["url"])
				if trackURL == "" {
					continue
				}
				ext := strings.ToLower(firstNonEmpty(stringAny(row["ext"]), stringAny(row["protocol"])))
				trackKey := key + ":" + lang + ":" + ext
				if seen[trackKey] {
					continue
				}
				if !captionTrackSupported(ext, trackURL) {
					continue
				}
				seen[trackKey] = true
				out = append(out, youtubeCaptionTrack{
					key:      trackKey,
					lang:     lang,
					name:     stringAny(row["name"]),
					ext:      ext,
					url:      trackURL,
					auto:     auto,
					provider: key,
				})
				break
			}
		}
	}
	addContainer("subtitles", false)
	addContainer("automatic_captions", true)
	return out
}

func prioritizedCaptionLanguages(container map[string]any) []string {
	out := make([]string, 0)
	seen := map[string]bool{}
	for _, lang := range splitCSV(envString("R_YOUTUBE_TRANSCRIPT_LANGS", "ko,en,en-US")) {
		if _, ok := container[lang]; ok && !seen[lang] {
			out = append(out, lang)
			seen[lang] = true
		}
	}
	for lang := range container {
		if !seen[lang] {
			out = append(out, lang)
			seen[lang] = true
		}
	}
	return out
}

func captionTrackSupported(ext, trackURL string) bool {
	ext = strings.ToLower(ext)
	lowerURL := strings.ToLower(trackURL)
	return ext == "vtt" || ext == "json3" || strings.Contains(lowerURL, "fmt=vtt") || strings.Contains(lowerURL, "fmt=json3")
}

func parseCaptionSegments(body []byte, ext string) []youtubeTranscriptSegment {
	ext = strings.ToLower(ext)
	if ext == "json3" || bytes.Contains(bytes.TrimSpace(body), []byte(`"events"`)) {
		if rows := parseJSON3TranscriptSegments(body); len(rows) > 0 {
			return rows
		}
	}
	return parseVTTTranscriptSegments(body)
}

func parseJSON3TranscriptSegments(body []byte) []youtubeTranscriptSegment {
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil
	}
	events := anySlice(decoded["events"])
	out := make([]youtubeTranscriptSegment, 0, len(events))
	for _, item := range events {
		row := mapAny(item)
		start := intAny(row["tStartMs"])
		duration := intAny(row["dDurationMs"])
		textParts := make([]string, 0)
		for _, seg := range anySlice(row["segs"]) {
			if text := stringAny(mapAny(seg)["utf8"]); text != "" {
				textParts = append(textParts, text)
			}
		}
		text := stripTags(strings.Join(textParts, ""))
		if text == "" {
			continue
		}
		out = append(out, youtubeTranscriptSegment{
			index:   len(out),
			startMS: start,
			endMS:   start + duration,
			textRaw: text,
		})
	}
	return out
}

func parseVTTTranscriptSegments(body []byte) []youtubeTranscriptSegment {
	text := strings.ReplaceAll(string(bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf})), "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	blocks := strings.Split(text, "\n\n")
	out := make([]youtubeTranscriptSegment, 0, len(blocks))
	for _, block := range blocks {
		lines := strings.Split(strings.TrimSpace(block), "\n")
		if len(lines) == 0 {
			continue
		}
		timeLine := -1
		for idx, line := range lines {
			if strings.Contains(line, "-->") {
				timeLine = idx
				break
			}
		}
		if timeLine < 0 || timeLine+1 >= len(lines) {
			continue
		}
		parts := strings.Split(lines[timeLine], "-->")
		if len(parts) != 2 {
			continue
		}
		startFields := strings.Fields(strings.TrimSpace(parts[0]))
		if len(startFields) == 0 {
			continue
		}
		start := parseVTTTimeMS(startFields[0])
		endFields := strings.Fields(strings.TrimSpace(parts[1]))
		if len(endFields) == 0 {
			continue
		}
		end := parseVTTTimeMS(endFields[0])
		if end <= start {
			continue
		}
		cueText := stripTags(strings.Join(lines[timeLine+1:], " "))
		if cueText == "" || strings.EqualFold(cueText, "WEBVTT") {
			continue
		}
		out = append(out, youtubeTranscriptSegment{
			index:   len(out),
			startMS: start,
			endMS:   end,
			textRaw: cueText,
		})
	}
	return out
}

func parseVTTTimeMS(value string) int64 {
	value = strings.ReplaceAll(strings.TrimSpace(value), ",", ".")
	parts := strings.Split(value, ":")
	var hours, minutes int64
	secondsPart := "0"
	switch len(parts) {
	case 3:
		hours, _ = strconv.ParseInt(parts[0], 10, 64)
		minutes, _ = strconv.ParseInt(parts[1], 10, 64)
		secondsPart = parts[2]
	case 2:
		minutes, _ = strconv.ParseInt(parts[0], 10, 64)
		secondsPart = parts[1]
	default:
		secondsPart = value
	}
	seconds, _ := strconv.ParseFloat(secondsPart, 64)
	return hours*3600000 + minutes*60000 + int64(seconds*1000)
}

func fetchYouTubeOEmbedPayload(canonicalURL string) (map[string]any, error) {
	sourceURL := "https://www.youtube.com/oembed?format=json&url=" + url.QueryEscape(canonicalURL)
	var decoded map[string]any
	if err := fetchJSON(sourceURL, &decoded); err != nil {
		return nil, err
	}
	return map[string]any{
		"video_title":   stringAny(decoded["title"]),
		"thumbnail_url": stringAny(decoded["thumbnail_url"]),
		"channel_title": stringAny(decoded["author_name"]),
		"canonical_url": canonicalURL,
		"source_oembed": sourceURL,
	}, nil
}

func youtubeSearchEvents(seeds []map[string]any, limit int) []genericEvent {
	queries := youtubeSearchQueries(seeds)
	events := make([]genericEvent, 0)
	enrichedVideos := 0
	enrichLimit := envInt("R_YOUTUBE_SEARCH_VIDEO_ENRICH_LIMIT", 10)
	for _, query := range queries {
		if limit > 0 && len(events) >= limit {
			break
		}
		searchURL := "https://www.youtube.com/results?search_query=" + url.QueryEscape(query)
		body, err := fetchBytes(searchURL)
		if err != nil {
			events = append(events, collectionFailureEvent("r.youtube.collection.failure.v1", "youtube_public_search_html", searchURL, "R-YouTube", "", err))
			continue
		}
		results := extractYouTubeSearchResults(string(body), query, searchURL)
		for _, result := range results {
			if limit > 0 && len(events) >= limit {
				break
			}
			payload := mapStringAny(result)
			events = append(events, newGenericEvent("r.youtube.search.result.v1", "youtube_public_search_html", result["result_url"], "R-YouTube", "", "", "", payload))
			if result["parsed_video_id"] != "" {
				if enrichLimit > 0 && enrichedVideos < enrichLimit {
					seedPayload := map[string]any{
						"title":             query,
						"url":               result["result_url"],
						"category":          "search_result",
						"source_type":       "video",
						"source_confidence": "search_html_discovered",
						"language_hint":     "und",
					}
					if videoPayload, err := fetchYouTubeVideoSnapshotPayload(result["parsed_video_id"], result["result_url"], seedPayload); err == nil {
						videoPayload["source_category"] = "search_result"
						videoPayload["source_confidence"] = "search_html_discovered"
						videoPayload["search_query"] = query
						events = append(events, newGenericEvent("r.youtube.video.snapshot.v1", stringAny(videoPayload["source_method"]), result["result_url"], "R-YouTube", "", "", stringAny(videoPayload["published_at"]), videoPayload))
						if strings.Contains(stringAny(videoPayload["source_method"]), "youtube_data_api") {
							events = append(events, youtubeQuotaUsageEvent(result["result_url"]))
						}
						events = append(events, youtubeMetadataPackageMentionEvents(result["parsed_video_id"], videoPayload)...)
						enrichedVideos++
						continue
					}
				}
				videoPayload := map[string]any{
					"youtube_video_id":       result["parsed_video_id"],
					"youtube_channel_id":     "",
					"playlist_ids_json":      "[]",
					"video_title":            "YouTube video " + result["parsed_video_id"],
					"video_description":      "Discovered from YouTube search query: " + query,
					"canonical_url":          result["result_url"],
					"thumbnail_url":          "https://i.ytimg.com/vi/" + result["parsed_video_id"] + "/hqdefault.jpg",
					"published_at":           "",
					"duration_seconds":       "0",
					"view_count":             "0",
					"like_count":             "0",
					"comment_count":          "0",
					"favorite_count":         "0",
					"caption_available":      "0",
					"default_audio_language": "",
					"default_language":       "",
					"language_code":          "und",
					"tags_json":              "[]",
					"thumbnail_urls_json":    mustJSON(map[string]any{"hqdefault": map[string]any{"url": "https://i.ytimg.com/vi/" + result["parsed_video_id"] + "/hqdefault.jpg"}}),
					"channel_title":          "",
					"privacy_status":         "",
					"source_method":          "youtube_public_search_html_unenriched_candidate",
					"source_tag":             "r_project_ecosystem_youtube",
					"source_category":        "search_result",
					"source_confidence":      "search_html_discovered_unenriched",
					"metadata_errors_json":   mustJSON([]string{"metadata_enrich_limit_reached"}),
					"active":                 "0",
					"collection_status":      "candidate",
				}
				finalizeYouTubeVideoPayload(videoPayload)
				events = append(events, newGenericEvent("r.youtube.video.candidate.v1", "youtube_public_search_html", result["result_url"], "R-YouTube", "", "", "", videoPayload))
			}
		}
	}
	return events
}

func youtubeSearchQueries(seeds []map[string]any) []string {
	defaults := []string{
		"R programming tutorial",
		"R package tutorial",
		"R statistical computing",
		"CRAN package R",
		"Bioconductor R tutorial",
		"tidyverse tutorial R",
		"ggplot2 tutorial",
		"Shiny R tutorial",
		"Quarto R tutorial",
		"useR conference R",
		"R-Ladies R programming",
	}
	queries := splitCSV(envString("R_YOUTUBE_SEARCH_QUERIES", ""))
	queries = append(queries, defaults...)
	for _, seed := range seeds {
		title := stringAny(seed["title"])
		category := stringAny(seed["category"])
		if title != "" {
			queries = append(queries, title+" R YouTube")
		}
		if category != "" {
			queries = append(queries, category+" R programming YouTube")
		}
	}
	return firstNStrings(uniqueStrings(queries), maxInt(1, envInt("R_YOUTUBE_SEARCH_QUERY_LIMIT", 40)))
}

func extractYouTubeSearchResults(text, query, searchURL string) []map[string]string {
	out := make([]map[string]string, 0)
	seen := map[string]bool{}
	add := func(resultURL string, ref map[string]string) {
		resultURL = strings.ReplaceAll(resultURL, `\u0026`, "&")
		if resultURL == "" || seen[resultURL] {
			return
		}
		seen[resultURL] = true
		row := map[string]string{
			"query":             query,
			"search_url":        searchURL,
			"result_url":        resultURL,
			"source_method":     "youtube_public_search_html_no_data_api",
			"collection_status": "collected",
		}
		for key, value := range ref {
			row[key] = value
		}
		out = append(out, row)
	}
	for _, match := range ytWatchRE.FindAllStringSubmatch(text, -1) {
		videoID := match[1]
		resultURL := "https://www.youtube.com/watch?v=" + videoID
		ref := parseYouTubeRef(resultURL)
		add(resultURL, ref)
	}
	for _, match := range ytPlaylistRE.FindAllStringSubmatch(text, -1) {
		playlistID := match[1]
		resultURL := "https://www.youtube.com/playlist?list=" + playlistID
		ref := parseYouTubeRef(resultURL)
		add(resultURL, ref)
	}
	for _, match := range ytChannelRE.FindAllStringSubmatch(text, -1) {
		path := strings.TrimPrefix(match[1], "/")
		resultURL := "https://www.youtube.com/" + path
		ref := parseYouTubeRef(resultURL)
		add(resultURL, ref)
	}
	return firstNMaps(out, maxInt(1, envInt("R_YOUTUBE_SEARCH_RESULT_LIMIT", 80)))
}

func collectMastodonRSS(instance, acct string, limit int, ai *aiClient, translationModel string, translate bool, dedup mastodonDedupState) ([]webREvent, error) {
	instance = strings.TrimRight(firstNonEmpty(instance, "https://fosstodon.org"), "/")
	acct = strings.TrimPrefix(firstNonEmpty(acct, "R_Foundation"), "@")
	sourceURL := fmt.Sprintf("%s/@%s.rss", instance, url.PathEscape(acct))
	body, err := fetchBytes(sourceURL)
	if err != nil {
		return nil, err
	}
	var feed rssFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, err
	}
	host := hostFromURL(instance)
	events := make([]webREvent, 0, len(feed.Channel.Items)*2+2)
	started := nowKST()
	apiStatuses, apiErr := fetchMastodonAccountStatusesMap(instance, acct, limit)
	sourceMethod := "mastodon_public_rss_no_api"
	if apiErr == nil && len(apiStatuses) > 0 {
		sourceMethod = "mastodon_public_api_noauth+mastodon_public_rss_no_api"
	} else if apiErr != nil {
		events = append(events, newWebREvent("webr.mastodon.log.v1", sourceURL, map[string]any{
			"uuid":          uuid7(),
			"created_at":    formatKST(started),
			"language_code": "en",
			"created_log": map[string]any{
				"type":          "mastodon_pipeline",
				"stage":         "api_enrichment_unavailable",
				"instance":      instance,
				"acct":          acct,
				"error":         apiErr.Error(),
				"source_method": "mastodon_public_api_noauth",
			},
		}, started))
	}
	events = append(events, newWebREvent("webr.mastodon.log.v1", sourceURL, map[string]any{
		"uuid":          uuid7(),
		"created_at":    formatKST(started),
		"language_code": "en",
		"created_log": map[string]any{
			"type":          "mastodon_pipeline",
			"stage":         "rss_started",
			"instance":      instance,
			"acct":          acct,
			"source_method": sourceMethod,
		},
	}, started))
	count := 0
	for _, item := range feed.Channel.Items {
		if limit > 0 && count >= limit {
			break
		}
		statusURL := firstNonEmpty(item.Link, item.GUID)
		if statusURL == "" {
			continue
		}
		statusID := stableID(statusURL)
		rowUUID := deterministicUUID("mastodon:" + host + ":" + acct + ":" + statusID)
		if existingUUID := dedup.rawByURL[statusURL]; existingUUID != "" {
			rowUUID = existingUUID
		}
		createdAt := parseRSSDate(item.PubDate, time.Now())
		contentHTML := strings.TrimSpace(item.Description)
		contentText := stripTags(contentHTML)
		itemSourceMethod := "mastodon_public_rss_no_api"
		rawPayload := map[string]any{
			"uuid":                   rowUUID,
			"instance_host":          host,
			"account_acct":           acct,
			"account_id":             "rss:" + acct,
			"status_id":              statusID,
			"status_uri":             firstNonEmpty(item.GUID, statusURL),
			"status_url":             statusURL,
			"status_created_at":      formatKST(createdAt),
			"status_edited_at":       "",
			"visibility":             "public",
			"language":               "en",
			"language_code":          "en",
			"sensitive":              0,
			"spoiler_text":           "",
			"content_html":           contentHTML,
			"content_text":           contentText,
			"in_reply_to_id":         "",
			"in_reply_to_account_id": "",
			"is_reblog":              0,
			"reblog_status_id":       "",
			"replies_count":          0,
			"reblogs_count":          0,
			"favourites_count":       0,
			"active":                 1,
			"tags":                   []string{},
			"mentions":               []string{},
			"emojis":                 []string{},
			"media_attachments":      []string{},
			"card":                   map[string]any{},
			"poll":                   map[string]any{},
			"raw_status_json":        map[string]any{"rss_title": item.Title, "rss_guid": item.GUID},
			"payload_hash":           stableUInt64(contentHTML + statusURL),
			"image_count":            0,
			"image_base64_count":     0,
			"has_image_base64":       0,
			"fetched_at":             formatKST(time.Now()),
			"source_method":          itemSourceMethod,
		}
		if apiStatus := mastodonMatchStatus(apiStatuses, statusURL, statusID); len(apiStatus) > 0 {
			itemSourceMethod = "mastodon_public_api_noauth+mastodon_public_rss_no_api"
			createdAt = mastodonStatusCreatedAt(apiStatus, createdAt)
			rawPayload = mastodonRawPayloadFromAPI(rowUUID, host, acct, statusURL, statusID, item, apiStatus, createdAt, itemSourceMethod)
			statusID = firstNonEmpty(stringAny(rawPayload["status_id"]), statusID)
			contentHTML = stringAny(rawPayload["content_html"])
			contentText = stringAny(rawPayload["content_text"])
		}
		if !dedup.raw[rowUUID] && dedup.rawByURL[statusURL] == "" {
			events = append(events, newWebREvent("webr.mastodon.raw.v1", statusURL, rawPayload, createdAt))
			dedup.raw[rowUUID] = true
			dedup.rawByURL[statusURL] = rowUUID
		}
		if dedup.translated[rowUUID] || dedup.translatedURL[statusURL] {
			count++
			continue
		}
		boardTitle := firstNonEmpty(stripTags(item.Title), firstWords(contentText, 12), "R Foundation")
		boardContent := safeMastodonBoardContent(boardTitle, safeHTML(firstNonEmpty(contentText, boardTitle)))
		if translate {
			translatedTitle, translatedContent, err := translateMastodonStatus(ai, translationModel, boardTitle, contentText)
			if err != nil {
				events = append(events, newWebREvent("webr.mastodon.log.v1", statusURL, map[string]any{
					"uuid":          uuid7(),
					"created_at":    formatKST(nowKST()),
					"language_code": "en",
					"created_log": map[string]any{
						"type":          "mastodon_pipeline",
						"stage":         "translation_failed",
						"instance":      instance,
						"acct":          acct,
						"status_id":     statusID,
						"status_url":    statusURL,
						"error":         err.Error(),
						"source_method": itemSourceMethod,
					},
				}, nowKST()))
				count++
				continue
			}
			boardTitle = translatedTitle
			boardContent = translatedContent
		}
		boardPayload := mastodonBoardPayloadWithSourceMethod(rowUUID, statusURL, statusID, createdAt, time.Time{}, boardTitle, boardContent, itemSourceMethod)
		events = append(events, newWebREvent("webr.mastodon.board.v1", statusURL, boardPayload, createdAt))
		dedup.translated[rowUUID] = true
		dedup.translatedURL[statusURL] = true
		count++
	}
	done := nowKST()
	events = append(events, newWebREvent("webr.mastodon.log.v1", sourceURL, map[string]any{
		"uuid":          uuid7(),
		"created_at":    formatKST(done),
		"language_code": "en",
		"created_log": map[string]any{
			"type":          "mastodon_pipeline",
			"stage":         "rss_done",
			"instance":      instance,
			"acct":          acct,
			"published":     count,
			"source_method": sourceMethod,
		},
	}, done))
	return events, nil
}

func fetchMastodonAccountStatusesMap(instance, acct string, limit int) (map[string]map[string]any, error) {
	out := map[string]map[string]any{}
	instance = strings.TrimRight(instance, "/")
	acct = strings.TrimPrefix(acct, "@")
	lookupURL := instance + "/api/v1/accounts/lookup?acct=" + url.QueryEscape(acct)
	var account map[string]any
	if err := fetchJSON(lookupURL, &account); err != nil {
		return out, err
	}
	accountID := stringAny(account["id"])
	if accountID == "" {
		return out, errors.New("Mastodon account lookup returned no id")
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(maxInt(1, limit)))
	q.Set("exclude_replies", "false")
	q.Set("exclude_reblogs", "false")
	statusesURL := instance + "/api/v1/accounts/" + url.PathEscape(accountID) + "/statuses?" + q.Encode()
	var statuses []map[string]any
	if err := fetchJSON(statusesURL, &statuses); err != nil {
		return out, err
	}
	for _, status := range statuses {
		for _, key := range mastodonStatusKeys(status) {
			out[key] = status
		}
	}
	return out, nil
}

func mastodonStatusKeys(status map[string]any) []string {
	keys := make([]string, 0)
	status = effectiveMastodonStatus(status)
	for _, key := range []string{"url", "uri", "id"} {
		value := stringAny(status[key])
		if value != "" {
			keys = append(keys, value)
			keys = append(keys, stableID(value))
		}
	}
	return uniqueStrings(keys)
}

func mastodonMatchStatus(statuses map[string]map[string]any, statusURL, statusID string) map[string]any {
	for _, key := range []string{statusURL, statusID, stableID(statusURL)} {
		if status := statuses[key]; len(status) > 0 {
			return status
		}
	}
	return nil
}

func mastodonStatusCreatedAt(status map[string]any, fallback time.Time) time.Time {
	status = effectiveMastodonStatus(status)
	if parsed := parseRSSDate(stringAny(status["created_at"]), time.Time{}); !parsed.IsZero() {
		return parsed
	}
	return fallback
}

func effectiveMastodonStatus(status map[string]any) map[string]any {
	if reblog := mapAny(status["reblog"]); len(reblog) > 0 {
		return reblog
	}
	return status
}

func mastodonRawPayloadFromAPI(rowUUID, host, acct, fallbackStatusURL, fallbackStatusID string, rssItem rssItem, status map[string]any, createdAt time.Time, sourceMethod string) map[string]any {
	originalStatus := status
	status = effectiveMastodonStatus(status)
	account := mapAny(status["account"])
	statusURL := firstNonEmpty(stringAny(status["url"]), stringAny(status["uri"]), fallbackStatusURL)
	statusID := firstNonEmpty(stringAny(status["id"]), fallbackStatusID, stableID(statusURL))
	contentHTML := firstNonEmpty(stringAny(status["content"]), strings.TrimSpace(rssItem.Description))
	contentText := stripTags(contentHTML)
	media := anySlice(status["media_attachments"])
	return map[string]any{
		"uuid":                   rowUUID,
		"instance_host":          host,
		"account_acct":           firstNonEmpty(stringAny(account["acct"]), stringAny(account["username"]), acct),
		"account_id":             firstNonEmpty(stringAny(account["id"]), "rss:"+acct),
		"status_id":              statusID,
		"status_uri":             firstNonEmpty(stringAny(status["uri"]), statusURL),
		"status_url":             statusURL,
		"status_created_at":      formatKST(createdAt),
		"status_edited_at":       stringAny(status["edited_at"]),
		"visibility":             firstNonEmpty(stringAny(status["visibility"]), "public"),
		"language":               firstNonEmpty(stringAny(status["language"]), "en"),
		"language_code":          firstNonEmpty(stringAny(status["language"]), "en"),
		"sensitive":              boolString(status["sensitive"]),
		"spoiler_text":           stringAny(status["spoiler_text"]),
		"content_html":           contentHTML,
		"content_text":           contentText,
		"in_reply_to_id":         stringAny(status["in_reply_to_id"]),
		"in_reply_to_account_id": stringAny(status["in_reply_to_account_id"]),
		"is_reblog":              boolOrString(len(mapAny(originalStatus["reblog"])) > 0),
		"reblog_status_id":       stringAny(mapAny(originalStatus["reblog"])["id"]),
		"replies_count":          intString(status["replies_count"]),
		"reblogs_count":          intString(status["reblogs_count"]),
		"favourites_count":       intString(status["favourites_count"]),
		"active":                 1,
		"tags":                   anySlice(status["tags"]),
		"mentions":               anySlice(status["mentions"]),
		"emojis":                 anySlice(status["emojis"]),
		"media_attachments":      media,
		"card":                   mapAny(status["card"]),
		"poll":                   mapAny(status["poll"]),
		"account":                account,
		"raw_status_json":        status,
		"original_status_json":   originalStatus,
		"payload_hash":           stableUInt64(mustJSON(status)),
		"image_count":            mastodonImageCount(media),
		"image_base64_count":     0,
		"has_image_base64":       0,
		"fetched_at":             formatKST(time.Now()),
		"source_method":          sourceMethod,
	}
}

func mastodonImageCount(media []any) int {
	count := 0
	for _, item := range media {
		row := mapAny(item)
		if strings.EqualFold(stringAny(row["type"]), "image") || stringAny(row["preview_url"]) != "" || stringAny(row["url"]) != "" {
			count++
		}
	}
	return count
}

func emptyMastodonDedupState() mastodonDedupState {
	return mastodonDedupState{
		raw:           map[string]bool{},
		rawByURL:      map[string]string{},
		translated:    map[string]bool{},
		translatedURL: map[string]bool{},
	}
}

func loadMastodonDedupState(ctx context.Context, cfg clickHouseQueryConfig) (mastodonDedupState, error) {
	state := emptyMastodonDedupState()
	rawRows, err := cfg.queryJSONEachRow(`SELECT
    toString(uuid) AS uuid,
    toString(status_url) AS status_url
FROM
(
    SELECT
        uuid,
        status_url,
        row_number() OVER (
            PARTITION BY status_url
            ORDER BY fetched_at DESC, ingested_at DESC
        ) AS rn
    FROM Data_R_Community_Raw.mastodon_status_raw
    WHERE active != 0
      AND notEmpty(status_url)
)
WHERE rn = 1
FORMAT JSONEachRow`)
	if err != nil {
		return state, err
	}
	for _, row := range rawRows {
		if uuid := stringAny(row["uuid"]); uuid != "" {
			state.raw[uuid] = true
			if statusURL := stringAny(row["status_url"]); statusURL != "" {
				state.rawByURL[statusURL] = uuid
			}
		}
	}
	boardRows, err := cfg.queryJSONEachRow(`SELECT
    toString(uuid) AS uuid,
    toString(status_url) AS status_url
FROM Data_R_Community_Service.v_r_foundation_board
WHERE language_code = 'ko'
  AND active != 0
  AND position(toString(created_log), 'mastodon_board_translation') > 0
FORMAT JSONEachRow`)
	if err != nil {
		return state, err
	}
	for _, row := range boardRows {
		if uuid := stringAny(row["uuid"]); uuid != "" {
			state.translated[uuid] = true
		}
		if statusURL := stringAny(row["status_url"]); statusURL != "" {
			state.translatedURL[statusURL] = true
		}
	}
	return state, nil
}

func collectMastodonBoardBackfill(ctx context.Context, cfg clickHouseQueryConfig, limit int, ai *aiClient, translationModel string) ([]webREvent, error) {
	if ai == nil || !ai.enabled() {
		return nil, errors.New("Mastodon board backfill requires an AI provider key")
	}
	queryLimit := maxInt(1, limit)
	query := fmt.Sprintf(`SELECT
    toString(r.uuid) AS uuid,
    toString(r.status_url) AS status_url,
    toString(r.status_id) AS status_id,
    toString(r.status_created_at) AS status_created_at,
    toString(r.content_text) AS content_text,
    toString(r.content_html) AS content_html
FROM Data_R_Community_Raw.mastodon_status_raw AS r
LEFT JOIN
(
    SELECT
        uuid,
        toString(created_log) AS board_log
    FROM Data_R_Community_Service.v_r_foundation_board
    WHERE active != 0
      AND language_code = 'ko'
) AS b ON b.uuid = r.uuid
WHERE r.active != 0
  AND r.status_url NOT IN
  (
      SELECT status_url
      FROM Data_R_Community_Service.v_r_foundation_board
      WHERE active != 0
        AND language_code = 'ko'
        AND notEmpty(status_url)
        AND position(toString(created_log), 'mastodon_board_translation') > 0
  )
  AND ifNull(b.board_log, '') NOT LIKE '%%mastodon_board_translation%%'
  AND (
      ifNull(b.board_log, '') = ''
      OR position(b.board_log, 'mastodon_board_rss_fallback') > 0
      OR position(b.board_log, '"translation_status":"not_translated"') > 0
  )
ORDER BY r.status_created_at DESC
LIMIT %d
FORMAT JSONEachRow`, queryLimit)
	rows, err := cfg.queryJSONEachRow(query)
	if err != nil {
		return nil, err
	}
	events := make([]webREvent, 0, len(rows)+2)
	started := nowKST()
	events = append(events, newWebREvent("webr.mastodon.log.v1", "clickhouse://Data_R_Community_Raw.mastodon_status_raw", map[string]any{
		"uuid":          uuid7(),
		"created_at":    formatKST(started),
		"language_code": "en",
		"created_log": map[string]any{
			"type":          "mastodon_pipeline",
			"stage":         "board_backfill_started",
			"limit":         queryLimit,
			"source_method": "clickhouse_raw_to_translated_board",
		},
	}, started))
	published := 0
	for _, row := range rows {
		rowUUID := stringAny(row["uuid"])
		statusURL := stringAny(row["status_url"])
		statusID := stringAny(row["status_id"])
		statusCreated := parseKSTTime(stringAny(row["status_created_at"]), started)
		sourceTitle := firstWords(firstNonEmpty(stripTags(stringAny(row["content_html"])), stringAny(row["content_text"]), "R Foundation"), 16)
		sourceText := firstNonEmpty(stringAny(row["content_text"]), stripTags(stringAny(row["content_html"])), sourceTitle)
		title, content, err := translateMastodonStatus(ai, translationModel, sourceTitle, sourceText)
		if err != nil {
			events = append(events, newWebREvent("webr.mastodon.log.v1", statusURL, map[string]any{
				"uuid":          uuid7(),
				"created_at":    formatKST(nowKST()),
				"language_code": "en",
				"created_log": map[string]any{
					"type":          "mastodon_pipeline",
					"stage":         "board_backfill_translation_failed",
					"status_id":     statusID,
					"status_url":    statusURL,
					"error":         err.Error(),
					"source_method": "clickhouse_raw_to_translated_board",
				},
			}, nowKST()))
			continue
		}
		events = append(events, newWebREvent("webr.mastodon.board.v1", statusURL, mastodonBoardPayload(rowUUID, statusURL, statusID, statusCreated, nowKST(), title, content), statusCreated))
		published++
	}
	done := nowKST()
	events = append(events, newWebREvent("webr.mastodon.log.v1", "clickhouse://Data_R_Community_Raw.mastodon_status_raw", map[string]any{
		"uuid":          uuid7(),
		"created_at":    formatKST(done),
		"language_code": "en",
		"created_log": map[string]any{
			"type":          "mastodon_pipeline",
			"stage":         "board_backfill_done",
			"published":     published,
			"scanned":       len(rows),
			"source_method": "clickhouse_raw_to_translated_board",
		},
	}, done))
	return events, nil
}

func mastodonBoardPayload(rowUUID, statusURL, statusID string, createdAt, updatedAt time.Time, title, content string) map[string]any {
	title = cleanBoardTitle(title)
	if title == "" {
		title = "R Foundation"
	}
	content = safeMastodonBoardContent(title, content)
	var updated any
	if !updatedAt.IsZero() {
		updated = formatKST(updatedAt)
	}
	return map[string]any{
		"uuid":          rowUUID,
		"title":         title,
		"content":       content,
		"active":        1,
		"created_at":    formatKST(createdAt),
		"updated_at":    updated,
		"language_code": "ko",
		"created_log": map[string]any{
			"type":             "mastodon_board_translation",
			"source":           "Statground_Data_R-project",
			"source_method":    "mastodon_public_rss_no_api",
			"prompt_language":  "en",
			"target_language":  "ko",
			"hyperlinks":       "removed",
			"content_fallback": "title_when_blank",
			"raw_status_url":   statusURL,
			"raw_status_id":    statusID,
			"raw_created_at":   createdAt.UTC().Format(time.RFC3339Nano),
		},
		"updated_log": nil,
	}
}

func mastodonBoardPayloadWithSourceMethod(rowUUID, statusURL, statusID string, createdAt, updatedAt time.Time, title, content, sourceMethod string) map[string]any {
	payload := mastodonBoardPayload(rowUUID, statusURL, statusID, createdAt, updatedAt, title, content)
	createdLog := mapAny(payload["created_log"])
	createdLog["source_method"] = firstNonEmpty(sourceMethod, stringAny(createdLog["source_method"]))
	payload["created_log"] = createdLog
	return payload
}

func translateMastodonStatus(ai *aiClient, model, title, text string) (string, string, error) {
	if ai == nil || !ai.enabled() {
		return "", "", errors.New("AI provider key is not configured")
	}
	title = firstNonEmpty(title, firstWords(text, 12), "R Foundation")
	text = firstNonEmpty(text, title)
	translatedTitle := title
	var err error
	if !looksKorean(title, 0.20) {
		translatedTitle, err = ai.chat(mastodonTitlePrompt(title, text), model)
		if err != nil {
			return "", "", err
		}
	}
	translatedTitle = cleanBoardTitle(translatedTitle)
	if translatedTitle == "" {
		translatedTitle = cleanBoardTitle(title)
	}
	translatedContent := text
	if !looksKorean(text, 0.25) {
		translatedContent, err = ai.chat(mastodonContentPrompt(title, text), model)
		if err != nil {
			return "", "", err
		}
	}
	translatedContent, err = sanitizeBoardHTML(translatedContent)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(translatedContent) == "" {
		translatedContent = safeHTML(firstNonEmpty(translatedTitle, title))
	}
	return translatedTitle, safeMastodonBoardContent(translatedTitle, translatedContent), nil
}

func mastodonTitlePrompt(title, text string) string {
	return fmt.Sprintf(`You are a professional Korean translator for R Project community announcements.

Output rules:
- Return exactly one Korean title line.
- Do not include explanations, labels, quotes, Markdown, HTML, URLs, or hyperlinks.
- Preserve R, CRAN, package names, function names, version numbers, and proper nouns.
- Keep it concise and natural for a Korean Web-R board.

Source title:
%s

Source text:
%s`, title, truncateRunes(text, 1600))
}

func mastodonContentPrompt(title, text string) string {
	return fmt.Sprintf(`You are an editorial Korean translator for R Project community announcements.

Translate and lightly edit the source for a Korean Web-R community board post.

Output rules:
- Return only a compact HTML fragment. The first character must be "<".
- Never use <html>, <head>, or <body>.
- Allowed tags only: <h2>, <h3>, <p>, <ul>, <ol>, <li>, <strong>, <em>, <code>, <pre>, <blockquote>.
- Do not output hyperlinks, URLs, Markdown links, HTML <a> tags, href attributes, citations, source links, or "read more" links.
- Use polite formal Korean ending in ~합니다 or ~입니다.
- Preserve R, CRAN, package names, function names, code, numbers, and version strings.
- Do not add an introduction, explanation, label, or meta-commentary.

Source title:
%s

Source body:
%s`, title, truncateRunes(text, 7000))
}

func newAIClient(timeout time.Duration) *aiClient {
	keys := map[string]string{
		"openrouter":    strings.TrimSpace(os.Getenv("OPENROUTER_API_KEY")),
		"groq":          strings.TrimSpace(os.Getenv("GROQ_API_KEY")),
		"cerebras":      strings.TrimSpace(os.Getenv("CEREBRAS_API_KEY")),
		"github_models": strings.TrimSpace(os.Getenv("GH_MODELS_API_KEY")),
	}
	providers := make([]string, 0, 4)
	for _, provider := range []string{"openrouter", "groq", "cerebras", "github_models"} {
		if keys[provider] != "" {
			providers = append(providers, provider)
		}
	}
	return &aiClient{httpClient: &http.Client{Timeout: timeout}, keys: keys, providers: providers}
}

func (a *aiClient) enabled() bool {
	return a != nil && len(a.providers) > 0
}

func (a *aiClient) chat(prompt, model string) (string, error) {
	errs := make([]string, 0, len(a.providers))
	for _, provider := range a.providers {
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

func (a *aiClient) callProvider(provider, prompt, model string) (string, error) {
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
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 700))
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
		return stringAny(message["content"]), nil
	}
	return stringAny(first["text"]), nil
}

func (a *aiClient) providerRequest(provider, model string) (string, map[string]string, string, error) {
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

func fetchCRANPackages() ([]cranRecord, error) {
	return fetchDCF(cranPackagesURL())
}

func fetchDCF(sourceURL string) ([]cranRecord, error) {
	body, err := fetchBytes(sourceURL)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(strings.ToLower(sourceURL), ".gz") {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		body, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	}
	return parseDCF(string(body)), nil
}

func parseDCF(text string) []cranRecord {
	records := make([]cranRecord, 0)
	record := cranRecord{}
	current := ""
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			if len(record) > 0 {
				records = append(records, record)
				record = cranRecord{}
				current = ""
			}
			continue
		}
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') && current != "" {
			record[current] = record[current] + "\n" + strings.TrimSpace(line)
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		current = strings.TrimSpace(key)
		record[current] = strings.TrimSpace(value)
	}
	if len(record) > 0 {
		records = append(records, record)
	}
	return records
}

func cranlogsTop(limit int) ([]string, error) {
	var decoded any
	sourceURL := fmt.Sprintf("%s/top/last-week/%d", cranlogsBaseURL(), limit)
	if err := fetchJSON(sourceURL, &decoded); err != nil {
		return nil, err
	}
	out := make([]string, 0, limit)
	switch value := decoded.(type) {
	case map[string]any:
		for _, item := range anySlice(value["downloads"]) {
			if row, ok := item.(map[string]any); ok {
				name := stringAny(row["package"])
				if name != "" {
					out = append(out, name)
				}
			}
		}
	case []any:
		for _, item := range value {
			if row, ok := item.(map[string]any); ok {
				name := stringAny(row["package"])
				if name != "" {
					out = append(out, name)
				}
			}
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type dependency struct {
	name string
	spec string
}

func parseDependencies(value string) []dependency {
	out := make([]dependency, 0)
	for _, raw := range strings.Split(value, ",") {
		spec := strings.TrimSpace(raw)
		if spec == "" {
			continue
		}
		name := strings.TrimSpace(depVersionRE.ReplaceAllString(spec, ""))
		if parts := strings.Fields(name); len(parts) > 0 {
			name = parts[0]
		}
		if name != "" && name != "R" {
			out = append(out, dependency{name: name, spec: spec})
		}
	}
	return out
}

func repositoryURLs(record cranRecord) []string {
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, field := range []string{record["URL"], record["BugReports"]} {
		for _, part := range strings.FieldsFunc(field, func(r rune) bool { return r == ',' || r == ';' || r == '\n' || r == ' ' || r == '\t' }) {
			part = strings.TrimSpace(part)
			if strings.Contains(part, "github.com/") || strings.Contains(part, "gitlab.com/") || strings.Contains(part, "codeberg.org/") {
				if !strings.HasPrefix(part, "http") {
					part = "https://" + part
				}
				if !seen[part] {
					seen[part] = true
					out = append(out, part)
				}
			}
		}
	}
	return out
}

func descriptionURLs(record cranRecord) []string {
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, field := range []string{record["URL"], record["BugReports"]} {
		for _, raw := range boardURLRE.FindAllString(field, -1) {
			candidate := normalizeExternalURL(raw)
			if candidate == "" || seen[candidate] {
				continue
			}
			seen[candidate] = true
			out = append(out, candidate)
		}
	}
	return out
}

func normalizeExternalURL(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, ".,);]")
	if strings.HasPrefix(strings.ToLower(raw), "www.") {
		raw = "https://" + raw
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	parsed.Fragment = ""
	return parsed.String()
}

func cranTaskViewURLs(indexURL, htmlText string, limit int) []string {
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, match := range linkRE.FindAllStringSubmatch(htmlText, -1) {
		href := strings.TrimSpace(match[1])
		if !strings.HasSuffix(strings.ToLower(href), ".html") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimSpace(pathBase(href)), ".html")
		if name == "" || strings.EqualFold(name, "index") || strings.EqualFold(name, "views") {
			continue
		}
		viewURL := absoluteURL(indexURL, href)
		if seen[viewURL] {
			continue
		}
		seen[viewURL] = true
		out = append(out, viewURL)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func taskViewName(viewURL, htmlText string) string {
	name := strings.TrimSuffix(pathBase(viewURL), ".html")
	title := firstTitle(htmlText)
	title = strings.TrimSpace(strings.TrimPrefix(title, "CRAN Task View:"))
	if title != "" && !strings.EqualFold(title, "CRAN Task Views") {
		return title
	}
	return name
}

func taskViewPackages(htmlText string) []string {
	out := make([]string, 0)
	for _, match := range cranPackageLinkRE.FindAllStringSubmatch(htmlText, -1) {
		if len(match) < 2 {
			continue
		}
		packageName := strings.TrimSpace(html.UnescapeString(match[1]))
		if packageName != "" {
			out = append(out, packageName)
		}
	}
	return uniqueStrings(out)
}

func pathBase(raw string) string {
	if parsed, err := url.Parse(raw); err == nil {
		raw = parsed.Path
	}
	raw = strings.Trim(raw, "/")
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "/")
	return parts[len(parts)-1]
}

func packageMentionsInText(text string, packages []string) []string {
	lowerText := strings.ToLower(text)
	out := make([]string, 0)
	for _, packageName := range packages {
		packageName = strings.TrimSpace(packageName)
		if packageName == "" {
			continue
		}
		if strings.Contains(lowerText, strings.ToLower(packageName)) {
			out = append(out, packageName)
		}
	}
	return uniqueStrings(out)
}

func githubRepoRefs(records []cranRecord, limit int) []packageRepoRef {
	seen := map[string]bool{}
	out := make([]packageRepoRef, 0)
	for _, record := range records {
		packageName := record["Package"]
		if packageName == "" {
			continue
		}
		for _, repoURL := range repositoryURLs(record) {
			owner, repo, ok := parseGitHubRepo(repoURL)
			if !ok {
				continue
			}
			key := packageName + "|" + owner + "/" + repo
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, packageRepoRef{
				packageName: packageName,
				version:     record["Version"],
				repoURL:     repoURL,
				owner:       owner,
				repo:        repo,
			})
			if limit > 0 && len(out) >= limit {
				return out
			}
		}
	}
	return out
}

func parseGitHubRepo(raw string) (string, string, bool) {
	raw = strings.TrimSpace(strings.TrimSuffix(raw, ".git"))
	match := githubRepoRE.FindStringSubmatch(raw)
	if len(match) < 3 {
		return "", "", false
	}
	owner := strings.TrimSpace(match[1])
	repo := strings.TrimSpace(match[2])
	repo = strings.TrimSuffix(repo, ".git")
	repo = strings.Trim(repo, " .")
	if owner == "" || repo == "" {
		return "", "", false
	}
	if strings.EqualFold(repo, "issues") || strings.EqualFold(repo, "pull") || strings.EqualFold(repo, "releases") {
		return "", "", false
	}
	return owner, repo, true
}

func htmlCells(rowHTML string) []string {
	matches := cellRE.FindAllStringSubmatch(rowHTML, -1)
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		out = append(out, stripTags(match[1]))
	}
	return out
}

func worstStatus(cells []string) string {
	upper := strings.ToUpper(strings.Join(cells, " "))
	for _, status := range statusOrder {
		if strings.Contains(upper, status) {
			return status
		}
	}
	return "UNKNOWN"
}

func fetchWebsitePayload(targetURL string) map[string]any {
	body, err := fetchBytes(targetURL)
	if err != nil {
		return map[string]any{
			"target_url":        targetURL,
			"url_hash":          shaHex(targetURL),
			"host":              hostFromURL(targetURL),
			"status_code":       0,
			"content_type":      "",
			"title":             "",
			"description":       "",
			"canonical_url":     targetURL,
			"feed_urls":         []string{},
			"link_urls":         []string{},
			"youtube_urls":      []string{},
			"error_code":        fmt.Sprintf("%T", err),
			"page_text":         "",
			"source_method":     "html_fetch",
			"collection_status": "failed",
		}
	}
	text := string(body)
	meta := metaValues(text)
	pageText := stripTags(text)
	feeds := extractFeedLinks(targetURL, text)
	links := extractPageLinks(targetURL, text)
	youtubeLinks := extractYouTubeURLs(text)
	return map[string]any{
		"target_url":        targetURL,
		"url_hash":          shaHex(targetURL),
		"host":              hostFromURL(targetURL),
		"status_code":       200,
		"content_type":      "text/html",
		"title":             firstNonEmpty(meta["og:title"], firstTitle(text)),
		"description":       firstNonEmpty(meta["description"], meta["og:description"]),
		"canonical_url":     firstNonEmpty(meta["canonical"], targetURL),
		"feed_urls":         feeds,
		"link_urls":         links,
		"youtube_urls":      youtubeLinks,
		"og_image":          meta["og:image"],
		"error_code":        "",
		"page_text":         truncate(pageText, 20000),
		"source_method":     "html_fetch_no_api",
		"collection_status": "collected",
	}
}

func metaValues(text string) map[string]string {
	out := map[string]string{}
	for _, match := range metaRE.FindAllStringSubmatch(text, -1) {
		attrs := parseAttrs(match[1])
		key := strings.ToLower(firstNonEmpty(attrs["name"], attrs["property"]))
		if key != "" {
			out[key] = html.UnescapeString(attrs["content"])
		}
	}
	for _, match := range linkRE.FindAllStringSubmatch(text, -1) {
		if strings.Contains(strings.ToLower(match[0]), `rel="canonical"`) || strings.Contains(strings.ToLower(match[0]), `rel='canonical'`) {
			out["canonical"] = strings.TrimSpace(match[1])
		}
	}
	return out
}

func parseAttrs(value string) map[string]string {
	out := map[string]string{}
	for _, match := range attrRE.FindAllStringSubmatch(value, -1) {
		attrValue := firstNonEmpty(match[3], match[4], match[5])
		out[strings.ToLower(match[1])] = attrValue
	}
	return out
}

func extractFeedLinks(base, text string) []string {
	out := make([]string, 0)
	for _, match := range linkRE.FindAllStringSubmatch(text, -1) {
		raw := strings.TrimSpace(match[1])
		lower := strings.ToLower(match[0])
		if raw != "" && (strings.Contains(lower, "rss") || strings.Contains(lower, "atom") || strings.Contains(lower, "alternate")) {
			out = append(out, absoluteURL(base, raw))
		}
	}
	return firstNStrings(uniqueStrings(out), 20)
}

func extractPageLinks(base, text string) []string {
	out := make([]string, 0)
	for _, match := range linkRE.FindAllStringSubmatch(text, -1) {
		raw := strings.TrimSpace(html.UnescapeString(match[1]))
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		lower := strings.ToLower(raw)
		if strings.HasPrefix(lower, "mailto:") || strings.HasPrefix(lower, "javascript:") || strings.HasPrefix(lower, "tel:") {
			continue
		}
		resolved := absoluteURL(base, raw)
		if strings.HasPrefix(resolved, "http://") || strings.HasPrefix(resolved, "https://") {
			out = append(out, resolved)
		}
	}
	return firstNStrings(uniqueStrings(out), maxInt(1, envInt("RPKG_R_WEBSITE_PAGE_LINK_LIMIT", 80)))
}

func extractYouTubeURLs(text string) []string {
	out := make([]string, 0)
	for _, match := range youtubeURLRE.FindAllString(text, -1) {
		out = append(out, strings.TrimRight(match, ".,);]"))
	}
	return firstNStrings(uniqueStrings(out), 80)
}

func parseFeedItems(feedURL string, body []byte) []map[string]string {
	var rss rssFeed
	if err := xml.Unmarshal(body, &rss); err == nil && len(rss.Channel.Items) > 0 {
		out := make([]map[string]string, 0, len(rss.Channel.Items))
		for _, item := range rss.Channel.Items {
			itemURL := firstNonEmpty(item.Link, item.GUID)
			published := parseRSSDate(item.PubDate, time.Now()).UTC().Format("2006-01-02T15:04:05.000Z")
			summaryHTML := strings.TrimSpace(item.Description)
			out = append(out, map[string]string{
				"item_id":       firstNonEmpty(item.GUID, itemURL, shaHex(item.Title+item.PubDate)),
				"item_title":    stripTags(item.Title),
				"item_url":      absoluteURL(feedURL, itemURL),
				"published_at":  published,
				"summary_text":  truncate(stripTags(summaryHTML), 4000),
				"summary_html":  truncate(summaryHTML, 8000),
				"source_method": "rss_feed_item_xml",
			})
		}
		return out
	}
	var atom atomFeed
	if err := xml.Unmarshal(body, &atom); err == nil && len(atom.Entries) > 0 {
		out := make([]map[string]string, 0, len(atom.Entries))
		for _, entry := range atom.Entries {
			itemURL := atomEntryURL(feedURL, entry)
			publishedRaw := firstNonEmpty(entry.Published, entry.Updated)
			published := parseRSSDate(publishedRaw, time.Now()).UTC().Format("2006-01-02T15:04:05.000Z")
			summaryHTML := firstNonEmpty(entry.Summary, entry.Content)
			out = append(out, map[string]string{
				"item_id":       firstNonEmpty(entry.ID, itemURL, shaHex(entry.Title+publishedRaw)),
				"item_title":    stripTags(entry.Title),
				"item_url":      itemURL,
				"published_at":  published,
				"summary_text":  truncate(stripTags(summaryHTML), 4000),
				"summary_html":  truncate(summaryHTML, 8000),
				"source_method": "atom_feed_item_xml",
			})
		}
		return out
	}
	return nil
}

func atomEntryURL(feedURL string, entry atomEntry) string {
	for _, link := range entry.Links {
		rel := strings.ToLower(strings.TrimSpace(link.Rel))
		if link.Href != "" && (rel == "" || rel == "alternate") {
			return absoluteURL(feedURL, link.Href)
		}
	}
	if entry.ID != "" && strings.HasPrefix(entry.ID, "http") {
		return entry.ID
	}
	return feedURL
}

func sitemapURLFor(targetURL string) string {
	parsed, err := url.Parse(targetURL)
	if err != nil || parsed.Host == "" {
		return ""
	}
	parsed.Path = "/sitemap.xml"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func sitemapURLs(body []byte) []string {
	out := make([]string, 0)
	for _, match := range sitemapLocRE.FindAllStringSubmatch(string(body), -1) {
		pageURL := strings.TrimSpace(html.UnescapeString(match[1]))
		if strings.HasPrefix(pageURL, "http://") || strings.HasPrefix(pageURL, "https://") {
			out = append(out, pageURL)
		}
	}
	return firstNStrings(uniqueStrings(out), maxInt(1, envInt("RPKG_R_WEBSITE_SITEMAP_URLS_PER_SITE", 30)))
}

func normalizeYouTubeSeed(seed map[string]any) map[string]any {
	payload := map[string]any{}
	for key, value := range seed {
		payload[key] = value
	}
	ref := parseYouTubeRef(stringAny(payload["url"]))
	for key, value := range ref {
		payload[key] = value
	}
	if stringAny(payload["source_method"]) == "" {
		payload["source_method"] = "clickhouse_seed_current"
	}
	payload["collection_status"] = "seeded_from_db"
	return payload
}

func parseYouTubeRef(raw string) map[string]string {
	out := map[string]string{
		"parsed_ref_type":    "",
		"parsed_video_id":    "",
		"parsed_playlist_id": "",
		"parsed_channel_id":  "",
		"parsed_handle":      "",
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return out
	}
	host := strings.ToLower(parsed.Hostname())
	path := strings.Trim(parsed.Path, "/")
	if strings.Contains(host, "youtu.be") && path != "" {
		out["parsed_ref_type"] = "video"
		out["parsed_video_id"] = strings.Split(path, "/")[0]
		return out
	}
	if v := parsed.Query().Get("v"); v != "" {
		out["parsed_ref_type"] = "video"
		out["parsed_video_id"] = v
	}
	if list := parsed.Query().Get("list"); list != "" {
		out["parsed_playlist_id"] = list
		if out["parsed_ref_type"] == "" {
			out["parsed_ref_type"] = "playlist"
		}
	}
	parts := strings.Split(path, "/")
	if len(parts) >= 2 && (parts[0] == "shorts" || parts[0] == "live") {
		out["parsed_ref_type"] = "video"
		out["parsed_video_id"] = parts[1]
	}
	if len(parts) >= 2 && parts[0] == "channel" {
		out["parsed_ref_type"] = "channel"
		out["parsed_channel_id"] = parts[1]
	}
	if len(parts) >= 1 && strings.HasPrefix(parts[0], "@") {
		out["parsed_ref_type"] = "handle"
		out["parsed_handle"] = strings.TrimPrefix(parts[0], "@")
	}
	if out["parsed_ref_type"] == "" && strings.Contains(path, "results") {
		out["parsed_ref_type"] = "search"
	}
	return out
}

func needsYouTubeMetadataFill(payload map[string]any) bool {
	return isBadYouTubeTitleValue(payload["video_title"]) ||
		isBadYouTubeMetadataValue(payload["video_description"]) ||
		isBadYouTubeMetadataValue(payload["thumbnail_url"]) ||
		stringAny(payload["duration_seconds"]) == "0" ||
		stringAny(payload["view_count"]) == "0"
}

func mergePayload(dst map[string]any, src map[string]any) {
	for key, value := range src {
		if isZeroPayloadValue(value) {
			continue
		}
		if isZeroPayloadValue(dst[key]) {
			dst[key] = value
			continue
		}
		switch key {
		case "video_title":
			if !isBadYouTubeTitleValue(value) {
				dst[key] = cleanYouTubeTitleValue(value)
			}
		case "video_description":
			if !isBadYouTubeMetadataValue(value) {
				dst[key] = value
			}
		case "thumbnail_url", "youtube_channel_id", "channel_title", "published_at", "privacy_status":
			if !isBadYouTubeMetadataValue(value) {
				dst[key] = value
			}
		case "duration_seconds", "view_count", "like_count", "comment_count", "favorite_count", "caption_available":
			if intAny(value) > intAny(dst[key]) {
				dst[key] = value
			}
		case "tags_json", "thumbnail_urls_json", "default_audio_language", "default_language", "language_code":
			if stringAny(value) != "" && stringAny(value) != "[]" && stringAny(value) != "{}" && stringAny(value) != "und" {
				dst[key] = value
			}
		default:
			dst[key] = value
		}
	}
}

func isZeroPayloadValue(value any) bool {
	text := strings.TrimSpace(stringAny(value))
	return text == "" || text == "0" || text == "[]" || text == "{}" || text == "<nil>" || text == youtubeBoilerplateDescription
}

func isBadYouTubeMetadataValue(value any) bool {
	text := strings.TrimSpace(stringAny(value))
	return text == "" || text == youtubeBoilerplateDescription
}

func isBadYouTubeTitleValue(value any) bool {
	text := cleanYouTubeTitleValue(value)
	lower := strings.ToLower(text)
	return isBadYouTubeMetadataValue(value) ||
		lower == "youtube" ||
		lower == "- youtube" ||
		strings.HasPrefix(text, "YouTube video ")
}

func cleanYouTubeTitleValue(value any) string {
	text := strings.TrimSpace(stringAny(value))
	if strings.HasSuffix(text, " - YouTube") {
		text = strings.TrimSpace(strings.TrimSuffix(text, " - YouTube"))
	}
	return text
}

func finalizeYouTubeVideoPayload(payload map[string]any) {
	videoID := stringAny(payload["youtube_video_id"])
	sourceTag := firstNonEmpty(stringAny(payload["source_tag"]), "r_project_ecosystem_youtube")
	uuidArticle := stringAny(payload["uuid_article"])
	if stringAny(payload["stable_uuid"]) == "" && videoID != "" {
		payload["stable_uuid"] = stableYouTubeVideoUUID(videoID, sourceTag, uuidArticle)
	}
	if stringAny(payload["active"]) == "" {
		payload["active"] = "1"
	}
	if cleanedTitle := cleanYouTubeTitleValue(payload["video_title"]); cleanedTitle != stringAny(payload["video_title"]) {
		payload["video_title"] = cleanedTitle
	}
	if isBadYouTubeTitleValue(payload["video_title"]) && videoID != "" {
		payload["video_title"] = "YouTube video " + videoID
	}
	if isBadYouTubeMetadataValue(payload["thumbnail_url"]) && videoID != "" {
		payload["thumbnail_url"] = "https://i.ytimg.com/vi/" + videoID + "/hqdefault.jpg"
	}
	if isBadYouTubeMetadataValue(payload["video_description"]) {
		payload["video_description"] = firstNonEmpty(stringAny(payload["video_title"]), videoID)
	}
	if stringAny(payload["thumbnail_urls_json"]) == "{}" && !isBadYouTubeMetadataValue(payload["thumbnail_url"]) {
		payload["thumbnail_urls_json"] = mustJSON(map[string]any{"hqdefault": map[string]any{"url": stringAny(payload["thumbnail_url"])}})
	}
}

func stableYouTubeVideoUUID(videoID, sourceTag, uuidArticle string) string {
	return deterministicUUID("youtube-video:" + videoID + ":" + sourceTag + ":" + uuidArticle)
}

func youtubeDLBinary() (string, error) {
	configured := firstNonEmpty(os.Getenv("YOUTUBE_DL_BIN"), os.Getenv("YTDLP_BIN"))
	candidates := []string{}
	if configured != "" {
		candidates = append(candidates, configured)
	}
	candidates = append(candidates, "yt-dlp", "youtube-dl")
	for _, candidate := range candidates {
		if path, err := exec.LookPath(candidate); err == nil {
			return path, nil
		}
	}
	return "", errors.New("yt-dlp or youtube-dl binary was not found")
}

func bestYouTubeAPIThumbnail(thumbnails map[string]any) (string, string) {
	order := []string{"maxres", "standard", "high", "medium", "default"}
	out := map[string]any{}
	best := ""
	for _, key := range order {
		row := mapAny(thumbnails[key])
		if len(row) == 0 {
			continue
		}
		out[key] = row
		if best == "" {
			best = stringAny(row["url"])
		}
	}
	return best, mustJSON(out)
}

func bestYTDLPThumbnail(thumbnails []any) string {
	bestURL := ""
	bestArea := int64(-1)
	for _, item := range thumbnails {
		row := mapAny(item)
		rawURL := stringAny(row["url"])
		if rawURL == "" {
			continue
		}
		area := intAny(row["width"]) * intAny(row["height"])
		if area >= bestArea {
			bestArea = area
			bestURL = rawURL
		}
	}
	return bestURL
}

func parseYTDLPPrintedMetadata(text string) (map[string]any, error) {
	line := strings.TrimRight(text, "\r\n")
	fields := strings.Split(line, "\t")
	keys := []string{
		"id",
		"title",
		"description",
		"channel_id",
		"channel",
		"timestamp",
		"upload_date",
		"duration",
		"view_count",
		"like_count",
		"comment_count",
		"language",
		"tags",
		"thumbnails",
		"availability",
		"webpage_url",
	}
	if len(fields) != len(keys) {
		return nil, fmt.Errorf("yt-dlp printed %d fields, expected %d", len(fields), len(keys))
	}
	out := map[string]any{}
	for i, key := range keys {
		out[key] = decodeYTDLPPrintedField(fields[i])
	}
	return out, nil
}

func decodeYTDLPPrintedField(value string) any {
	value = strings.TrimSpace(value)
	if value == "" || value == "NA" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(value), &decoded); err == nil {
		return decoded
	}
	return value
}

func ytdlpPrintedPublishedAt(decoded map[string]any) string {
	if ts := intAny(decoded["timestamp"]); ts > 0 {
		return time.Unix(ts, 0).UTC().Format(time.RFC3339)
	}
	if uploadDate := stringAny(decoded["upload_date"]); len(uploadDate) == 8 {
		if parsed, err := time.Parse("20060102", uploadDate); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
	}
	return ""
}

func ytdlpPlaylistIDsJSON(decoded map[string]any) string {
	ids := []string{}
	for _, key := range []string{"playlist_id", "playlist"} {
		if value := stringAny(decoded[key]); value != "" {
			ids = append(ids, value)
		}
	}
	return mustJSON(uniqueStrings(ids))
}

func ytdlpPublishedAt(decoded map[string]any) string {
	for _, key := range []string{"timestamp", "release_timestamp", "modified_timestamp"} {
		if ts := intAny(decoded[key]); ts > 0 {
			return time.Unix(ts, 0).UTC().Format(time.RFC3339)
		}
	}
	if uploadDate := stringAny(decoded["upload_date"]); len(uploadDate) == 8 {
		if parsed, err := time.Parse("20060102", uploadDate); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
	}
	return ""
}

func parseISO8601DurationSeconds(value string) int64 {
	match := isoDurationRE.FindStringSubmatch(strings.TrimSpace(value))
	if len(match) == 0 {
		return 0
	}
	hours, _ := strconv.ParseInt(firstNonEmpty(match[1], "0"), 10, 64)
	minutes, _ := strconv.ParseInt(firstNonEmpty(match[2], "0"), 10, 64)
	seconds, _ := strconv.ParseInt(firstNonEmpty(match[3], "0"), 10, 64)
	return hours*3600 + minutes*60 + seconds
}

func youtubeMetadataPackageMentionEvents(videoID string, payload map[string]any) []genericEvent {
	packages := splitCSV(envString("R_YOUTUBE_MENTION_PACKAGES", "ggplot2,dplyr,shiny,tidymodels,quarto,data.table,tidyverse,knitr,rmarkdown,caret,randomForest,xgboost,survival"))
	if len(packages) == 0 {
		return nil
	}
	sources := map[string]string{
		"title":       stringAny(payload["video_title"]),
		"description": stringAny(payload["video_description"]),
		"tag":         strings.Join(anyStringSliceFromJSON(stringAny(payload["tags_json"])), " "),
	}
	events := make([]genericEvent, 0)
	for _, packageName := range packages {
		needle := strings.ToLower(packageName)
		for sourceName, text := range sources {
			if text == "" || !strings.Contains(strings.ToLower(text), needle) {
				continue
			}
			matchText := mentionContext(text, packageName, 240)
			confidence := "medium"
			score := "0.65"
			if sourceName == "title" || strings.Contains(strings.ToLower(matchText), "r package "+needle) {
				confidence = "high"
				score = "0.85"
			}
			events = append(events, newGenericEvent("r.youtube.package.mention.v1", "youtube_metadata_mention_extractor", stringAny(payload["canonical_url"]), "CRAN", packageName, "", stringAny(payload["published_at"]), map[string]any{
				"youtube_video_id":  videoID,
				"package":           packageName,
				"mention_source":    sourceName,
				"language_code":     firstNonEmpty(stringAny(payload["language_code"]), "und"),
				"segment_start_ms":  "0",
				"segment_end_ms":    "0",
				"match_text":        matchText,
				"confidence":        confidence,
				"confidence_score":  score,
				"extractor_version": "rpkg-youtube-metadata-mention-v1",
				"source_method":     "title_description_tag_scan",
				"collection_status": "collected",
			}))
		}
	}
	return events
}

func youtubeTextPackageMentionEvents(videoID, canonicalURL, sourceName, languageCode, text string, startMS, endMS int64, observedAt, sourceMethod, extractorVersion, confidenceScore string) []genericEvent {
	packages := splitCSV(envString("R_YOUTUBE_MENTION_PACKAGES", "ggplot2,dplyr,shiny,tidymodels,quarto,data.table,tidyverse,knitr,rmarkdown,caret,randomForest,xgboost,survival"))
	if len(packages) == 0 || strings.TrimSpace(text) == "" {
		return nil
	}
	events := make([]genericEvent, 0)
	lowerText := strings.ToLower(text)
	for _, packageName := range packages {
		packageName = strings.TrimSpace(packageName)
		if packageName == "" || !strings.Contains(lowerText, strings.ToLower(packageName)) {
			continue
		}
		events = append(events, newGenericEvent("r.youtube.package.mention.v1", sourceMethod, canonicalURL, "CRAN", packageName, "", observedAt, map[string]any{
			"youtube_video_id":  videoID,
			"package":           packageName,
			"mention_source":    sourceName,
			"language_code":     firstNonEmpty(languageCode, "und"),
			"segment_start_ms":  intString(startMS),
			"segment_end_ms":    intString(endMS),
			"match_text":        mentionContext(text, packageName, 240),
			"confidence":        "medium",
			"confidence_score":  firstNonEmpty(confidenceScore, "0.60"),
			"extractor_version": firstNonEmpty(extractorVersion, "rpkg-youtube-text-mention-v1"),
			"source_method":     sourceMethod,
			"collection_status": "collected",
		}))
	}
	return events
}

func mentionContext(text, needle string, limit int) string {
	runes := []rune(text)
	lowerRunes := []rune(strings.ToLower(text))
	needleRunes := []rune(strings.ToLower(needle))
	idx := -1
	for i := 0; i+len(needleRunes) <= len(lowerRunes); i++ {
		if string(lowerRunes[i:i+len(needleRunes)]) == string(needleRunes) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return truncateRunes(text, limit)
	}
	start := idx - limit/2
	if start < 0 {
		start = 0
	}
	end := idx + len(needleRunes) + limit/2
	if end > len(runes) {
		end = len(runes)
	}
	return strings.TrimSpace(string(runes[start:end]))
}

func anyStringSliceFromJSON(value string) []string {
	var arr []any
	if err := json.Unmarshal([]byte(value), &arr); err != nil {
		return nil
	}
	return anyStringSlice(arr)
}

func newPublisher(topic, clientID string, dryRun bool) *publisher {
	return &publisher{
		topic:              topic,
		brokers:            splitCSV(firstNonEmpty(os.Getenv("KAFKA_BROKERS"), os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))),
		username:           firstNonEmpty(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_EXTERNAL_USER")),
		password:           firstNonEmpty(os.Getenv("KAFKA_PASSWORD"), os.Getenv("KAFKA_EXTERNAL_PASSWORD")),
		security:           envString("KAFKA_SECURITY_PROTOCOL", ""),
		clientID:           envString("KAFKA_CLIENT_ID", clientID),
		dryRun:             dryRun,
		publishMode:        normalizePublishMode(envString("RPROJECT_PUBLISH_MODE", envString("R_DATA_PUBLISH_MODE", "clickhouse"))),
		writeTimeout:       time.Duration(maxInt(1, envInt("KAFKA_WRITE_TIMEOUT", envInt("KAFKA_WRITE_TIMEOUT_SECONDS", 60)))) * time.Second,
		chunkSize:          maxInt(1, envInt("KAFKA_WRITE_CHUNK_SIZE", 100)),
		createTopic:        envBool("KAFKA_CREATE_TOPIC", envBool("KAFKA_ALLOW_TOPIC_CREATE", true)),
		partitions:         maxInt(1, envInt("KAFKA_TOPIC_PARTITIONS", 3)),
		replicas:           maxInt(1, envInt("KAFKA_TOPIC_REPLICATION_FACTOR", 1)),
		writerMaxAttempts:  maxInt(1, envInt("KAFKA_WRITER_MAX_ATTEMPTS", 1)),
		writeAttempts:      maxInt(1, envInt("KAFKA_WRITE_ATTEMPTS", 3)),
		writeBackoffMin:    time.Duration(envFloat("KAFKA_WRITE_BACKOFF_MIN", envFloat("KAFKA_WRITE_BACKOFF_MIN_SECONDS", 1.0)) * float64(time.Second)),
		writeBackoffMax:    time.Duration(envFloat("KAFKA_WRITE_BACKOFF_MAX", envFloat("KAFKA_WRITE_BACKOFF_MAX_SECONDS", 12.0)) * float64(time.Second)),
		partitionFallback:  envBool("KAFKA_PARTITION_FALLBACK_ENABLED", true),
		fallbackPartitions: splitIntCSV(envString("KAFKA_FALLBACK_PARTITIONS", "")),
		fallbackTimeout:    time.Duration(envFloat("KAFKA_PARTITION_FALLBACK_TIMEOUT_SECONDS", 8.0) * float64(time.Second)),
	}
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

func (p *publisher) usesClickHouse() bool {
	return p.publishMode == "clickhouse" || p.publishMode == "dual"
}

func (p *publisher) usesKafka() bool {
	return p.publishMode == "kafka" || p.publishMode == "dual"
}

func (p *publisher) validate(ctx context.Context) error {
	if p.dryRun || !p.usesKafka() {
		return nil
	}
	if p.topic == "" {
		return errors.New("Kafka topic is required")
	}
	if len(p.brokers) == 0 {
		return errors.New("KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS is required")
	}
	for _, broker := range p.brokers {
		if isLoopbackBroker(broker) {
			return fmt.Errorf("Kafka broker %q is not reachable from GitHub Actions", broker)
		}
	}
	dialer := p.dialer()
	probeCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	partitions, err := p.readPartitions(probeCtx, dialer)
	if err != nil {
		if p.createTopic && kafkaTopicMissing(err) {
			if createErr := p.createKafkaTopic(probeCtx, dialer); createErr != nil {
				fmt.Printf("[kafka] topic_create_deferred topic=%s err=%v\n", p.topic, createErr)
			}
			partitions, err = p.readPartitions(probeCtx, dialer)
			if err != nil && kafkaTopicMissing(err) {
				fmt.Printf("[kafka] topic_metadata_missing topic=%s auto_topic_creation=true\n", p.topic)
				return nil
			}
		}
		if err != nil {
			return fmt.Errorf("kafka metadata read failed topic=%s: %w", p.topic, err)
		}
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka metadata found zero partitions topic=%s", p.topic)
	}
	if err := validateKafkaAdvertisedLeaders(partitions, p.brokers, "kafka metadata"); err != nil {
		return err
	}
	p.knownPartitions = partitionIDsForTopic(partitions, p.topic)
	return nil
}

func (p *publisher) dialer() *kafka.Dialer {
	dialer := &kafka.Dialer{
		ClientID: p.clientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(p.brokers, 10*time.Second),
	}
	if p.username != "" || p.password != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: p.username, Password: p.password}
	}
	if p.usesTLS() {
		dialer.TLS = kafkaTLSConfig()
	}
	return dialer
}

func (p *publisher) readPartitions(ctx context.Context, dialer *kafka.Dialer) ([]kafka.Partition, error) {
	conn, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafka preflight failed for %q: %w", p.brokers[0], err)
	}
	defer conn.Close()
	return conn.ReadPartitions(p.topic)
}

func (p *publisher) createKafkaTopic(ctx context.Context, dialer *kafka.Dialer) error {
	conn, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return err
	}
	controller, err := conn.Controller()
	conn.Close()
	if err != nil {
		return err
	}
	controllerConn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()
	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             p.topic,
		NumPartitions:     p.partitions,
		ReplicationFactor: p.replicas,
	})
	if err != nil && !kafkaTopicAlreadyExists(err) {
		return err
	}
	fmt.Printf("[kafka] topic_ready topic=%s partitions=%d replicas=%d\n", p.topic, p.partitions, p.replicas)
	return nil
}

func (p *publisher) publishGeneric(ctx context.Context, events []genericEvent) error {
	if p.dryRun {
		for _, event := range events {
			body, err := json.Marshal(event)
			if err != nil {
				return err
			}
			fmt.Println(string(body))
		}
		return nil
	}
	if p.usesClickHouse() {
		target, err := insertGenericRawEventsDirect(ctx, events)
		if err != nil {
			return fmt.Errorf("ClickHouse direct publish failed target=%s: %s", target.label, publicClickHouseError(err))
		}
		if len(events) > 0 {
			fmt.Printf("[clickhouse] direct publish succeeded target=%s table=%s events=%d\n", target.label, target.table, len(events))
		}
		if !p.usesKafka() {
			return nil
		}
	}
	fallbackEnabled := p.packageClickHouseFallbackEnabled(events)
	directFallback := false
	for _, chunk := range chunkGenericEvents(events, p.chunkSize) {
		if directFallback {
			if err := insertPackageRawEventsFallback(ctx, chunk); err != nil {
				return fmt.Errorf("ClickHouse package raw fallback failed: %s", publicClickHouseError(err))
			}
			continue
		}
		messages, err := genericEventsToMessages(chunk)
		if err != nil {
			return err
		}
		if err := p.writeMessagesWithRetry(ctx, messages); err != nil {
			if fallbackEnabled && shouldUsePackageClickHouseFallback(err, len(chunk)) {
				if fallbackErr := insertPackageRawEventsFallback(ctx, chunk); fallbackErr != nil {
					return fmt.Errorf("kafka publish failed and ClickHouse package raw fallback failed: %s; original_error=%s", publicClickHouseError(fallbackErr), shortKafkaError(err))
				}
				fmt.Printf("[clickhouse] package raw fallback succeeded events=%d reason=%s\n", len(chunk), kafkaRetryReason(err))
				directFallback = true
				continue
			}
			if fallbackEnabled {
				failedEvents, failedErr := failedPackageEventsFromKafkaError(err)
				if failedErr != nil {
					return fmt.Errorf("kafka publish failed and failed-message extraction failed: %w; original_error=%s", failedErr, shortKafkaError(err))
				}
				if len(failedEvents) > 0 {
					if fallbackErr := insertPackageRawEventsFallback(ctx, failedEvents); fallbackErr != nil {
						return fmt.Errorf("partial kafka publish failed and ClickHouse package raw fallback failed: %s; original_error=%s", publicClickHouseError(fallbackErr), shortKafkaError(err))
					}
					fmt.Printf("[clickhouse] package raw partial fallback succeeded events=%d reason=%s\n", len(failedEvents), kafkaRetryReason(err))
					if len(failedEvents) == len(chunk) {
						directFallback = true
					}
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (p *publisher) packageClickHouseFallbackEnabled(events []genericEvent) bool {
	if p.dryRun || len(events) == 0 || !envBool("RPKG_CLICKHOUSE_FALLBACK_ENABLED", false) {
		return false
	}
	for _, event := range events {
		if !strings.HasPrefix(event.EventType, "rpkg.") {
			return false
		}
	}
	return true
}

func shouldUsePackageClickHouseFallback(err error, eventCount int) bool {
	if err == nil || eventCount <= 0 {
		return false
	}
	reason := kafkaRetryReason(err)
	if reason != "leader-metadata-stale" && reason != "leader-not-available" && reason != "network" && reason != "timeout" && reason != "temporary-kafka-error" {
		return false
	}
	return kafkaErrorFailedAllMessages(err, eventCount)
}

func shouldDeferPackagePublishFailure(err error) bool {
	if err == nil || !envBool("RPKG_PUBLISH_TRANSIENT_FAIL_OPEN", false) {
		return false
	}
	message := strings.ToLower(err.Error())
	if isKafkaAuthOrPermissionErrorText(message) ||
		strings.Contains(message, "clickhouse-auth") ||
		strings.Contains(message, "clickhouse-permission") ||
		strings.Contains(message, "kafka-auth") ||
		strings.Contains(message, "not authorized") ||
		strings.Contains(message, "authorization failed") {
		return false
	}
	reason := kafkaRetryReason(err)
	return reason == "leader-metadata-stale" ||
		reason == "leader-not-available" ||
		reason == "network" ||
		reason == "timeout" ||
		reason == "temporary-kafka-error" ||
		strings.Contains(message, "clickhouse-timeout") ||
		strings.Contains(message, "clickhouse-network") ||
		strings.Contains(message, "clickhouse-server-error")
}

func packagePublishFailureReason(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	chReason := ""
	switch {
	case strings.Contains(msg, "clickhouse-timeout"):
		chReason = "clickhouse-timeout"
	case strings.Contains(msg, "clickhouse-network"):
		chReason = "clickhouse-network"
	case strings.Contains(msg, "clickhouse-server-error"):
		chReason = "clickhouse-server-error"
	}
	kReason := kafkaRetryReason(err)
	if chReason == "" {
		return kReason
	}
	return kReason + "+" + chReason
}

func kafkaErrorFailedAllMessages(err error, eventCount int) bool {
	if err == nil || eventCount <= 0 {
		return false
	}
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		return writeErrs.Count() == eventCount
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, fmt.Sprintf("kafka write errors (%d/%d)", eventCount, eventCount)) ||
		hasCountField(msg, "failed_messages", eventCount) ||
		(!strings.Contains(msg, "kafka write errors") && !strings.Contains(msg, "failed_messages="))
}

func hasCountField(message, name string, count int) bool {
	token := fmt.Sprintf("%s=%d", name, count)
	start := strings.Index(message, token)
	if start < 0 {
		return false
	}
	end := start + len(token)
	if end >= len(message) {
		return true
	}
	next := message[end]
	return next < '0' || next > '9'
}

func genericEventsToMessages(events []genericEvent) ([]kafka.Message, error) {
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		messages = append(messages, kafka.Message{Key: []byte(eventKey(event)), Value: body, Time: time.Now()})
	}
	return messages, nil
}

type genericDirectTarget struct {
	label          string
	table          string
	includePackage bool
}

func insertGenericRawEventsDirect(ctx context.Context, events []genericEvent) (genericDirectTarget, error) {
	target, err := genericEventsDirectTarget(events)
	if err != nil || len(events) == 0 {
		return target, err
	}
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return target, err
	}
	chunkSize := maxInt(1, envInt("RPROJECT_CLICKHOUSE_CHUNK_SIZE", envInt("RPKG_CLICKHOUSE_FALLBACK_CHUNK_SIZE", minInt(envInt("KAFKA_WRITE_CHUNK_SIZE", 100), 10))))
	for _, chunk := range chunkGenericEvents(events, chunkSize) {
		if err := insertGenericRawEventChunkWithSplit(ctx, cfg, target, chunk); err != nil {
			return target, err
		}
	}
	return target, nil
}

func genericEventsDirectTarget(events []genericEvent) (genericDirectTarget, error) {
	if len(events) == 0 {
		return genericDirectTarget{}, nil
	}
	target := genericDirectTarget{}
	for _, event := range events {
		next, err := genericEventDirectTarget(event)
		if err != nil {
			return target, err
		}
		if target.table == "" {
			target = next
			continue
		}
		if target.table != next.table {
			return target, fmt.Errorf("mixed direct ClickHouse targets are not supported: %s and %s", target.table, next.table)
		}
	}
	return target, nil
}

func genericEventDirectTarget(event genericEvent) (genericDirectTarget, error) {
	switch {
	case strings.HasPrefix(event.EventType, "rpkg."):
		return genericDirectTarget{label: "package", table: "Data_R_Package_Raw.r_package_event_raw", includePackage: true}, nil
	case strings.HasPrefix(event.EventType, "r.youtube."):
		return genericDirectTarget{label: "youtube", table: "Data_R_Community_Raw.r_youtube_event_raw", includePackage: true}, nil
	case strings.HasPrefix(event.EventType, "r.community."):
		return genericDirectTarget{label: "community", table: "Data_R_Community_Raw.r_community_event_raw"}, nil
	default:
		return genericDirectTarget{}, fmt.Errorf("direct ClickHouse publish does not support event_type=%q", event.EventType)
	}
}

func insertGenericRawEventChunkWithSplit(ctx context.Context, cfg clickHouseQueryConfig, target genericDirectTarget, events []genericEvent) error {
	if len(events) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(genericRawEventInsertPrefix(cfg, target.table))
	now := time.Now().UTC()
	for _, event := range events {
		row := genericRawEventDirectRow(event, now, target.includePackage)
		body, err := json.Marshal(row)
		if err != nil {
			return err
		}
		b.Write(body)
		b.WriteByte('\n')
	}
	err := execClickHouseDirectChunk(ctx, cfg, b.String(), target.label, len(events))
	if err == nil {
		return nil
	}
	if len(events) > 1 && envBool("RPROJECT_CLICKHOUSE_SPLIT_ON_TIMEOUT", true) && retryableClickHouseFallbackError(err) {
		mid := len(events) / 2
		fmt.Printf("[clickhouse] direct publish split target=%s events=%d reason=%s\n", target.label, len(events), publicClickHouseError(err))
		if splitErr := insertGenericRawEventChunkWithSplit(ctx, cfg, target, events[:mid]); splitErr != nil {
			return splitErr
		}
		return insertGenericRawEventChunkWithSplit(ctx, cfg, target, events[mid:])
	}
	return err
}

func genericRawEventInsertPrefix(cfg clickHouseQueryConfig, table string) string {
	sync := 0
	if cfg.InsertDistributedSync {
		sync = 1
	}
	return fmt.Sprintf("INSERT INTO %s SETTINGS insert_distributed_sync = %d, insert_deduplicate = 1 FORMAT JSONEachRow\n", table, sync)
}

func genericRawEventDirectRow(event genericEvent, now time.Time, includePackage bool) map[string]any {
	observedAt := parseKSTTime(event.ObservedAt, now)
	collectedAt := parseKSTTime(event.CollectedAt, now)
	row := map[string]any{
		"uuid":           event.EventID,
		"event_id":       event.EventID,
		"event_type":     event.EventType,
		"schema_version": event.SchemaVersion,
		"source":         event.Source,
		"source_url":     event.SourceURL,
		"repository":     event.Repository,
		"observed_at":    formatKST(observedAt),
		"collected_at":   formatKST(collectedAt),
		"payload_hash":   event.PayloadHash,
		"payload":        event.Payload,
		"ingested_at":    formatKST(now),
	}
	if includePackage {
		row["package_name"] = event.PackageName
		row["package_version"] = event.PackageVersion
	}
	return row
}

func execClickHouseDirectChunk(ctx context.Context, cfg clickHouseQueryConfig, query, label string, eventCount int) error {
	attempts := maxInt(1, envInt("RPROJECT_CLICKHOUSE_ATTEMPTS", envInt("RPKG_CLICKHOUSE_FALLBACK_ATTEMPTS", 3)))
	backoff := time.Duration(envFloat("RPROJECT_CLICKHOUSE_BACKOFF_SECONDS", envFloat("RPKG_CLICKHOUSE_FALLBACK_BACKOFF_SECONDS", 2.0)) * float64(time.Second))
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		err := cfg.exec(ctx, query)
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[clickhouse] direct publish retry succeeded target=%s attempt=%d events=%d\n", label, attempt, eventCount)
			}
			return nil
		}
		lastErr = err
		if attempt == attempts || !retryableClickHouseFallbackError(err) {
			return err
		}
		fmt.Printf("[clickhouse] direct publish retry target=%s attempt=%d/%d events=%d reason=%s\n", label, attempt+1, attempts, eventCount, publicClickHouseError(err))
		if sleepErr := sleepContext(ctx, backoff); sleepErr != nil {
			return fmt.Errorf("clickhouse direct retry wait stopped: %w; last_error=%s", sleepErr, publicClickHouseError(lastErr))
		}
	}
	return lastErr
}

func insertPackageRawEventsFallback(ctx context.Context, events []genericEvent) error {
	if len(events) == 0 {
		return nil
	}
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return err
	}
	chunkSize := maxInt(1, envInt("RPKG_CLICKHOUSE_FALLBACK_CHUNK_SIZE", minInt(envInt("KAFKA_WRITE_CHUNK_SIZE", 100), 10)))
	for _, chunk := range chunkGenericEvents(events, chunkSize) {
		if err := insertPackageRawEventChunk(ctx, cfg, chunk); err != nil {
			return err
		}
	}
	return nil
}

func failedPackageEventsFromKafkaError(err error) ([]genericEvent, error) {
	messages := kafkaFailedMessages(err)
	if len(messages) == 0 {
		return nil, nil
	}
	events := make([]genericEvent, 0, len(messages))
	for _, message := range messages {
		var event genericEvent
		if decodeErr := json.Unmarshal(message.Value, &event); decodeErr != nil {
			return nil, decodeErr
		}
		if event.EventID == "" || event.EventType == "" {
			return nil, fmt.Errorf("failed kafka message is missing package event identity")
		}
		if !strings.HasPrefix(event.EventType, "rpkg.") {
			return nil, fmt.Errorf("failed kafka message event_type %q is not a package event", event.EventType)
		}
		events = append(events, event)
	}
	return events, nil
}

func insertPackageRawEventChunk(ctx context.Context, cfg clickHouseQueryConfig, events []genericEvent) error {
	return insertPackageRawEventChunkWithSplit(ctx, cfg, events)
}

func insertPackageRawEventChunkWithSplit(ctx context.Context, cfg clickHouseQueryConfig, events []genericEvent) error {
	if len(events) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(packageRawEventInsertPrefix(cfg))
	now := time.Now().UTC()
	for _, event := range events {
		row := packageRawEventFallbackRow(event, now)
		body, err := json.Marshal(row)
		if err != nil {
			return err
		}
		b.Write(body)
		b.WriteByte('\n')
	}
	err := execPackageRawFallbackChunk(ctx, cfg, b.String(), len(events))
	if err == nil {
		return nil
	}
	if len(events) > 1 && envBool("RPKG_CLICKHOUSE_FALLBACK_SPLIT_ON_TIMEOUT", true) && retryableClickHouseFallbackError(err) {
		mid := len(events) / 2
		fmt.Printf("[clickhouse] package raw fallback split events=%d reason=%s\n", len(events), publicClickHouseError(err))
		if splitErr := insertPackageRawEventChunkWithSplit(ctx, cfg, events[:mid]); splitErr != nil {
			return splitErr
		}
		return insertPackageRawEventChunkWithSplit(ctx, cfg, events[mid:])
	}
	return err
}

func packageRawEventInsertPrefix(cfg clickHouseQueryConfig) string {
	sync := 0
	if cfg.InsertDistributedSync {
		sync = 1
	}
	return fmt.Sprintf("INSERT INTO Data_R_Package_Raw.r_package_event_raw SETTINGS insert_distributed_sync = %d, insert_deduplicate = 1 FORMAT JSONEachRow\n", sync)
}

func execPackageRawFallbackChunk(ctx context.Context, cfg clickHouseQueryConfig, query string, eventCount int) error {
	attempts := maxInt(1, envInt("RPKG_CLICKHOUSE_FALLBACK_ATTEMPTS", 3))
	backoff := time.Duration(envFloat("RPKG_CLICKHOUSE_FALLBACK_BACKOFF_SECONDS", 2.0) * float64(time.Second))
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		err := cfg.exec(ctx, query)
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[clickhouse] package raw fallback retry succeeded attempt=%d events=%d\n", attempt, eventCount)
			}
			return nil
		}
		lastErr = err
		if attempt == attempts || !retryableClickHouseFallbackError(err) {
			return err
		}
		fmt.Printf("[clickhouse] package raw fallback retry attempt=%d/%d events=%d reason=%s\n", attempt+1, attempts, eventCount, publicClickHouseError(err))
		if sleepErr := sleepContext(ctx, backoff); sleepErr != nil {
			return fmt.Errorf("clickhouse fallback retry wait stopped: %w; last_error=%s", sleepErr, publicClickHouseError(lastErr))
		}
	}
	return lastErr
}

func retryableClickHouseFallbackError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "temporary failure in name resolution") ||
		strings.Contains(msg, "clickhouse http 5")
}

func packageRawEventFallbackRow(event genericEvent, now time.Time) map[string]any {
	observedAt := parseKSTTime(event.ObservedAt, now)
	collectedAt := parseKSTTime(event.CollectedAt, now)
	return map[string]any{
		"uuid":            event.EventID,
		"event_id":        event.EventID,
		"event_type":      event.EventType,
		"schema_version":  event.SchemaVersion,
		"source":          event.Source,
		"source_url":      event.SourceURL,
		"repository":      event.Repository,
		"package_name":    event.PackageName,
		"package_version": event.PackageVersion,
		"observed_at":     formatKST(observedAt),
		"collected_at":    formatKST(collectedAt),
		"payload_hash":    event.PayloadHash,
		"payload":         event.Payload,
		"ingested_at":     formatKST(now),
	}
}

func (p *publisher) publishWebR(ctx context.Context, events []webREvent) error {
	if p.dryRun {
		for _, event := range events {
			body, err := json.Marshal(event)
			if err != nil {
				return err
			}
			fmt.Println(string(body))
		}
		return nil
	}
	if p.usesClickHouse() {
		counts, err := insertWebREventsDirect(ctx, events)
		if err != nil {
			return fmt.Errorf("ClickHouse direct Web-R publish failed: %s", publicClickHouseError(err))
		}
		if len(events) > 0 {
			fmt.Printf("[clickhouse] direct Web-R publish succeeded events=%d tables=%s\n", len(events), directCountSummary(counts))
		}
		if !p.usesKafka() {
			return nil
		}
	}
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{Key: []byte(firstNonEmpty(event.URL, event.EventUUID)), Value: body, Time: time.Now()})
	}
	return p.write(ctx, messages)
}

func insertWebREventsDirect(ctx context.Context, events []webREvent) (map[string]int, error) {
	counts := map[string]int{}
	if len(events) == 0 {
		return counts, nil
	}
	cfg, err := newClickHouseQueryConfig()
	if err != nil {
		return counts, err
	}
	rowsByTable := map[string][]map[string]any{}
	for _, event := range events {
		payload, err := decodeWebREventPayload(event)
		if err != nil {
			return counts, err
		}
		table, row, err := webREventDirectRow(event, payload)
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
		if err := insertDirectRows(ctx, cfg, table, rows); err != nil {
			return counts, err
		}
		counts[table] += len(rows)
	}
	return counts, nil
}

func decodeWebREventPayload(event webREvent) (map[string]any, error) {
	payload := map[string]any{}
	if strings.TrimSpace(event.Payload) == "" {
		return payload, nil
	}
	if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid Web-R event payload event_type=%s: %w", event.EventType, err)
	}
	return payload, nil
}

func webREventDirectRow(event webREvent, payload map[string]any) (string, map[string]any, error) {
	switch event.EventType {
	case "webr.rblogger.log.v1":
		return "Data_R_Community_Log.r_blogger_log", rbloggerLogDirectRow(event, payload), nil
	case "webr.rblogger.raw.v1":
		return "Data_R_Community_Raw.r_blogger_article_raw", rbloggerRawDirectRow(event, payload), nil
	case "webr.rblogger.board.v1":
		return "Data_R_Community_Service.r_blogger_board", webRBoardDirectRow(event, payload, "ko"), nil
	case "webr.mastodon.log.v1":
		return "Data_R_Community_Log.mastodon_log", mastodonLogDirectRow(event, payload), nil
	case "webr.mastodon.raw.v1":
		row, err := mastodonRawDirectRow(event, payload)
		return "Data_R_Community_Raw.mastodon_status_raw", row, err
	case "webr.mastodon.board.v1":
		return "Data_R_Community_Service.mastodon_board", webRBoardDirectRow(event, payload, "ko"), nil
	default:
		return "", nil, fmt.Errorf("direct ClickHouse Web-R publish does not support event_type=%q", event.EventType)
	}
}

func rbloggerLogDirectRow(event webREvent, payload map[string]any) map[string]any {
	return map[string]any{
		"uuid":          firstNonEmpty(stringAny(payload["uuid"]), event.EventUUID),
		"created_at":    nullableDirectString(firstNonEmpty(stringAny(payload["created_at"]), event.CreatedAt)),
		"created_log":   nullableDirectJSON(payload["created_log"]),
		"language_code": firstNonEmpty(stringAny(payload["language_code"]), "en"),
	}
}

func mastodonLogDirectRow(event webREvent, payload map[string]any) map[string]any {
	return map[string]any{
		"uuid":          firstNonEmpty(stringAny(payload["uuid"]), event.EventUUID),
		"created_at":    nullableDirectString(firstNonEmpty(stringAny(payload["created_at"]), event.CreatedAt)),
		"created_log":   nullableDirectJSON(payload["created_log"]),
		"language_code": firstNonEmpty(stringAny(payload["language_code"]), "en"),
	}
}

func rbloggerRawDirectRow(event webREvent, payload map[string]any) map[string]any {
	createdLog := mapAny(payload["created_log"])
	articleLog := mapAny(createdLog["article"])
	return map[string]any{
		"uuid":                  firstNonEmpty(stringAny(payload["uuid"]), event.EventUUID),
		"created_at":            nullableDirectString(firstNonEmpty(stringAny(payload["created_at"]), event.CreatedAt)),
		"created_log":           nullableDirectJSON(payload["created_log"]),
		"updated_at":            nullableDirectString(stringAny(payload["updated_at"])),
		"updated_log":           nullableDirectJSON(payload["updated_log"]),
		"active":                nullableDirectUInt8(payload["active"]),
		"github_path":           nullableDirectString(stringAny(payload["github_path"])),
		"title":                 nullableDirectString(stringAny(payload["title"])),
		"content":               nullableDirectString(stringAny(payload["content"])),
		"url":                   nullableDirectString(firstNonEmpty(stringAny(payload["url"]), event.URL)),
		"url_hash":              firstNonEmpty(stringAny(payload["url_hash"]), shaHex(firstNonEmpty(stringAny(payload["url"]), event.URL))),
		"language_code":         firstNonEmpty(stringAny(payload["language_code"]), "en"),
		"canonical_url":         firstNonEmpty(stringAny(payload["canonical_url"]), stringAny(articleLog["canonical_url"])),
		"html_title":            firstNonEmpty(stringAny(payload["html_title"]), stringAny(articleLog["html_title"])),
		"h1_title":              firstNonEmpty(stringAny(payload["h1_title"]), stringAny(articleLog["h1_title"])),
		"meta_description":      firstNonEmpty(stringAny(payload["meta_description"]), stringAny(articleLog["meta_description"])),
		"meta_keywords":         firstNonEmpty(stringAny(payload["meta_keywords"]), stringAny(articleLog["meta_keywords"])),
		"og_title":              firstNonEmpty(stringAny(payload["og_title"]), stringAny(articleLog["og_title"])),
		"og_description":        firstNonEmpty(stringAny(payload["og_description"]), stringAny(articleLog["og_description"])),
		"og_image":              firstNonEmpty(stringAny(payload["og_image"]), stringAny(articleLog["og_image"])),
		"twitter_title":         firstNonEmpty(stringAny(payload["twitter_title"]), stringAny(articleLog["twitter_title"])),
		"twitter_description":   firstNonEmpty(stringAny(payload["twitter_description"]), stringAny(articleLog["twitter_description"])),
		"article_headline":      firstNonEmpty(stringAny(payload["article_headline"]), stringAny(articleLog["article_headline"])),
		"article_section":       firstNonEmpty(stringAny(payload["article_section"]), stringAny(articleLog["article_section"])),
		"article_tags_json":     directJSONString(firstNonEmptyAny(payload["article_tags"], articleLog["article_tags"]), "[]"),
		"article_author":        firstNonEmpty(stringAny(payload["article_author"]), stringAny(articleLog["article_author"])),
		"article_published_at":  nullableDirectString(firstNonEmpty(stringAny(payload["article_published"]), stringAny(articleLog["article_published"]))),
		"article_modified_at":   nullableDirectString(firstNonEmpty(stringAny(payload["article_modified"]), stringAny(articleLog["article_modified"]))),
		"word_count":            directUInt32(firstNonEmptyAny(payload["word_count"], articleLog["word_count"])),
		"reading_time_min":      directFloat32(firstNonEmptyAny(payload["reading_time_min"], articleLog["reading_time_min"])),
		"internal_links_json":   directJSONString(firstNonEmptyAny(payload["internal_links"], articleLog["internal_links"]), "[]"),
		"external_links_json":   directJSONString(firstNonEmptyAny(payload["external_links"], articleLog["external_links"]), "[]"),
		"images_json":           directJSONString(firstNonEmptyAny(payload["images"], articleLog["images"]), "[]"),
		"main_text_excerpt":     firstNonEmpty(stringAny(payload["main_text_excerpt"]), stringAny(articleLog["main_text_excerpt"])),
		"raw_article_json":      directJSONString(articleLog, "{}"),
	}
}

func webRBoardDirectRow(event webREvent, payload map[string]any, defaultLanguage string) map[string]any {
	title := stringAny(payload["title"])
	content := stringAny(payload["content"])
	if strings.TrimSpace(content) == "" {
		content = title
	}
	return map[string]any{
		"uuid":          firstNonEmpty(stringAny(payload["uuid"]), event.EventUUID),
		"title":         nullableDirectString(title),
		"content":       nullableDirectString(content),
		"active":        nullableDirectUInt8(payload["active"]),
		"created_at":    nullableDirectString(firstNonEmpty(stringAny(payload["created_at"]), event.CreatedAt)),
		"updated_at":    nullableDirectString(stringAny(payload["updated_at"])),
		"created_log":   nullableDirectJSON(payload["created_log"]),
		"updated_log":   nullableDirectJSON(payload["updated_log"]),
		"language_code": firstNonEmpty(stringAny(payload["language_code"]), defaultLanguage),
	}
}

func mastodonRawDirectRow(event webREvent, payload map[string]any) (map[string]any, error) {
	rowUUID := stringAny(payload["uuid"])
	if rowUUID == "" {
		return nil, fmt.Errorf("mastodon raw event is missing payload.uuid")
	}
	now := formatKST(time.Now())
	return map[string]any{
		"uuid":                   rowUUID,
		"event_uuid":             event.EventUUID,
		"source":                 firstNonEmpty(event.Source, "github_actions"),
		"host":                   firstNonEmpty(event.Host, "github-actions"),
		"uuid_user":              nullableDirectString(event.UUIDUser),
		"ip":                     firstNonEmpty(event.IP, "::"),
		"url":                    event.URL,
		"event_type":             event.EventType,
		"instance_host":          stringAny(payload["instance_host"]),
		"account_acct":           stringAny(payload["account_acct"]),
		"account_id":             stringAny(payload["account_id"]),
		"status_id":              stringAny(payload["status_id"]),
		"status_uri":             stringAny(payload["status_uri"]),
		"status_url":             stringAny(payload["status_url"]),
		"status_created_at":      firstNonEmpty(stringAny(payload["status_created_at"]), stringAny(payload["fetched_at"]), event.CreatedAt, now),
		"status_edited_at":       nullableDirectString(stringAny(payload["status_edited_at"])),
		"visibility":             firstNonEmpty(stringAny(payload["visibility"]), "unknown"),
		"language":               stringAny(payload["language"]),
		"language_code":          firstNonEmpty(stringAny(payload["language_code"]), "en"),
		"sensitive":              directUInt8(payload["sensitive"]),
		"spoiler_text":           stringAny(payload["spoiler_text"]),
		"content_html":           stringAny(payload["content_html"]),
		"content_text":           stringAny(payload["content_text"]),
		"in_reply_to_id":         nullableDirectString(stringAny(payload["in_reply_to_id"])),
		"in_reply_to_account_id": nullableDirectString(stringAny(payload["in_reply_to_account_id"])),
		"is_reblog":              directUInt8(payload["is_reblog"]),
		"reblog_status_id":       nullableDirectString(stringAny(payload["reblog_status_id"])),
		"replies_count":          directUInt32(payload["replies_count"]),
		"reblogs_count":          directUInt32(payload["reblogs_count"]),
		"favourites_count":       directUInt32(payload["favourites_count"]),
		"active":                 directUInt8(payload["active"]),
		"tags_json":              directJSONString(payload["tags"], "[]"),
		"mentions_json":          directJSONString(payload["mentions"], "[]"),
		"emojis_json":            directJSONString(payload["emojis"], "[]"),
		"media_attachments_json": directJSONString(payload["media_attachments"], "[]"),
		"card_json":              directJSONString(payload["card"], "{}"),
		"poll_json":              directJSONString(payload["poll"], "{}"),
		"raw_status_json":        directJSONString(payload["raw_status_json"], "{}"),
		"payload_hash":           directUInt64(payload["payload_hash"]),
		"image_count":            directUInt8(payload["image_count"]),
		"image_base64_count":     directUInt8(payload["image_base64_count"]),
		"has_image_base64":       directUInt8(payload["has_image_base64"]),
		"fetched_at":             firstNonEmpty(stringAny(payload["fetched_at"]), event.CreatedAt, now),
		"created_at":             firstNonEmpty(event.CreatedAt, now),
		"ingested_at":            now,
		"kafka_topic":            "direct.clickhouse",
		"kafka_partition":        0,
		"kafka_offset":           0,
	}, nil
}

func insertDirectRows(ctx context.Context, cfg clickHouseQueryConfig, table string, rows []map[string]any) error {
	chunkSize := maxInt(1, envInt("RPROJECT_CLICKHOUSE_CHUNK_SIZE", 50))
	for _, chunk := range chunkMapRows(rows, chunkSize) {
		if err := insertDirectRowsChunkWithSplit(ctx, cfg, table, chunk); err != nil {
			return err
		}
	}
	return nil
}

func insertDirectRowsChunkWithSplit(ctx context.Context, cfg clickHouseQueryConfig, table string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(genericRawEventInsertPrefix(cfg, table))
	for _, row := range rows {
		body, err := json.Marshal(row)
		if err != nil {
			return err
		}
		b.Write(body)
		b.WriteByte('\n')
	}
	err := execClickHouseDirectChunk(ctx, cfg, b.String(), table, len(rows))
	if err == nil {
		return nil
	}
	if len(rows) > 1 && envBool("RPROJECT_CLICKHOUSE_SPLIT_ON_TIMEOUT", true) && retryableClickHouseFallbackError(err) {
		mid := len(rows) / 2
		fmt.Printf("[clickhouse] direct row split table=%s rows=%d reason=%s\n", table, len(rows), publicClickHouseError(err))
		if splitErr := insertDirectRowsChunkWithSplit(ctx, cfg, table, rows[:mid]); splitErr != nil {
			return splitErr
		}
		return insertDirectRowsChunkWithSplit(ctx, cfg, table, rows[mid:])
	}
	return err
}

func (p *publisher) write(ctx context.Context, messages []kafka.Message) error {
	if p.dryRun || len(messages) == 0 {
		return nil
	}
	for _, chunk := range chunkMessages(messages, p.chunkSize) {
		if err := p.writeMessagesWithRetry(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (p *publisher) writerWithBalancer(balancer kafka.Balancer) *kafka.Writer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(p.brokers...),
		Topic:                  p.topic,
		Balancer:               balancer,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: p.createTopic,
		BatchSize:              p.chunkSize,
		BatchTimeout:           500 * time.Millisecond,
		WriteTimeout:           p.writeTimeout,
		ReadTimeout:            p.writeTimeout,
		MaxAttempts:            p.writerMaxAttempts,
	}
	transport := &kafka.Transport{
		ClientID: p.clientID,
		Dial:     kafkaAdvertisedBrokerDialFunc(p.brokers, 10*time.Second),
	}
	if p.username != "" || p.password != "" {
		transport.SASL = plain.Mechanism{Username: p.username, Password: p.password}
	}
	if p.usesTLS() {
		transport.TLS = kafkaTLSConfig()
	}
	writer.Transport = transport
	return writer
}

func (p *publisher) writeMessagesWithRetry(ctx context.Context, messages []kafka.Message) error {
	pending := messages
	var lastErr error
	for attempt := 1; attempt <= p.writeAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(ctx, p.writeTimeout+15*time.Second)
		writer := p.writerWithBalancer(&kafka.Hash{})
		err := writer.WriteMessages(writeCtx, pending...)
		_ = writer.Close()
		cancel()
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[kafka] publish retry succeeded attempt=%d messages=%d\n", attempt, len(pending))
			}
			return nil
		}

		lastErr = err
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 {
			return nil
		}
		if !retryable || attempt == p.writeAttempts {
			if shouldTryPartitionFallback(err, attempt, p.writeAttempts, p.partitionFallback) {
				if fallbackErr := p.writeMessagesToWritablePartition(ctx, failed); fallbackErr == nil {
					return nil
				} else {
					return newKafkaPublishError(
						fmt.Errorf("kafka publish failed after fixed-partition fallback: %s; original_error=%s", shortKafkaError(fallbackErr), shortKafkaError(err)),
						firstKafkaFailedMessages(fallbackErr, failed),
					)
				}
			}
			return newKafkaPublishError(err, failed)
		}
		fmt.Printf("[kafka] retrying publish attempt=%d/%d failed_messages=%d reason=%s error=%s\n", attempt+1, p.writeAttempts, len(failed), kafkaRetryReason(err), shortKafkaError(err))
		if err := sleepContext(ctx, kafkaBackoffDuration(attempt, p.writeBackoffMin, p.writeBackoffMax)); err != nil {
			return fmt.Errorf("kafka retry wait stopped: %w; last_error=%s", err, shortKafkaError(lastErr))
		}
		pending = failed
	}
	return lastErr
}

func shouldTryPartitionFallback(err error, attempt, maxAttempts int, enabled bool) bool {
	return enabled && attempt >= maxAttempts && shouldUsePartitionFallback(err)
}

func (p *publisher) writeMessagesToWritablePartition(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	partitions, err := p.fallbackPartitionIDs(ctx)
	if err != nil {
		return newKafkaPublishError(err, messages)
	}
	if len(partitions) == 0 {
		return newKafkaPublishError(fmt.Errorf("kafka partition fallback found zero partitions for topic=%s", p.topic), messages)
	}

	pending := messages
	var lastErr error
	for _, partition := range partitions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, p.fallbackTimeout)
		writer := p.writerWithBalancer(fixedPartitionBalancer{partition: partition})
		err := writer.WriteMessages(attemptCtx, pending...)
		_ = writer.Close()
		cancel()
		if err == nil {
			fmt.Printf("[kafka] fixed partition fallback succeeded partition=%d messages=%d\n", partition, len(pending))
			return nil
		}

		lastErr = err
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 {
			return nil
		}
		pending = failed
		fmt.Printf("[kafka] fixed partition fallback failed partition=%d failed_messages=%d reason=%s error=%s\n", partition, len(pending), kafkaRetryReason(err), shortKafkaError(err))
		if !retryable {
			return newKafkaPublishError(err, failed)
		}
	}
	return newKafkaPublishError(fmt.Errorf("kafka fixed partition fallback exhausted partitions=%v failed_messages=%d last_error=%s", partitions, len(pending), shortKafkaError(lastErr)), pending)
}

type kafkaPublishError struct {
	err            error
	failedMessages []kafka.Message
}

func (err *kafkaPublishError) Error() string {
	if err == nil || err.err == nil {
		return ""
	}
	return err.err.Error()
}

func (err *kafkaPublishError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.err
}

func newKafkaPublishError(err error, failedMessages []kafka.Message) error {
	if err == nil {
		return nil
	}
	if len(failedMessages) == 0 {
		return err
	}
	return &kafkaPublishError{
		err:            err,
		failedMessages: cloneKafkaMessages(failedMessages),
	}
}

func kafkaFailedMessages(err error) []kafka.Message {
	var publishErr *kafkaPublishError
	if !errors.As(err, &publishErr) || publishErr == nil {
		return nil
	}
	return cloneKafkaMessages(publishErr.failedMessages)
}

func firstKafkaFailedMessages(err error, fallback []kafka.Message) []kafka.Message {
	if failed := kafkaFailedMessages(err); len(failed) > 0 {
		return failed
	}
	return fallback
}

func cloneKafkaMessages(messages []kafka.Message) []kafka.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]kafka.Message, len(messages))
	copy(out, messages)
	return out
}

func (p *publisher) fallbackPartitionIDs(ctx context.Context) ([]int, error) {
	if len(p.fallbackPartitions) > 0 {
		out := append([]int(nil), p.fallbackPartitions...)
		sort.Ints(out)
		return uniqueInts(out), nil
	}
	if len(p.knownPartitions) > 0 {
		out := append([]int(nil), p.knownPartitions...)
		sort.Ints(out)
		return uniqueInts(out), nil
	}

	dialer := p.dialer()
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(probeCtx, "tcp", p.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to connect to bootstrap broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(p.topic)
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to read metadata for topic %q: %w", p.topic, err)
	}
	return partitionIDsForTopic(partitions, p.topic), nil
}

func partitionIDsForTopic(partitions []kafka.Partition, topic string) []int {
	out := make([]int, 0, len(partitions))
	for _, partition := range partitions {
		if partition.Topic == topic {
			out = append(out, partition.ID)
		}
	}
	sort.Ints(out)
	return uniqueInts(out)
}

type fixedPartitionBalancer struct {
	partition int
}

func (b fixedPartitionBalancer) Balance(_ kafka.Message, partitions ...int) int {
	for _, partition := range partitions {
		if partition == b.partition {
			return partition
		}
	}
	if len(partitions) > 0 {
		return partitions[0]
	}
	return b.partition
}

func (p *publisher) usesTLS() bool {
	return kafkaSecurityUsesTLS(p.security)
}

func kafkaTopicMissing(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unknown topic") ||
		strings.Contains(message, "unknown topic or partition") ||
		strings.Contains(message, "[3]")
}

func kafkaTopicAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "topic already exists") ||
		strings.Contains(message, "already exists") ||
		strings.Contains(message, "[36]")
}

func retryableFailedMessages(messages []kafka.Message, err error) ([]kafka.Message, bool) {
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if len(writeErrs) != len(messages) {
			return messages, retryableKafkaWriteError(err)
		}
		failed := make([]kafka.Message, 0, writeErrs.Count())
		retryable := true
		for i, writeErr := range writeErrs {
			if writeErr == nil {
				continue
			}
			failed = append(failed, messages[i])
			if !retryableKafkaWriteError(writeErr) {
				retryable = false
			}
		}
		return failed, retryable
	}
	return messages, retryableKafkaWriteError(err)
}

func retryableKafkaWriteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if isKafkaAuthOrPermissionErrorText(err.Error()) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if writeErrs.Count() == 0 {
			return false
		}
		for _, writeErr := range writeErrs {
			if writeErr != nil && !retryableKafkaWriteError(writeErr) {
				return false
			}
		}
		return true
	}
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	return errors.Is(err, io.EOF) || isRetryableKafkaErrorText(err.Error())
}

func isRetryableKafkaErrorText(message string) bool {
	msg := strings.ToLower(message)
	if isKafkaAuthOrPermissionErrorText(msg) {
		return false
	}
	return strings.Contains(msg, "not leader for partition") ||
		strings.Contains(msg, "partition has no leader") ||
		strings.Contains(msg, "has no leader") ||
		strings.Contains(msg, "leader not available") ||
		strings.Contains(msg, "metadata are likely out of date") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "failed to dial") ||
		strings.Contains(msg, "failed to open connection") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "temporary failure in name resolution") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "eof")
}

func isKafkaAuthOrPermissionErrorText(message string) bool {
	msg := strings.ToLower(message)
	if strings.Contains(msg, "read tcp") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "failed to dial") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "temporary failure in name resolution") {
		return false
	}
	return strings.Contains(msg, "sasl authentication failed") ||
		strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "not authorized") ||
		strings.Contains(msg, "authorization failed") ||
		strings.Contains(msg, "invalid credentials") ||
		strings.Contains(msg, "invalid username") ||
		strings.Contains(msg, "invalid password")
}

func shouldUsePartitionFallback(err error) bool {
	return kafkaRetryReason(err) == "leader-metadata-stale" || kafkaRetryReason(err) == "leader-not-available"
}

func kafkaRetryReason(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case isKafkaAuthOrPermissionErrorText(msg):
		return "kafka-auth"
	case strings.Contains(msg, "not leader for partition"),
		strings.Contains(msg, "partition has no leader"),
		strings.Contains(msg, "has no leader"),
		strings.Contains(msg, "metadata are likely out of date"):
		return "leader-metadata-stale"
	case strings.Contains(msg, "leader not available"):
		return "leader-not-available"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "eof"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "failed to dial"),
		strings.Contains(msg, "failed to open connection"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "temporary failure in name resolution"):
		return "network"
	default:
		return "temporary-kafka-error"
	}
}

func kafkaBackoffDuration(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	if minDelay <= 0 {
		minDelay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 12 * time.Second
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	delay := minDelay
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shortKafkaError(err error) string {
	if err == nil {
		return ""
	}
	msg := sanitizeKafkaError(strings.Join(strings.Fields(err.Error()), " "))
	return truncate(msg, 280)
}

func sanitizeKafkaError(message string) string {
	return kafkaIPv4RE.ReplaceAllString(message, "[ip]")
}

func publicClickHouseError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, context.DeadlineExceeded),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "deadline exceeded"):
		return "clickhouse-timeout"
	case strings.Contains(msg, "401"),
		strings.Contains(msg, "authentication"),
		strings.Contains(msg, "unauthorized"):
		return "clickhouse-auth"
	case strings.Contains(msg, "403"),
		strings.Contains(msg, "not enough privileges"),
		strings.Contains(msg, "readonly"):
		return "clickhouse-permission"
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "no such host"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "temporary failure in name resolution"):
		return "clickhouse-network"
	case strings.Contains(msg, "clickhouse http 5"):
		return "clickhouse-server-error"
	case strings.Contains(msg, "clickhouse http 4"):
		return "clickhouse-request-error"
	default:
		return "clickhouse-error"
	}
}

func kafkaSecurityUsesTLS(value string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	return value == "SSL" || value == "SASL_SSL" || envBool("KAFKA_TLS", false)
}

func kafkaTLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

func newGenericEvent(eventType, source, sourceURL, repository, packageName, packageVersion, observedAt string, payload map[string]any) genericEvent {
	payloadJSON := mustJSON(payload)
	collectedAt := utcNow()
	if observedAt == "" {
		observedAt = collectedAt
	}
	return genericEvent{
		EventID:        uuid7(),
		EventType:      eventType,
		SchemaVersion:  1,
		Source:         source,
		SourceURL:      sourceURL,
		Repository:     repository,
		PackageName:    packageName,
		PackageVersion: packageVersion,
		ObservedAt:     observedAt,
		CollectedAt:    collectedAt,
		PayloadHash:    shaHex(payloadJSON),
		Payload:        payloadJSON,
	}
}

func newWebREvent(eventType, sourceURL string, payload map[string]any, createdAt time.Time) webREvent {
	payloadJSON := mustJSON(payload)
	host, _ := os.Hostname()
	return webREvent{
		EventUUID: uuid7(),
		Source:    envString("PRODUCER_SOURCE", "github_actions"),
		Host:      firstNonEmpty(host, "github-actions"),
		UUIDUser:  "",
		IP:        envString("PRODUCER_IP", "::"),
		URL:       sourceURL,
		EventType: eventType,
		Payload:   payloadJSON,
		CreatedAt: formatKST(createdAt),
	}
}

func collectionFailureEvent(eventType, source, sourceURL, repository, packageName string, err error) genericEvent {
	return newGenericEvent(eventType, source, sourceURL, repository, packageName, "", "", map[string]any{
		"source_url":        sourceURL,
		"error_code":        fmt.Sprintf("%T", err),
		"error_message":     err.Error(),
		"source_method":     source,
		"collection_status": "failed",
	})
}

func fetchBytes(targetURL string) ([]byte, error) {
	body, _, err := fetchBytesWithContentType(targetURL)
	return body, err
}

func fetchBytesWithContentType(targetURL string) ([]byte, string, error) {
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/json,text/plain,*/*")
	client := &http.Client{Timeout: time.Duration(envInt("HTTP_TIMEOUT", 90)) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(envInt("HTTP_MAX_BYTES", 20*1024*1024))))
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode/100 != 2 {
		return nil, "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" && len(body) > 0 {
		contentType = http.DetectContentType(body)
	}
	return body, contentType, nil
}

func fetchJSON(targetURL string, out any) error {
	body, err := fetchBytes(targetURL)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

func fetchJSONWithHeaders(targetURL string, headers map[string]string, out any) error {
	return doJSONRequest(http.MethodGet, targetURL, headers, nil, out)
}

func postJSON(targetURL string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return doJSONRequest(http.MethodPost, targetURL, map[string]string{"Content-Type": "application/json"}, body, out)
}

func doJSONRequest(method, targetURL string, headers map[string]string, body []byte, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, targetURL, reader)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json,*/*")
	for key, value := range headers {
		if strings.TrimSpace(value) != "" {
			req.Header.Set(key, value)
		}
	}
	client := &http.Client{Timeout: time.Duration(envInt("HTTP_TIMEOUT", 90)) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, int64(envInt("HTTP_MAX_BYTES", 20*1024*1024))))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 500))
	}
	return json.Unmarshal(respBody, out)
}

func newClickHouseQueryConfig() (clickHouseQueryConfig, error) {
	cfg := clickHouseQueryConfig{
		Host:                  firstNonEmpty(os.Getenv("CH_HOST"), os.Getenv("CLICKHOUSE_HOST")),
		Port:                  maxInt(1, envInt("CH_PORT", envInt("CLICKHOUSE_PORT", 8123))),
		User:                  firstNonEmpty(os.Getenv("CH_USER"), os.Getenv("CLICKHOUSE_USER")),
		Password:              firstNonEmpty(os.Getenv("CH_PASSWORD"), os.Getenv("CLICKHOUSE_PASSWORD")),
		Database:              envString("CH_DATABASE", envString("CLICKHOUSE_DATABASE", "Data_R_Community_Service")),
		Secure:                envBool("CH_SECURE", envBool("CLICKHOUSE_SECURE", false)),
		Timeout:               time.Duration(maxInt(10, envInt("CH_TIMEOUT", envInt("CLICKHOUSE_TIMEOUT", 60)))) * time.Second,
		InsertDistributedSync: envBool("RPROJECT_CLICKHOUSE_INSERT_DISTRIBUTED_SYNC", envBool("RPKG_CLICKHOUSE_FALLBACK_INSERT_DISTRIBUTED_SYNC", false)),
	}
	if cfg.Host == "" {
		return cfg, errors.New("CH_HOST or CLICKHOUSE_HOST is required for DB-backed collectors")
	}
	if cfg.User == "" {
		return cfg, errors.New("CH_USER or CLICKHOUSE_USER is required for DB-backed collectors")
	}
	if cfg.Password == "" {
		return cfg, errors.New("CH_PASSWORD or CLICKHOUSE_PASSWORD is required for DB-backed collectors")
	}
	return cfg, nil
}

func (cfg clickHouseQueryConfig) endpoint() (string, error) {
	rawHost := strings.TrimSpace(cfg.Host)
	if strings.HasPrefix(rawHost, "http://") || strings.HasPrefix(rawHost, "https://") {
		parsed, err := url.Parse(rawHost)
		if err != nil {
			return "", err
		}
		q := parsed.Query()
		if cfg.Database != "" && q.Get("database") == "" {
			q.Set("database", cfg.Database)
		}
		parsed.RawQuery = q.Encode()
		return parsed.String(), nil
	}
	host := rawHost
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, strconv.Itoa(cfg.Port))
	}
	scheme := "http"
	if cfg.Secure {
		scheme = "https"
	}
	endpoint := url.URL{Scheme: scheme, Host: host, Path: "/"}
	q := endpoint.Query()
	if cfg.Database != "" {
		q.Set("database", cfg.Database)
	}
	endpoint.RawQuery = q.Encode()
	return endpoint.String(), nil
}

func (cfg clickHouseQueryConfig) queryJSONEachRow(query string) ([]map[string]any, error) {
	endpoint, err := cfg.endpoint()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(cfg.User, cfg.Password)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	client := &http.Client{Timeout: cfg.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 20*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("ClickHouse HTTP %d: %s", resp.StatusCode, truncate(string(body), 500))
	}
	rows := make([]map[string]any, 0)
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (cfg clickHouseQueryConfig) exec(ctx context.Context, query string) error {
	endpoint, err := cfg.endpoint()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(query))
	if err != nil {
		return err
	}
	req.SetBasicAuth(cfg.User, cfg.Password)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	client := &http.Client{Timeout: cfg.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("ClickHouse HTTP %d: %s", resp.StatusCode, truncate(string(body), 700))
	}
	return nil
}

func firstTitle(text string) string {
	match := titleRE.FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return stripTags(match[1])
}

func textMatches(re *regexp.Regexp, text string, limit int) []string {
	out := make([]string, 0)
	for _, match := range re.FindAllStringSubmatch(text, -1) {
		if len(match) > 1 {
			value := stripTags(match[1])
			if value != "" {
				out = append(out, value)
				if limit > 0 && len(out) >= limit {
					break
				}
			}
		}
	}
	return out
}

func stripTags(value string) string {
	value = tagRE.ReplaceAllString(value, " ")
	value = html.UnescapeString(value)
	value = spaceRE.ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func recordPayload(record cranRecord) map[string]any {
	payload := map[string]any{}
	for key, value := range record {
		payload[strings.ToLower(strings.ReplaceAll(key, "/", "_"))] = value
	}
	return payload
}

func mapStringAny(in map[string]string) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func anyStringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []map[string]string:
		out := []string{}
		for _, row := range v {
			if text := firstNonEmpty(row["url"], row["href"], row["value"]); text != "" {
				out = append(out, text)
			}
		}
		return out
	}
	out := []string{}
	for _, item := range anySlice(value) {
		if text := stringAny(item); text != "" {
			out = append(out, text)
		}
	}
	return out
}

func anySlice(value any) []any {
	if items, ok := value.([]any); ok {
		return items
	}
	if items, ok := value.([]string); ok {
		out := make([]any, 0, len(items))
		for _, item := range items {
			out = append(out, item)
		}
		return out
	}
	return nil
}

func firstPresent(row map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := row[key]; ok {
			return value
		}
	}
	return nil
}

func mapAny(value any) map[string]any {
	if row, ok := value.(map[string]any); ok {
		return row
	}
	return map[string]any{}
}

func stringAny(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", value)
	}
}

func intString(value any) string {
	return strconv.FormatInt(intAny(value), 10)
}

func boolString(value any) string {
	return boolOrString(boolAny(value))
}

func boolOrString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func boolAny(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return envBoolValue(v)
	case float64:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	case json.Number:
		n, _ := v.Int64()
		return n != 0
	default:
		return false
	}
}

func envBoolValue(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "1" || value == "true" || value == "yes" || value == "y"
}

func intAny(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}

func floatAny(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		n, err := v.Float64()
		return n, err == nil
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func configuredURLs(envName string, defaults []string) []string {
	raw := envString(envName, "")
	if raw == "" {
		return defaults
	}
	return splitCSV(raw)
}

func expandJobs(raw string, all []string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "all" {
		return all
	}
	return splitCSV(raw)
}

func splitCSV(value string) []string {
	out := make([]string, 0)
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func splitIntCSV(value string) []int {
	out := make([]int, 0)
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			continue
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return uniqueInts(out)
}

func uniqueInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	out := values[:0]
	var last int
	for i, value := range values {
		if i > 0 && value == last {
			continue
		}
		out = append(out, value)
		last = value
	}
	return out
}

func mergeStringSlices(groups ...[]string) []string {
	out := make([]string, 0)
	seen := make(map[string]bool)
	for _, group := range groups {
		for _, value := range group {
			value = strings.TrimSpace(value)
			if value != "" && !seen[value] {
				seen[value] = true
				out = append(out, value)
			}
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonEmptyAny(values ...any) any {
	for _, value := range values {
		if value == nil {
			continue
		}
		if strings.TrimSpace(stringAny(value)) != "" {
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

func nullableDirectString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" || value == "<nil>" {
		return nil
	}
	return value
}

func nullableDirectJSON(value any) any {
	if value == nil {
		return nil
	}
	if strings.TrimSpace(stringAny(value)) == "" && len(mapAny(value)) == 0 {
		return nil
	}
	return value
}

func nullableDirectUInt8(value any) any {
	if value == nil || strings.TrimSpace(stringAny(value)) == "" {
		return nil
	}
	return directUInt8(value)
}

func directUInt8(value any) int {
	if directBool(value) {
		return 1
	}
	n := directInt64(value)
	if n < 0 {
		return 0
	}
	if n > 255 {
		return 255
	}
	return int(n)
}

func directUInt32(value any) uint32 {
	n := directInt64(value)
	if n < 0 {
		return 0
	}
	if n > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(n)
}

func directUInt64(value any) uint64 {
	switch typed := value.(type) {
	case uint64:
		return typed
	case uint:
		return uint64(typed)
	case int:
		if typed > 0 {
			return uint64(typed)
		}
	case int64:
		if typed > 0 {
			return uint64(typed)
		}
	case float64:
		if typed > 0 {
			return uint64(typed)
		}
	case json.Number:
		if n, err := strconv.ParseUint(typed.String(), 10, 64); err == nil {
			return n
		}
	}
	if n, err := strconv.ParseUint(strings.TrimSpace(stringAny(value)), 10, 64); err == nil {
		return n
	}
	return 0
}

func directFloat32(value any) float32 {
	switch typed := value.(type) {
	case float32:
		return typed
	case float64:
		return float32(typed)
	case int:
		return float32(typed)
	case int64:
		return float32(typed)
	case json.Number:
		if n, err := strconv.ParseFloat(typed.String(), 32); err == nil {
			return float32(n)
		}
	}
	if n, err := strconv.ParseFloat(strings.TrimSpace(stringAny(value)), 32); err == nil {
		return float32(n)
	}
	return 0
}

func directInt64(value any) int64 {
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
	case json.Number:
		if n, err := typed.Int64(); err == nil {
			return n
		}
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(stringAny(value)), 10, 64); err == nil {
		return n
	}
	return 0
}

func directBool(value any) bool {
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

func directJSONString(value any, fallback string) string {
	if value == nil {
		return fallback
	}
	if raw := strings.TrimSpace(stringAny(value)); raw != "" && (strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[")) {
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

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envFloat(name string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	return envBoolValue(value)
}

func cranPackagesURL() string {
	return envString("RPKG_CRAN_PACKAGES_URL", "https://cran.r-project.org/src/contrib/PACKAGES.gz")
}

func cranlogsBaseURL() string {
	return strings.TrimRight(envString("RPKG_CRANLOGS_BASE_URL", "https://cranlogs.r-pkg.org"), "/")
}

func dayToObserved(day string) string {
	if day == "" {
		return utcNow()
	}
	return day + "T00:00:00Z"
}

func utcNow() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

func nowKST() time.Time {
	return time.Now().UTC().Add(9 * time.Hour)
}

func formatKST(t time.Time) string {
	return t.UTC().Add(9 * time.Hour).Format("2006-01-02 15:04:05.000")
}

func parseRSSDate(value string, fallback time.Time) time.Time {
	for _, layout := range []string{time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822, time.RFC3339} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
			return parsed
		}
	}
	return fallback
}

func uuid7() string {
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
	return formatUUID(b)
}

func deterministicUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return formatUUID(b)
}

func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func shaHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}

func hashMaybe(salt, value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return shaHex(salt + ":" + value)
}

func stableUInt64(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	n, _ := strconv.ParseUint(fmt.Sprintf("%x", sum[:8]), 16, 64)
	return n
}

func stableID(value string) string {
	if parsed, err := url.Parse(value); err == nil {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		if len(parts) > 0 && parts[len(parts)-1] != "" {
			return parts[len(parts)-1]
		}
	}
	return shaHex(value)[:16]
}

func mustJSON(value any) string {
	body, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func eventKey(event genericEvent) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.Payload), &payload); err == nil {
		payloadKey := firstNonEmpty(
			stringAny(payload["external_id"]),
			stringAny(payload["digest_id"]),
			stringAny(payload["uuid"]),
			stringAny(payload["row_external_id"]),
		)
		if payloadKey != "" {
			return strings.Join([]string{event.Repository, event.Source, event.EventType, payloadKey}, ":")
		}
	}
	return strings.Join([]string{event.Repository, event.Source, event.PackageName, event.PackageVersion, event.EventType, event.EventID}, ":")
}

func absoluteURL(base, raw string) string {
	parsedBase, err := url.Parse(base)
	if err != nil {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return parsedBase.ResolveReference(parsed).String()
}

func hostFromURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func safeHTML(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		text = "R Foundation"
	}
	return "<p>" + html.EscapeString(text) + "</p>"
}

func sanitizeBoardHTML(value string) (string, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```html")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, "<") {
		if loc := boardFirstTagRE.FindStringIndex(value); loc != nil {
			value = strings.TrimSpace(value[loc[0]:])
		} else {
			value = "<p>" + html.EscapeString(removeBoardURLs(value)) + "</p>"
		}
	}
	value = removeBoardBlockTag(value, "script")
	value = removeBoardBlockTag(value, "style")
	value = removeBoardBlockTag(value, "iframe")
	value = removeBoardVoidTag(value, "img")
	for {
		next := boardAnchorRE.ReplaceAllStringFunc(value, func(match string) string {
			inner := boardAnchorOnlyRE.FindStringSubmatch(match)
			if len(inner) < 2 {
				return ""
			}
			return html.EscapeString(removeBoardURLs(stripTags(inner[1])))
		})
		if next == value {
			break
		}
		value = next
	}
	value = boardMdLinkRE.ReplaceAllString(value, "$1")
	value = boardURLRE.ReplaceAllString(value, "")
	value = boardAnyTagRE.ReplaceAllStringFunc(value, func(tag string) string {
		match := boardAnyTagRE.FindStringSubmatch(tag)
		if len(match) < 2 {
			return ""
		}
		name := strings.ToLower(match[1])
		if !boardAllowedTags[name] {
			return ""
		}
		if strings.HasPrefix(strings.TrimSpace(tag), "</") {
			return "</" + name + ">"
		}
		return "<" + name + ">"
	})
	value = strings.TrimSpace(value)
	value = regexp.MustCompile(`\s+</`).ReplaceAllString(value, "</")
	value = regexp.MustCompile(`>\s+`).ReplaceAllString(value, ">")
	if !strings.Contains(value, "<") {
		value = "<p>" + html.EscapeString(removeBoardURLs(value)) + "</p>"
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "<a") || strings.Contains(lower, "href=") || boardURLRE.MatchString(value) {
		return "", errors.New("sanitized translation still contains a hyperlink or URL")
	}
	return value, nil
}

func safeMastodonBoardContent(title, content string) string {
	content = strings.TrimSpace(content)
	if content != "" && strings.TrimSpace(stripTags(content)) != "" {
		if sanitized, err := sanitizeBoardHTML(content); err == nil && strings.TrimSpace(stripTags(sanitized)) != "" {
			return sanitized
		}
	}
	return safeHTML(removeBoardURLs(firstNonEmpty(title, "R Foundation")))
}

func removeBoardBlockTag(value, name string) string {
	re := regexp.MustCompile(`(?is)<` + regexp.QuoteMeta(name) + `\b[^>]*>.*?</` + regexp.QuoteMeta(name) + `>`)
	return re.ReplaceAllString(value, "")
}

func removeBoardVoidTag(value, name string) string {
	re := regexp.MustCompile(`(?is)<` + regexp.QuoteMeta(name) + `\b[^>]*>`)
	return re.ReplaceAllString(value, "")
}

func cleanBoardTitle(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	if i := strings.IndexAny(value, "\r\n"); i >= 0 {
		value = value[:i]
	}
	prefixRE := regexp.MustCompile(`(?i)^(translation|translated title|title|result|output|번역|번역문|제목|결과|출력)\s*[:\-]\s*`)
	value = prefixRE.ReplaceAllString(strings.TrimSpace(value), "")
	value = strings.Trim(value, " \t\"'“”‘’")
	return removeBoardURLs(value)
}

func removeBoardURLs(value string) string {
	value = boardMdLinkRE.ReplaceAllString(value, "$1")
	value = boardURLRE.ReplaceAllString(value, "")
	return strings.TrimSpace(spaceRE.ReplaceAllString(value, " "))
}

func looksKorean(value string, threshold float64) bool {
	compact := strings.Join(strings.Fields(value), "")
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

func parseKSTTime(value string, fallback time.Time) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		loc = time.FixedZone("KST", 9*3600)
	}
	for _, layout := range []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05", time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.ParseInLocation(layout, value, loc); err == nil {
			return parsed
		}
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.In(loc)
		}
	}
	return fallback
}

func firstWords(text string, count int) string {
	parts := strings.Fields(text)
	if len(parts) > count {
		parts = parts[:count]
	}
	return strings.Join(parts, " ")
}

func truncate(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if limit <= 0 || len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
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

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func firstNMaps(values []map[string]string, n int) []map[string]string {
	if n > 0 && len(values) > n {
		return values[:n]
	}
	return values
}

func firstNAnyMaps(values []map[string]any, n int) []map[string]any {
	if n > 0 && len(values) > n {
		return values[:n]
	}
	return values
}

func firstNStrings(values []string, n int) []string {
	if n > 0 && len(values) > n {
		return values[:n]
	}
	return values
}

func uniqueSeedRows(rows []map[string]any) []map[string]any {
	seen := map[string]bool{}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		key := firstNonEmpty(stringAny(row["source_code"]), stringAny(row["url"]), stringAny(row["title"]))
		if key == "" {
			key = mustJSON(row)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, row)
	}
	return out
}

func chunkMessages(values []kafka.Message, size int) [][]kafka.Message {
	if size <= 0 {
		size = len(values)
	}
	chunks := make([][]kafka.Message, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func chunkGenericEvents(values []genericEvent, size int) [][]genericEvent {
	if len(values) == 0 {
		return nil
	}
	if size <= 0 {
		size = len(values)
	}
	chunks := make([][]genericEvent, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func chunkMapRows(values []map[string]any, size int) [][]map[string]any {
	if len(values) == 0 {
		return nil
	}
	if size <= 0 {
		size = len(values)
	}
	chunks := make([][]map[string]any, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func directCountSummary(counts map[string]int) string {
	if len(counts) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", key, counts[key]))
	}
	return strings.Join(parts, ",")
}

func isLoopbackBroker(raw string) bool {
	parsed, err := url.Parse("tcp://" + raw)
	host := raw
	if err == nil && parsed.Hostname() != "" {
		host = parsed.Hostname()
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
			return fmt.Errorf("%s advertises loopback listener %s:%d; fix Kafka server KAFKA_PUBLIC_HOST/KAFKA_ADVERTISED_LISTENERS and force-recreate Kafka_Platform", label, leaderHost, partition.Leader.Port)
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
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "" || host == "localhost" || host == "0.0.0.0" || host == "::" || host == "::1" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsUnspecified()
	}
	return false
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
