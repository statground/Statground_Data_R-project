package main

import (
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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	defaultPackageTopic = "rpkg.events"
	defaultYouTubeTopic = "r.youtube.events"
	defaultCommunityTopic = "r.community.events"
	defaultWebRTopic    = "webr.events"
	userAgent           = "StatgroundBot/1.0 (+https://www.statground.net; R ecosystem collector)"
	youtubeBoilerplateDescription = "Enjoy the videos and music you love, upload original content, and share it all with friends, family, and the world on YouTube."
)

var (
	tagRE          = regexp.MustCompile(`(?is)<[^>]+>`)
	spaceRE        = regexp.MustCompile(`[ \t\r\n\x{00a0}]+`)
	trRE           = regexp.MustCompile(`(?is)<tr\b[^>]*>(.*?)</tr>`)
	cellRE         = regexp.MustCompile(`(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>`)
	linkRE         = regexp.MustCompile(`(?is)<a\b[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>`)
	titleRE        = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
	metaRE         = regexp.MustCompile(`(?is)<meta\b([^>]*)>`)
	attrRE         = regexp.MustCompile(`(?is)\s([a-zA-Z_:][-a-zA-Z0-9_:]*)\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))`)
	headingRE      = regexp.MustCompile(`(?is)<h[1-4]\b[^>]*>(.*?)</h[1-4]>`)
	listItemRE     = regexp.MustCompile(`(?is)<li\b[^>]*>(.*?)</li>`)
	youtubeURLRE   = regexp.MustCompile(`https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s"'<>]+`)
	ytWatchRE      = regexp.MustCompile(`(?:/watch\?v=|watch\\u003fv=|watch\\\?v=)([A-Za-z0-9_-]{11})`)
	ytChannelRE    = regexp.MustCompile(`/(channel/[A-Za-z0-9_-]+|@[A-Za-z0-9._-]+|c/[A-Za-z0-9._-]+|user/[A-Za-z0-9._-]+)`)
	ytPlaylistRE   = regexp.MustCompile(`(?:/playlist\?list=|playlist\\u003flist=|playlist\\\?list=)([A-Za-z0-9_-]+)`)
	githubRepoRE   = regexp.MustCompile(`(?i)^https?://(?:www\.)?github\.com/([^/\s?#]+)/([^/\s?#]+)`)
	cranPackageLinkRE = regexp.MustCompile(`(?i)(?:/web/packages/|\.\./packages/)([^/\s?#"']+)/?`)
	isoDurationRE  = regexp.MustCompile(`^P(?:\d+D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$`)
	depVersionRE   = regexp.MustCompile(`\s*\(.*?\)\s*`)
	boardURLRE      = regexp.MustCompile(`(?i)\b(?:https?://|www\.)[^\s<>()"']+`)
	boardMdLinkRE   = regexp.MustCompile(`\[([^\]]+)\]\((?:https?://|www\.)[^)]+\)`)
	boardFirstTagRE = regexp.MustCompile(`(?is)<\s*(h2|h3|p|ul|ol|li|strong|em|code|pre|blockquote)\b`)
	boardAnyTagRE   = regexp.MustCompile(`(?is)</?\s*([a-zA-Z0-9]+)\b[^>]*>`)
	boardAnchorRE   = regexp.MustCompile(`(?is)<a\b[^>]*>(.*?)</a>`)
	boardAnchorOnlyRE = regexp.MustCompile(`(?is)^<a\b[^>]*>(.*?)</a>$`)
	sitemapLocRE    = regexp.MustCompile(`(?is)<loc>\s*([^<]+?)\s*</loc>`)
	statusOrder     = []string{"ERROR", "FAIL", "WARNING", "NOTE", "OK"}
	newsCandidates  = []string{"news/news.html", "news.html"}
	defaultWebsites = []string{
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
)

var boardAllowedTags = map[string]bool{
	"h2": true, "h3": true, "p": true, "ul": true, "ol": true, "li": true,
	"strong": true, "em": true, "code": true, "pre": true, "blockquote": true,
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
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Secure   bool
	Timeout  time.Duration
}

type publisher struct {
	topic        string
	brokers      []string
	username     string
	password     string
	security     string
	clientID     string
	dryRun       bool
	writeTimeout time.Duration
	chunkSize    int
	createTopic  bool
	partitions   int
	replicas     int
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
		Title string `xml:"title"`
		Link  string `xml:"link"`
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
		fatal(errors.New("usage: rproject-collector <package|youtube|community|mastodon> [flags]"))
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
		"bioconductor",
		"runiverse",
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
	for _, currentJob := range jobs {
		events, err := collectPackageJob(currentJob, getRecords, packageJobLimits{
			metadataLimit: *metadataLimit,
			downloadTop:    *downloadTop,
			reverseLimit:   *reverseLimit,
			checkLimit:     *checkLimit,
			archiveLimit:   *archiveLimit,
			taskViewLimit:  *taskViewLimit,
			newsLimit:      *newsLimit,
			websiteLimit:   *websiteLimit,
			websiteCandidateLimit: *websiteCandidateLimit,
			websiteFeedLimit: *websiteFeedLimit,
			websiteLinkLimit: *websiteLinkLimit,
			websiteSitemapLimit: *websiteSitemapLimit,
			githubLimit:    *githubLimit,
			osvLimit:       *osvLimit,
			bibliometricLimit: *bibliometricLimit,
		})
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

func runCommunity(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("community", flag.ExitOnError)
	jsonlPath := fs.String("jsonl", envString("R_COMMUNITY_JSONL", "data/collected/r/latest.jsonl"), "normalized R Community JSONL path")
	topic := fs.String("topic", envString("R_COMMUNITY_KAFKA_TOPIC", defaultCommunityTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	limit := fs.Int("limit", envInt("R_COMMUNITY_EVENT_LIMIT", 0), "max JSONL rows to publish; 0 means all")
	fs.Parse(args)

	events, err := readCommunityJSONLEvents(*jsonlPath, *limit)
	if err != nil {
		return err
	}
	pub := newPublisher(*topic, "statground-rcommunity-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	if err := pub.publishGeneric(ctx, events); err != nil {
		return err
	}
	fmt.Printf("published=%d topic=%s jsonl=%s\n", len(events), *topic, *jsonlPath)
	return nil
}

func readCommunityJSONLEvents(path string, limit int) ([]genericEvent, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	events := make([]genericEvent, 0)
	for lineNo, rawLine := range bytes.Split(body, []byte("\n")) {
		line := bytes.TrimSpace(rawLine)
		if len(line) == 0 {
			continue
		}
		if limit > 0 && len(events) >= limit {
			break
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineNo+1, err)
		}
		events = append(events, communityRowEvent(row))
	}
	return events, nil
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

type packageJobLimits struct {
	metadataLimit int
	downloadTop    int
	reverseLimit   int
	checkLimit     int
	archiveLimit   int
	taskViewLimit  int
	newsLimit      int
	websiteLimit   int
	websiteCandidateLimit int
	websiteFeedLimit int
	websiteLinkLimit int
	websiteSitemapLimit int
	githubLimit    int
	osvLimit       int
	bibliometricLimit int
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
	case "bioconductor":
		return collectBioconductor()
	case "runiverse":
		return collectRUniverse()
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
	job := fs.String("job", envString("R_YOUTUBE_JOB", "all"), "all, seeds, pages, search, links, videos, backfill-metadata")
	topic := fs.String("topic", envString("R_YOUTUBE_KAFKA_TOPIC", defaultYouTubeTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	seedLimit := fs.Int("seed-limit", envInt("R_YOUTUBE_SEED_LIMIT", 0), "seed limit")
	pageLimit := fs.Int("page-limit", envInt("R_YOUTUBE_PAGE_LIMIT", 30), "HTML page fetch limit")
	videoLimit := fs.Int("video-limit", envInt("R_YOUTUBE_VIDEO_LIMIT", 30), "YouTube video metadata enrichment limit")
	backfillLimit := fs.Int("backfill-limit", envInt("R_YOUTUBE_BACKFILL_LIMIT", 30), "existing weak current video metadata backfill limit")
	fs.Parse(args)

	pub := newPublisher(*topic, "statground-ryoutube-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	jobs := expandJobs(*job, []string{"seeds", "pages", "search", "links", "videos", "backfill-metadata"})
	total := 0
	for _, currentJob := range jobs {
		events, err := collectYouTubeJob(currentJob, *seedLimit, *pageLimit, *videoLimit, *backfillLimit)
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
			"package":          packageName,
			"version":          version,
			"title":            record["Title"],
			"description":      record["Description"],
			"license":          record["License"],
			"maintainer":       record["Maintainer"],
			"author":           record["Author"],
			"authors_at_r":     record["Authors@R"],
			"depends":          record["Depends"],
			"imports":          record["Imports"],
			"suggests":         record["Suggests"],
			"linking_to":       record["LinkingTo"],
			"enhances":         record["Enhances"],
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
					"snapshot_date":      snapshotDate,
					"source":             "CRAN",
					"from_repository":    "CRAN",
					"from_package":       fromPackage,
					"from_version":       record["Version"],
					"to_package":         dep.name,
					"dependency_type":    field,
					"dependency_spec":    dep.spec,
					"source_method":      "cran_packages_gz_dependency_parser",
					"collection_status":  "collected",
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
	universes := splitCSV(envString("RPKG_RUNIVERSE_UNIVERSES", "r-universe,ropensci,tidyverse"))
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
			"package":              packageName,
			"version":              record["Version"],
			"query":                query,
			"result_count":         intString(meta["count"]),
			"top_work_id":          stringAny(top["id"]),
			"top_work_title":       stringAny(top["title"]),
			"top_work_year":        intString(top["publication_year"]),
			"top_work_cited_by":    intString(top["cited_by_count"]),
			"source_method":        "openalex_works_search",
			"collection_status":    "collected",
			"confidence":           "phrase_search",
		}
		events = append(events, newGenericEvent("rpkg.bibliometric.mention_snapshot.v1", "openalex_works_search", sourceURL, "OpenAlex", packageName, record["Version"], "", payload))
		count++
	}
	return events, nil
}

func collectYouTubeJob(job string, seedLimit, pageLimit, videoLimit, backfillLimit int) ([]genericEvent, error) {
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
FROM Data_R_Youtube_Service.r_youtube_source_seed_current
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
FROM Data_R_Youtube_Service.v_webr_official_youtube
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
    '' AS payload_hash,
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
    FROM Data_R_Youtube_Service.r_youtube_video_current FINAL
    WHERE active = 1
      AND notEmpty(youtube_video_id)
)
GROUP BY youtube_video_id, source_tag, uuid_article
ORDER BY last_collected_at ASC, youtube_video_id
LIMIT %d
FORMAT JSONEachRow`, queryLimit)
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
				"youtube_video_id":      ref["parsed_video_id"],
				"youtube_channel_id":    "",
				"playlist_ids_json":     "[]",
				"video_title":           stringAny(page["title"]),
				"video_description":     stringAny(page["description"]),
				"canonical_url":         targetURL,
				"thumbnail_url":         stringAny(page["og_image"]),
				"published_at":          "",
				"duration_seconds":      "0",
				"view_count":            "0",
				"like_count":            "0",
				"comment_count":         "0",
				"favorite_count":        "0",
				"caption_available":     "0",
				"default_audio_language": "",
				"default_language":      "",
				"language_code":         firstNonEmpty(stringAny(seedPayload["language_hint"]), "und"),
				"tags_json":             "[]",
				"thumbnail_urls_json":   "{}",
				"channel_title":         stringAny(seedPayload["title"]),
				"privacy_status":        "",
				"source_method":         "youtube_public_html_no_data_api",
				"source_tag":            "r_project_ecosystem_youtube",
				"source_category":       stringAny(seedPayload["category"]),
				"source_confidence":     firstNonEmpty(stringAny(seedPayload["source_confidence"]), "html_discovered"),
				"collection_status":     "collected",
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
	return newGenericEvent("r.youtube.quota.usage.v1", "youtube_data_api_v3", sourceURL, "R-YouTube", "", "", "", map[string]any{
		"quota_date":        time.Now().UTC().Format("2006-01-02"),
		"api_key_alias":     envString("YOUTUBE_API_KEY_ALIAS", "default"),
		"method_name":       "videos.list",
		"quota_cost":        "1",
		"request_count":     "1",
		"quota_units_used":  "1",
		"source_method":     "youtube_data_api_v3_videos_list",
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

func fetchYouTubeOEmbedPayload(canonicalURL string) (map[string]any, error) {
	sourceURL := "https://www.youtube.com/oembed?format=json&url=" + url.QueryEscape(canonicalURL)
	var decoded map[string]any
	if err := fetchJSON(sourceURL, &decoded); err != nil {
		return nil, err
	}
	return map[string]any{
		"video_title":    stringAny(decoded["title"]),
		"thumbnail_url":  stringAny(decoded["thumbnail_url"]),
		"channel_title":  stringAny(decoded["author_name"]),
		"canonical_url":  canonicalURL,
		"source_oembed":  sourceURL,
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
	events = append(events, newWebREvent("webr.mastodon.log.v1", sourceURL, map[string]any{
		"uuid":          uuid7(),
		"created_at":    formatKST(started),
		"language_code": "en",
		"created_log": map[string]any{
			"type":          "mastodon_pipeline",
			"stage":         "rss_started",
			"instance":      instance,
			"acct":          acct,
			"source_method": "mastodon_public_rss_no_api",
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
			"source_method":          "mastodon_public_rss_no_api",
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
						"source_method": "mastodon_public_rss_no_api",
					},
				}, nowKST()))
				count++
				continue
			}
			boardTitle = translatedTitle
			boardContent = translatedContent
		}
		boardPayload := mastodonBoardPayload(rowUUID, statusURL, statusID, createdAt, time.Time{}, boardTitle, boardContent)
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
			"source_method": "mastodon_public_rss_no_api",
		},
	}, done))
	return events, nil
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
    FROM Data_R_Project_Mastodon_Raw.raw
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
FROM Data_R_Project_Mastodon_Service.v_r_foundation_board
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
FROM Data_R_Project_Mastodon_Raw.raw AS r
LEFT JOIN
(
    SELECT
        uuid,
        toString(created_log) AS board_log
    FROM Data_R_Project_Mastodon_Service.v_r_foundation_board
    WHERE active != 0
      AND language_code = 'ko'
) AS b ON b.uuid = r.uuid
WHERE r.active != 0
  AND r.status_url NOT IN
  (
      SELECT status_url
      FROM Data_R_Project_Mastodon_Service.v_r_foundation_board
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
	events = append(events, newWebREvent("webr.mastodon.log.v1", "clickhouse://Data_R_Project_Mastodon_Raw.raw", map[string]any{
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
	events = append(events, newWebREvent("webr.mastodon.log.v1", "clickhouse://Data_R_Project_Mastodon_Raw.raw", map[string]any{
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
	return isBadYouTubeMetadataValue(payload["video_title"]) ||
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
		case "video_title", "video_description", "thumbnail_url", "youtube_channel_id", "channel_title", "published_at", "privacy_status":
			dst[key] = value
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
	if isBadYouTubeMetadataValue(payload["video_title"]) && videoID != "" {
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
		topic:        topic,
		brokers:      splitCSV(firstNonEmpty(os.Getenv("KAFKA_BROKERS"), os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))),
		username:     firstNonEmpty(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_EXTERNAL_USER")),
		password:     firstNonEmpty(os.Getenv("KAFKA_PASSWORD"), os.Getenv("KAFKA_EXTERNAL_PASSWORD")),
		security:     envString("KAFKA_SECURITY_PROTOCOL", ""),
		clientID:     envString("KAFKA_CLIENT_ID", clientID),
		dryRun:       dryRun,
		writeTimeout: time.Duration(envInt("KAFKA_WRITE_TIMEOUT", 60)) * time.Second,
		chunkSize:    maxInt(1, envInt("KAFKA_WRITE_CHUNK_SIZE", 100)),
		createTopic:  envBool("KAFKA_CREATE_TOPIC", envBool("KAFKA_ALLOW_TOPIC_CREATE", true)),
		partitions:   maxInt(1, envInt("KAFKA_TOPIC_PARTITIONS", 3)),
		replicas:     maxInt(1, envInt("KAFKA_TOPIC_REPLICATION_FACTOR", 1)),
	}
}

func (p *publisher) validate(ctx context.Context) error {
	if p.dryRun {
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
	for _, partition := range partitions {
		if isLoopbackHost(partition.Leader.Host) {
			return fmt.Errorf("kafka metadata advertises loopback listener %s:%d", partition.Leader.Host, partition.Leader.Port)
		}
	}
	return nil
}

func (p *publisher) dialer() *kafka.Dialer {
	dialer := &kafka.Dialer{ClientID: p.clientID, Timeout: 10 * time.Second}
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
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if p.dryRun {
			fmt.Println(string(body))
			continue
		}
		messages = append(messages, kafka.Message{Key: []byte(eventKey(event)), Value: body, Time: time.Now()})
	}
	return p.write(ctx, messages)
}

func (p *publisher) publishWebR(ctx context.Context, events []webREvent) error {
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if p.dryRun {
			fmt.Println(string(body))
			continue
		}
		messages = append(messages, kafka.Message{Key: []byte(firstNonEmpty(event.URL, event.EventUUID)), Value: body, Time: time.Now()})
	}
	return p.write(ctx, messages)
}

func (p *publisher) write(ctx context.Context, messages []kafka.Message) error {
	if p.dryRun || len(messages) == 0 {
		return nil
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(p.brokers...),
		Topic:                  p.topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: p.createTopic,
		BatchSize:              p.chunkSize,
		BatchTimeout:           500 * time.Millisecond,
		WriteTimeout:           p.writeTimeout,
		ReadTimeout:            p.writeTimeout,
	}
	if p.clientID != "" || p.username != "" || p.password != "" || p.usesTLS() {
		transport := &kafka.Transport{ClientID: p.clientID}
		if p.username != "" || p.password != "" {
			transport.SASL = plain.Mechanism{Username: p.username, Password: p.password}
		}
		if p.usesTLS() {
			transport.TLS = kafkaTLSConfig()
		}
		writer.Transport = transport
	}
	defer writer.Close()
	for _, chunk := range chunkMessages(messages, p.chunkSize) {
		writeCtx, cancel := context.WithTimeout(ctx, p.writeTimeout+15*time.Second)
		err := writer.WriteMessages(writeCtx, chunk...)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
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
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/json,text/plain,*/*")
	client := &http.Client{Timeout: time.Duration(envInt("HTTP_TIMEOUT", 90)) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(envInt("HTTP_MAX_BYTES", 20*1024*1024))))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	return body, nil
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
		Host:     firstNonEmpty(os.Getenv("CH_HOST"), os.Getenv("CLICKHOUSE_HOST")),
		Port:     maxInt(1, envInt("CH_PORT", envInt("CLICKHOUSE_PORT", 8123))),
		User:     firstNonEmpty(os.Getenv("CH_USER"), os.Getenv("CLICKHOUSE_USER")),
		Password: firstNonEmpty(os.Getenv("CH_PASSWORD"), os.Getenv("CLICKHOUSE_PASSWORD")),
		Database: envString("CH_DATABASE", envString("CLICKHOUSE_DATABASE", "Data_R_Youtube_Service")),
		Secure:   envBool("CH_SECURE", envBool("CLICKHOUSE_SECURE", false)),
		Timeout:  time.Duration(maxInt(10, envInt("CH_TIMEOUT", envInt("CLICKHOUSE_TIMEOUT", 60)))) * time.Second,
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
	return strings.Join([]string{event.Repository, event.PackageName, event.PackageVersion, event.EventType}, ":")
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

func isLoopbackBroker(raw string) bool {
	parsed, err := url.Parse("tcp://" + raw)
	host := raw
	if err == nil && parsed.Hostname() != "" {
		host = parsed.Hostname()
	}
	return isLoopbackHost(host)
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
