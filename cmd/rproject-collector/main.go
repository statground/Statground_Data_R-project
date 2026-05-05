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
	defaultWebRTopic    = "webr.events"
	userAgent           = "StatgroundBot/1.0 (+https://www.statground.net; R ecosystem collector)"
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
	depVersionRE   = regexp.MustCompile(`\s*\(.*?\)\s*`)
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
}

type cranRecord map[string]string

type rssFeed struct {
	Channel struct {
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

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: rproject-collector <package|youtube|mastodon> [flags]"))
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "package":
		err = runPackage(ctx, os.Args[2:])
	case "youtube":
		err = runYouTube(ctx, os.Args[2:])
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
	newsLimit := fs.Int("package-news-limit", envInt("RPKG_PACKAGE_NEWS_LIMIT", 50), "package NEWS page limit")
	websiteLimit := fs.Int("website-limit", envInt("RPKG_R_WEBSITE_LIMIT", 0), "website seed limit")
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
		"r-core-news",
		"package-news",
		"bioconductor",
		"runiverse",
		"r-websites",
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
			newsLimit:      *newsLimit,
			websiteLimit:   *websiteLimit,
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

type packageJobLimits struct {
	metadataLimit int
	downloadTop    int
	reverseLimit   int
	checkLimit     int
	archiveLimit   int
	newsLimit      int
	websiteLimit   int
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
	case "r-websites":
		return collectRWebsites(limits.websiteLimit)
	default:
		return nil, fmt.Errorf("unknown package job %q", job)
	}
}

func runYouTube(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("youtube", flag.ExitOnError)
	job := fs.String("job", envString("R_YOUTUBE_JOB", "all"), "all, seeds, pages, search, links")
	topic := fs.String("topic", envString("R_YOUTUBE_KAFKA_TOPIC", defaultYouTubeTopic), "Kafka topic")
	dryRun := fs.Bool("dry-run", envBool("DRY_RUN", false), "print events instead of Kafka")
	seedLimit := fs.Int("seed-limit", envInt("R_YOUTUBE_SEED_LIMIT", 0), "seed limit")
	pageLimit := fs.Int("page-limit", envInt("R_YOUTUBE_PAGE_LIMIT", 30), "HTML page fetch limit")
	fs.Parse(args)

	pub := newPublisher(*topic, "statground-ryoutube-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	jobs := expandJobs(*job, []string{"seeds", "pages", "search", "links"})
	total := 0
	for _, currentJob := range jobs {
		events, err := collectYouTubeJob(currentJob, *seedLimit, *pageLimit)
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
	fs.Parse(args)

	pub := newPublisher(*topic, "statground-mastodon-go-collector", *dryRun)
	if err := pub.validate(ctx); err != nil {
		return err
	}
	events, err := collectMastodonRSS(*instance, *acct, *limit)
	if err != nil {
		return err
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

func collectRWebsites(limit int) ([]genericEvent, error) {
	urls := configuredURLs("RPKG_R_WEBSITE_URLS", defaultWebsites)
	mentions := splitCSV(envString("RPKG_WEBSITE_MENTION_PACKAGES", "ggplot2,dplyr,shiny,tidymodels,quarto,data.table,tidyverse"))
	events := make([]genericEvent, 0)
	for idx, targetURL := range urls {
		if limit > 0 && idx >= limit {
			break
		}
		payload := fetchWebsitePayload(targetURL)
		events = append(events, newGenericEvent("rpkg.r_website.fetch_snapshot.v1", "r_website_seed_fetcher", targetURL, "R-Web", "", "", "", payload))
		text := strings.ToLower(stringAny(payload["page_text"]))
		for _, packageName := range mentions {
			if packageName != "" && strings.Contains(text, strings.ToLower(packageName)) {
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
		}
	}
	return events, nil
}

func collectYouTubeJob(job string, seedLimit, pageLimit int) ([]genericEvent, error) {
	seeds, err := loadYouTubeSeedsFromClickHouse(seedLimit)
	if err != nil {
		return nil, err
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

func youtubeSearchEvents(seeds []map[string]any, limit int) []genericEvent {
	queries := youtubeSearchQueries(seeds)
	events := make([]genericEvent, 0)
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
				videoPayload := map[string]any{
					"youtube_video_id":       result["parsed_video_id"],
					"youtube_channel_id":     "",
					"playlist_ids_json":      "[]",
					"video_title":            "",
					"video_description":      "",
					"canonical_url":          result["result_url"],
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
					"language_code":          "und",
					"tags_json":              "[]",
					"thumbnail_urls_json":    "{}",
					"channel_title":          "",
					"privacy_status":         "",
					"source_method":          "youtube_public_search_html_no_data_api",
					"source_tag":             "r_project_ecosystem_youtube",
					"source_category":        "search_result",
					"source_confidence":      "search_html_discovered",
					"collection_status":      "collected",
				}
				events = append(events, newGenericEvent("r.youtube.video.snapshot.v1", "youtube_public_search_html", result["result_url"], "R-YouTube", "", "", "", videoPayload))
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

func collectMastodonRSS(instance, acct string, limit int) ([]webREvent, error) {
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
		events = append(events, newWebREvent("webr.mastodon.raw.v1", statusURL, rawPayload, createdAt))
		boardTitle := firstNonEmpty(stripTags(item.Title), firstWords(contentText, 12), "R Foundation")
		boardPayload := map[string]any{
			"uuid":          rowUUID,
			"title":         boardTitle,
			"content":       safeHTML(firstNonEmpty(contentText, boardTitle)),
			"active":        1,
			"created_at":    formatKST(createdAt),
			"updated_at":    nil,
			"language_code": "ko",
			"created_log": map[string]any{
				"type":               "mastodon_board_rss_fallback",
				"source":             "Statground_Data_R-project",
				"source_method":      "mastodon_public_rss_no_api",
				"translation_status": "not_translated",
				"raw_status_url":     statusURL,
			},
			"updated_log": nil,
		}
		events = append(events, newWebREvent("webr.mastodon.board.v1", statusURL, boardPayload, createdAt))
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

func extractYouTubeURLs(text string) []string {
	out := make([]string, 0)
	for _, match := range youtubeURLRE.FindAllString(text, -1) {
		out = append(out, strings.TrimRight(match, ".,);]"))
	}
	return firstNStrings(uniqueStrings(out), 80)
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
	dialer := &kafka.Dialer{ClientID: p.clientID, Timeout: 10 * time.Second}
	if p.username != "" || p.password != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: p.username, Password: p.password}
	}
	if p.usesTLS() {
		dialer.TLS = kafkaTLSConfig()
	}
	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(probeCtx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("kafka preflight failed for %q: %w", p.brokers[0], err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(p.topic)
	if err != nil {
		return fmt.Errorf("kafka metadata read failed topic=%s: %w", p.topic, err)
	}
	for _, partition := range partitions {
		if isLoopbackHost(partition.Leader.Host) {
			return fmt.Errorf("kafka metadata advertises loopback listener %s:%d", partition.Leader.Host, partition.Leader.Port)
		}
	}
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
		AllowAutoTopicCreation: false,
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
		return cfg, errors.New("CH_HOST or CLICKHOUSE_HOST is required for DB-backed YouTube seeds")
	}
	if cfg.User == "" {
		return cfg, errors.New("CH_USER or CLICKHOUSE_USER is required for DB-backed YouTube seeds")
	}
	if cfg.Password == "" {
		return cfg, errors.New("CH_PASSWORD or CLICKHOUSE_PASSWORD is required for DB-backed YouTube seeds")
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
	return value == "1" || value == "true" || value == "yes" || value == "y"
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
