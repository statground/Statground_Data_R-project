-- ClickHouse 26.1.2.11 / OLAP / 분석 전용
-- GitHub Actions 수집 실행 로그와 원천별 관측 이벤트를 TB 단위로 확장 가능한 형태로 저장하는 예시입니다.
-- AliSQL이 SSOT이며, 이 테이블은 분석/집계/모니터링 전용입니다.

CREATE TABLE IF NOT EXISTS analytics.r_community_collect_events
(
    uuid UUID COMMENT 'UUID v7, ETL/Django ingest 단계에서 생성하여 적재; ClickHouse 분석 이벤트 식별자',
    external_id String COMMENT '수집기 생성 SHA-256 외부 항목 ID 예: sha256:<64 hex>',
    source_id LowCardinality(String) COMMENT '수집 소스의 안정적 외부 식별자 예: reddit:r/rstats',
    source_type LowCardinality(String) COMMENT '소스 유형 예: official_release_notes, community_forum, social_tag',
    platform LowCardinality(String) COMMENT '플랫폼 예: reddit, mastodon, dcinside, r-project',
    host LowCardinality(String) COMMENT 'canonical_url의 호스트; OLAP 그룹핑/필터용',
    canonical_url String COMMENT '항목 대표 URL; 추적 파라미터 제거 후 저장',
    title String COMMENT '항목 제목 또는 본문 기반 표시 제목',
    author String COMMENT '공개 작성자명 또는 계정명; 없으면 빈 문자열',
    language LowCardinality(String) COMMENT '언어 코드 예: ko, en; 알 수 없으면 빈 문자열',
    tags Array(String) COMMENT '정규화 태그 배열',
    raw_json String COMMENT '수집 원본 메타데이터 JSON 문자열; OLAP 전용, SSOT 아님',
    published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT '외부 소스 게시 시각, Asia/Seoul 기준; 알 수 없으면 NULL',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT '수집 시각, Asia/Seoul 기준; 월 파티션 및 Primary Index 첫 번째 기준',
    ingest_date Date DEFAULT toDate(collected_at) COMMENT '수집 일자, Asia/Seoul 기준; 보조 필터용'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, uuid)
COMMENT 'R 언어 공식/커뮤니티 수집 이벤트 로그; OLAP 전용, SSOT 아님, AliSQL에서 정합성 관리';
