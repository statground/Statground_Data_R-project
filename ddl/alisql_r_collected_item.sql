-- AliSQL 8.0.44-35 / OLTP / SSOT
-- 외부 수집 항목의 정규화 메타데이터 테이블 예시입니다.
-- 원문 전문/대용량 이벤트 분석은 ClickHouse에 별도 적재하는 것이 적합합니다.

CREATE TABLE IF NOT EXISTS r_collected_item (
    uuid VARCHAR(36) NOT NULL DEFAULT (udf.uuid7()) COMMENT 'UUID v7 (36 chars with hyphens), 플랫폼 전역 식별자',
    source_uuid VARCHAR(36) NULL COMMENT 'r_source_registry.uuid 참조 UUID v7; 소스 레지스트리 미동기화 시 NULL 허용',
    external_id CHAR(71) NOT NULL COMMENT '수집기 생성 SHA-256 외부 항목 ID 예: sha256:<64 hex>',
    source_id VARCHAR(160) NOT NULL COMMENT '수집 소스의 안정적 외부 식별자 예: reddit:r/rstats',
    source_type VARCHAR(80) NOT NULL COMMENT '소스 유형 예: official_release_notes, community_forum, social_tag',
    platform VARCHAR(80) NOT NULL COMMENT '플랫폼 예: reddit, mastodon, dcinside, r-project',
    canonical_url VARCHAR(1200) NOT NULL COMMENT '항목 대표 URL; 추적 파라미터 제거 후 저장',
    title VARCHAR(600) NOT NULL COMMENT '항목 제목 또는 본문에서 생성한 표시 제목',
    summary TEXT NULL COMMENT '정규화 요약 텍스트; HTML 제거 후 저장',
    author VARCHAR(255) NULL COMMENT '공개 작성자명 또는 계정명; 공개 데이터만 저장',
    language VARCHAR(16) NULL COMMENT '언어 코드 예: ko, en; 알 수 없으면 NULL',
    tags JSON NULL COMMENT '태그 배열 JSON; R, rstats 등 정규화 태그',
    raw_meta JSON NULL COMMENT '수집기 원본 메타데이터 JSON; API 응답 전체가 아닌 운영에 필요한 최소값',
    published_at DATETIME(3) NULL COMMENT '외부 소스 게시 시각, 원천 시각을 Asia/Seoul 기준으로 정규화; 알 수 없으면 NULL',
    collected_at DATETIME(3) NOT NULL COMMENT '수집 시각, Asia/Seoul 기준',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '상태값: 1=active, 0=hidden, 9=archived',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '생성 시각, Asia/Seoul 기준',
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '수정 시각, Asia/Seoul 기준',
    PRIMARY KEY (uuid),
    UNIQUE KEY uk_r_collected_item_external_id (external_id),
    KEY idx_r_collected_item_source_published (source_id, published_at),
    KEY idx_r_collected_item_platform_collected (platform, collected_at),
    KEY idx_r_collected_item_status_collected (status, collected_at),
    CONSTRAINT fk_r_collected_item_source_uuid
        FOREIGN KEY (source_uuid) REFERENCES r_source_registry (uuid)
        ON UPDATE RESTRICT
        ON DELETE SET NULL
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='R 언어 공식/커뮤니티 수집 항목 정규화 메타데이터; AliSQL SSOT, UUID v7 사용';
