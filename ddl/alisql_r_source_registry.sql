-- AliSQL 8.0.44-35 / OLTP / SSOT
-- GitHub Actions 결과를 Django ingest 단계에서 검증한 뒤 upsert하는 기준 테이블 예시입니다.

CREATE TABLE IF NOT EXISTS r_source_registry (
    uuid VARCHAR(36) NOT NULL DEFAULT (udf.uuid7()) COMMENT 'UUID v7 (36 chars with hyphens), 플랫폼 전역 식별자',
    source_id VARCHAR(160) NOT NULL COMMENT '수집 소스의 안정적 외부 식별자 예: reddit:r/rstats',
    source_name VARCHAR(255) NOT NULL COMMENT '관리자 화면 표시용 소스명',
    source_type VARCHAR(80) NOT NULL COMMENT '소스 유형 예: official_release_notes, community_forum, social_tag',
    platform VARCHAR(80) NOT NULL COMMENT '플랫폼 예: reddit, mastodon, dcinside, r-project',
    source_url VARCHAR(1024) NOT NULL COMMENT '수집 소스 대표 URL',
    language VARCHAR(16) NULL COMMENT '주 언어 코드 예: ko, en; 알 수 없으면 NULL',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '상태값: 1=active, 0=inactive, 9=archived',
    last_collected_at DATETIME(3) NULL COMMENT '마지막 정상 수집 시각, Asia/Seoul 기준',
    last_error_at DATETIME(3) NULL COMMENT '마지막 수집 오류 시각, Asia/Seoul 기준',
    last_error_message VARCHAR(1000) NULL COMMENT '마지막 수집 오류 요약 메시지',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '생성 시각, Asia/Seoul 기준',
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '수정 시각, Asia/Seoul 기준',
    PRIMARY KEY (uuid),
    UNIQUE KEY uk_r_source_registry_source_id (source_id),
    KEY idx_r_source_registry_platform_status (platform, status),
    KEY idx_r_source_registry_last_collected_at (last_collected_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='R 언어 공식/커뮤니티 수집 소스 레지스트리; AliSQL SSOT, UUID v7 사용';
