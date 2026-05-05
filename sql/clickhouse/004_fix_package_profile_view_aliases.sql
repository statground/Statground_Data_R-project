CREATE OR REPLACE VIEW `Data_R_Package_Mart`.v_package_profile_latest
ON CLUSTER statground_cluster
AS
SELECT
    today() AS report_date,
    pc.repository AS repository,
    pc.package_name AS package_name,
    pc.latest_version,
    pc.title,
    pc.description,
    pc.maintainer,
    pc.license_text,
    pc.last_observed_at,
    ifNull(dl.downloads_30d, 0) AS downloads_30d,
    ifNull(dep.reverse_depends_count, 0) AS reverse_depends_count,
    ifNull(dep.reverse_imports_count, 0) AS reverse_imports_count,
    ifNull(ch.worst_status, '') AS cran_check_worst_status,
    ifNull(lc.lifecycle_status, 'active') AS lifecycle_status
FROM `Data_R_Package_Service`.package_current AS pc
LEFT JOIN
(
    SELECT
        repository,
        package_name,
        sumIf(downloads, download_date >= today() - 30) AS downloads_30d
    FROM `Data_R_Package_Service`.package_download_daily
    GROUP BY repository, package_name
) AS dl USING (repository, package_name)
LEFT JOIN
(
    SELECT
        repository,
        package_name,
        sum(reverse_depends_count) AS reverse_depends_count,
        sum(reverse_imports_count) AS reverse_imports_count
    FROM `Data_R_Package_Service`.v_package_reverse_dependency_daily
    GROUP BY repository, package_name
) AS dep USING (repository, package_name)
LEFT JOIN
(
    SELECT
        repository,
        package_name,
        argMax(status, status_rank) AS worst_status
    FROM `Data_R_Package_Service`.package_cran_check_current
    GROUP BY repository, package_name
) AS ch USING (repository, package_name)
LEFT JOIN `Data_R_Package_Service`.package_lifecycle_current AS lc USING (repository, package_name);

CREATE OR REPLACE VIEW `Data_R_Package_Mart`.v_ecosystem_summary_today
ON CLUSTER statground_cluster
AS
SELECT
    today() AS report_date,
    repository,
    countDistinct(package_name) AS total_packages,
    countIf(lifecycle_status = 'archived') AS archived_packages,
    countIf(cran_check_worst_status = 'ERROR') AS check_error_packages,
    countIf(cran_check_worst_status = 'WARNING') AS check_warning_packages,
    sum(downloads_30d) AS total_downloads_30d
FROM `Data_R_Package_Mart`.v_package_profile_latest
GROUP BY repository;
