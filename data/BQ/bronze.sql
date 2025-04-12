-- Description: Create external tables for bronze dataset in BigQuery
-- please do not forget to replace the bucket path

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.departments_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.encounters_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.patients_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.providers_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.transactions_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-a/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.departments_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.encounters_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.patients_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.providers_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `avd-practice-1.Project_Bronze.transactions_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-22032025/landing/hospital-b/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------