--Creating and connecting to new database, and creating new schema.
CREATE DATABASE sketchy_dw;
\c sketchy_dw;
CREATE SCHEMA penetration_report;

--Creating new tables to hold file data, as well as dimension table holding key information for joining enrollment data from aamc.org with provided university table info.
CREATE TABLE sketchy_dw.penetration_report.subscriptions (id integer PRIMARY KEY,
user_id integer NOT NULL,
term_start timestamp,
term_end timestamp,
transaction_type CHAR(10));

CREATE TABLE sketchy_dw.penetration_report.users (id integer PRIMARY KEY,
name varchar(70),
program_year char(13),
university_id integer,
created_at_1 timestamp,
created_at_2 timestamp,
updated_at timestamp
);

CREATE TABLE sketchy_dw.penetration_report.universities (id integer PRIMARY KEY,
name varchar(125),
short_name varchar(30),
country varchar(50),
state varchar(40));

CREATE TABLE sketchy_dw.penetration_report.enrollment (
State char(2),
Medical_School varchar(50),
"2019_2020_school_year" char(5),
"2020_2021_school_year" char(5),
"2021_2022_school_year" char(5),
"2022_2023_school_year" char(5),
"2023_2024_school_year" char(5));

CREATE TABLE sketchy_dw.penetration_report.uni_enroll_matched (id integer PRIMARY KEY,
university_id integer,
university_name varchar(125),
enroll_id integer,
enroll_name varchar(50));

--Copying data into newly created tables. Some files are unchanged from the csv's provided; some have been cleaned up in python; and one (uni_enroll_matched) has been generated without a single base file source.
COPY penetration_report.subscriptions FROM '/files/1_source_files/subscriptions.csv' DELIMITER ',' CSV HEADER;
COPY penetration_report.users FROM '/files/2_modified_files/users_modified.csv' DELIMITER ',' CSV HEADER;
COPY penetration_report.universities FROM '/files/1_source_files/universities.csv' DELIMITER ',' CSV HEADER;
COPY penetration_report.enrollment FROM '/files/2_modified_files/2023_FACTS_Table_B_1_point_2_MODIFIED.csv' DELIMITER ',' CSV HEADER;
COPY penetration_report.uni_enroll_matched FROM '/files/3_generated_files/uni_enroll_matched.csv' DELIMITER ',' CSV HEADER;

--Creating id for enrollment.
ALTER TABLE sketchy_dw.penetration_report.enrollment ADD COLUMN id serial PRIMARY KEY;

--CTEs used to create table "paid_subs_by_year_and_school", a critical table in the view "penetration_report_view".
WITH subscriptions_filtered AS (
    SELECT
        id,
        user_id,
        term_start,
        term_end
    FROM penetration_report.subscriptions
    WHERE
        transaction_type = 'PAID' AND
        (term_end - term_start) > '0 days'::interval
),
universities_filtered AS (
    SELECT
        id,
        name
    FROM penetration_report.universities un
    WHERE
        un.country = 'United States of America' AND
        lower(un.name) like '%med%'
),
uni_start_end_subs AS (
    SELECT DISTINCT
        un.name,
        un.id,
        date_part('year', su.term_start) AS term_start_year,
        date_part('year', su.term_end) AS term_end_year,
        count(su.id) OVER (PARTITION BY un.name, date_part('year', su.term_start), date_part('year', su.term_end)) AS total_subs
    FROM subscriptions_filtered su
    INNER JOIN penetration_report.users us
        ON su.user_id = us.id
    INNER JOIN universities_filtered un
        ON us.university_id = un.id
),
summed_subs AS (
    SELECT
        name,
        id,
        term_start_year,
        term_end_year,
        CASE WHEN (term_start_year IN (2019,2020) OR term_end_year IN (2019,2020) OR (term_start_year <= 2018 AND term_end_year >= 2021)) THEN SUM(total_subs) OVER (PARTITION BY name, term_start_year, term_end_year) END AS paid_subs_2019_2020,
        CASE WHEN (term_start_year IN (2020,2021) OR term_end_year IN (2020,2021) OR (term_start_year <= 2019 AND term_end_year >= 2022)) THEN SUM(total_subs) OVER (PARTITION BY name, term_start_year, term_end_year) END AS paid_subs_2020_2021,
        CASE WHEN (term_start_year IN (2021,2022) OR term_end_year IN (2021,2022) OR (term_start_year <= 2020 AND term_end_year >= 2023)) THEN SUM(total_subs) OVER (PARTITION BY name, term_start_year, term_end_year) END AS paid_subs_2021_2022,
        CASE WHEN (term_start_year IN (2022,2023) OR term_end_year IN (2022,2023) OR (term_start_year <= 2021 AND term_end_year >= 2024)) THEN SUM(total_subs) OVER (PARTITION BY name, term_start_year, term_end_year) END AS paid_subs_2022_2023,
        CASE WHEN (term_start_year IN (2023,2024) OR term_end_year IN (2023,2024) OR (term_start_year <= 2022 AND term_end_year >= 2025)) THEN SUM(total_subs) OVER (PARTITION BY name, term_start_year, term_end_year) END AS paid_subs_2023_2024
    FROM uni_start_end_subs
)
SELECT DISTINCT
    name AS university_name,
    id AS university_id,
    sum(paid_subs_2019_2020) OVER (partition by name) AS total_paid_subs_2019_2020,
    sum(paid_subs_2020_2021) OVER (partition by name) AS total_paid_subs_2020_2021,
    sum(paid_subs_2021_2022) OVER (partition by name) AS total_paid_subs_2021_2022,
    sum(paid_subs_2022_2023) OVER (partition by name) AS total_paid_subs_2022_2023,
    sum(paid_subs_2023_2024) OVER (partition by name) AS total_paid_subs_2023_2024
INTO penetration_report.paid_subs_by_year_and_school
FROM summed_subs;

--The view I am creating to use in the report. Users could either connect directly to the view, or they can create a file from this view and use that as the source.
CREATE VIEW penetration_report.penetration_report_view AS
SELECT
    ps.*,
    en.state,
    en.medical_school,
    en."2019_2020_school_year" AS "2019_2020_school_year_enrollment",
    en."2020_2021_school_year" AS "2020_2021_school_year_enrollment",
    en."2021_2022_school_year" AS "2021_2022_school_year_enrollment",
    en."2022_2023_school_year" AS "2022_2023_school_year_enrollment",
    en."2023_2024_school_year" AS "2023_2024_school_year_enrollment",
    en.id AS enrollment_id
FROM penetration_report.paid_subs_by_year_and_school ps 
INNER JOIN penetration_report.uni_enroll_matched uem
    ON ps.university_id = uem.university_id
INNER JOIN penetration_report.enrollment en
    ON en.id = uem.enroll_id
