{% macro pct(colname) -%}
    COALESCE(CAST(d."{{ colname }}" AS REAL), 0)
{%- endmacro %}

WITH hs_scope AS (
    SELECT DISTINCT course.LOCATION_ID
    FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course
    {% if high_school_only %}
    WHERE course.CURR_GRADE_LVL IN ('009','010','011','012','09','10','11','12')
    {% endif %}
)

SELECT
    '{{ school_year_dash }}' AS "School Year",
    d.LOCATION_ID,
    d.LOCATION_NAME AS "School Name",
    d.DISTRICT_NAME AS "District Name",

    d."School Number (NCES)" AS "School Number (NCES)",
    d."District Number (NCES)" AS "District Number (NCES)",
    d."Lowest Grade Level Served" AS "Lowest Grade Level Served",
    d."Highest Grade Level Served" AS "Highest Grade Level Served",

    CAST(ROUND(d.ENROLL) AS INTEGER) AS ENROLL,

    /* Race / Ethnicity */
    CAST(ROUND(d.ENROLL * ({{ pct("AI/AN") }}        / 100.0)) AS INTEGER) AS "Amer. Indian or Alaska Native",
    CAST(ROUND(d.ENROLL * ({{ pct("ASIAN") }}        / 100.0)) AS INTEGER) AS "Asian",
    CAST(ROUND(d.ENROLL * ({{ pct("BLK/AF_AMER") }}  / 100.0)) AS INTEGER) AS "Black or African Amer.",
    CAST(ROUND(d.ENROLL * ({{ pct("HISP") }}         / 100.0)) AS INTEGER) AS "Hisp. or Latino",
    CAST(ROUND(d.ENROLL * ({{ pct("NH/PI") }}        / 100.0)) AS INTEGER) AS "Native Hawaiian or Pacific Islander",
    CAST(ROUND(d.ENROLL * ({{ pct("MULTI-RACIAL") }} / 100.0)) AS INTEGER) AS "Two or more races",
    CAST(ROUND(d.ENROLL * ({{ pct("WHITE") }}        / 100.0)) AS INTEGER) AS "White",

    /* Gender */
    CAST(ROUND(d.ENROLL * ({{ pct("F") }}            / 100.0)) AS INTEGER) AS "Girls",
    CAST(ROUND(d.ENROLL * ({{ pct("M") }}            / 100.0)) AS INTEGER) AS "Boys",

    /* Programs / Needs */
    CAST(ROUND(d.ENROLL * ({{ pct("PLAN_504") }}     / 100.0)) AS INTEGER) AS "S504",

    /* Disability = SPL_ED + AUT + ... + OTHER */
    CAST(ROUND(d.ENROLL * (
        (
            {{ pct("SPL_ED") }} +
            {{ pct("AUT") }} +
            {{ pct("DEAF-BLIND") }} +
            {{ pct("DEV DELAY") }} +
            {{ pct("EMOTL DIST") }} +
            {{ pct("GIFT-DIS") }} +
            {{ pct("HI") }} +
            {{ pct("INF-TOD") }} +
            {{ pct("INTELL DIS") }} +
            {{ pct("MULTI") }} +
            {{ pct("ORTHO") }} +
            {{ pct("SPEC LRN DIS") }} +
            {{ pct("SPCH LANG") }} +
            {{ pct("TBI") }} +
            {{ pct("VI") }} +
            {{ pct("OTHER") }}
        ) / 100.0
    )) AS INTEGER) AS "Disability",

    /* Eco. Dis. = ED + HMLS */
    CAST(ROUND(d.ENROLL * (({{ pct("ED") }} + {{ pct("HMLS") }}) / 100.0)) AS INTEGER) AS "Eco. Dis.",

    /* English Learner */
    CAST(ROUND(d.ENROLL * ({{ pct("EL") }} / 100.0)) AS INTEGER) AS "EL"

FROM "{{ school_year_splat }}_Student_School_Demographics" AS d
{% if high_school_only %}
JOIN hs_scope s
  ON d.LOCATION_ID = s.LOCATION_ID
{% endif %}
ORDER BY "District Name", "School Name";
