SELECT
    '{{ school_year_dash }}' AS "School Year",
    course.LOCATION_ID,
    demographics.LOCATION_NAME AS "School Name",
    demographics.DISTRICT_NAME AS "District Name",

    demographics."School Number (NCES)" AS "School Number (NCES)",
    demographics."District Number (NCES)" AS "District Number (NCES)",
    demographics."Lowest Grade Level Served" AS "Lowest Grade Level Served",
    demographics."Highest Grade Level Served" AS "Highest Grade Level Served",

    COUNT(*) AS ENROLL,

    COUNT(CASE WHEN course.RACE_ETH = 'AI/AN' THEN 1 END) AS "Amer. Indian or Alaska Native",
    COUNT(CASE WHEN course.RACE_ETH = 'ASIAN' THEN 1 END) AS "Asian",
    COUNT(CASE WHEN course.RACE_ETH = 'BLK/AF_AMER' THEN 1 END) AS "Black or African Amer.",
    COUNT(CASE WHEN course.RACE_ETH = 'HISP' THEN 1 END) AS "Hisp. or Latino",
    COUNT(CASE WHEN course.RACE_ETH = 'NH/PI' THEN 1 END) AS "Native Hawaiian or Pacific Islander",
    COUNT(CASE WHEN course.RACE_ETH = 'MULTI_RACIAL' THEN 1 END) AS "Two or more races",
    COUNT(CASE WHEN course.RACE_ETH = 'WHITE' THEN 1 END) AS "White",

    COUNT(CASE WHEN course.STUDENT_GENDER_CD = 'F' THEN 1 END) AS "Girls",
    COUNT(CASE WHEN course.STUDENT_GENDER_CD = 'M' THEN 1 END) AS "Boys",

    COUNT(CASE WHEN course.PLAN_504 = 'Yes' THEN 1 END) AS "S504",

    COUNT(CASE WHEN course.SPL_ED_STATUS = 'Y' OR course.PRIMARY_DISABIITY IS NOT NULL THEN 1 END) AS "Disability",

    COUNT(CASE WHEN course.POVERTY_CODE = 'Y' OR course.HOMELESS_STATUS = 'Y' THEN 1 END) AS "Eco. Dis.",
    COUNT(CASE WHEN course.EL_STATUS = 'Y' THEN 1 END) AS "EL"

FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course
INNER JOIN "{{ school_year_splat }}_Student_School_Demographics" AS demographics
    ON course.LOCATION_ID = demographics.LOCATION_ID

{% if high_school_only %}
WHERE course.CURR_GRADE_LVL IN ('009', '010', '011', '012') 
{% endif %}

GROUP BY course.LOCATION_ID;
