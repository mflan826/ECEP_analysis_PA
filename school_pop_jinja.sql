SELECT
    '{{ school_year_dash }}' AS "School Year",
    course.LOCATION_ID,
    demo.LOCATION_NAME AS "School Name",
    demo.DISTRICT_NAME AS "District Name",

    demo."School Number (NCES)"     AS "School Number (NCES)",
    demo."District Number (NCES)"   AS "District Number (NCES)",
    demo."Lowest Grade Level Served" AS "Lowest Grade Level Served",
    demo."Highest Grade Level Served" AS "Highest Grade Level Served",

    SUM(course.N) AS ENROLL,

    SUM(CASE WHEN course.RACE_ETH = 'AI/AN'        THEN course.N ELSE 0 END) AS "Amer. Indian or Alaska Native",
    SUM(CASE WHEN course.RACE_ETH = 'ASIAN'        THEN course.N ELSE 0 END) AS "Asian",
    SUM(CASE WHEN course.RACE_ETH = 'BLK/AF_AMER'  THEN course.N ELSE 0 END) AS "Black or African Amer.",
    SUM(CASE WHEN course.RACE_ETH = 'HISP'         THEN course.N ELSE 0 END) AS "Hisp. or Latino",
    SUM(CASE WHEN course.RACE_ETH = 'NH/PI'        THEN course.N ELSE 0 END) AS "Native Hawaiian or Pacific Islander",
    SUM(CASE WHEN course.RACE_ETH = 'MULTI-RACIAL' THEN course.N ELSE 0 END) AS "Two or more races",
    SUM(CASE WHEN course.RACE_ETH = 'WHITE'        THEN course.N ELSE 0 END) AS "White",

    SUM(CASE WHEN course.STUDENT_GENDER_CD = 'F'   THEN course.N ELSE 0 END) AS "Girls",
    SUM(CASE WHEN course.STUDENT_GENDER_CD = 'M'   THEN course.N ELSE 0 END) AS "Boys",

    SUM(CASE WHEN course.PLAN_504 = 'Yes' THEN course.N ELSE 0 END) AS "S504",

    SUM(CASE WHEN course.SPL_ED_STATUS = 'Y' OR course.PRIMARY_DISABIITY IS NOT NULL
             THEN course.N ELSE 0 END) AS "Disability",

    SUM(CASE WHEN course.POVERTY_CODE = 'Y' OR course.HOMELESS_STATUS = 'Y'
             THEN course.N ELSE 0 END) AS "Eco. Dis.",
    SUM(CASE WHEN course.EL_STATUS = 'Y' THEN course.N ELSE 0 END) AS "EL"

FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course
JOIN "{{ school_year_splat }}_Student_School_Demographics" AS demo
  ON course.LOCATION_ID = demo.LOCATION_ID

{% if high_school_only %}
WHERE course.CURR_GRADE_LVL IN ('009','010','011','012','09','10','11','12')
{% endif %}

GROUP BY
    course.LOCATION_ID,
    demo.LOCATION_NAME,
    demo.DISTRICT_NAME,
    demo."School Number (NCES)",
    demo."District Number (NCES)",
    demo."Lowest Grade Level Served",
    demo."Highest Grade Level Served"

ORDER BY
    "District Name", "School Name";
