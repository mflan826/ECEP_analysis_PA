-- Statewide Demographics Aggregated to Reporting Categories

WITH courses_labeled AS (
  SELECT
    "Course Code",
    CASE
      WHEN Category = 'Data Science' THEN 'data science courses'
      WHEN Type = 'CS' AND Level = 'Basic' THEN 'foundational CS courses'
      WHEN Type = 'CS' AND Level = 'Advanced' THEN 'advanced CS courses'
      ELSE NULL
    END AS course_group
  FROM Courses
  WHERE Type = 'CS' OR Category = 'Data Science'
),

banded AS (
  SELECT
    {{ school_year_dash }} AS year,
    CASE
      WHEN course.CURR_GRADE_LVL IN ('K4A', 'K4F', 'K4P', 'K5A', 'K5F', 'K5P', 'PKF', '001', '002', '003', '004', '005') THEN 'k-5'
      WHEN course.CURR_GRADE_LVL IN ('006', '007', '008') THEN '6-8'
      WHEN course.CURR_GRADE_LVL = '009' THEN '9'
      WHEN course.CURR_GRADE_LVL = '010' THEN '10'
      WHEN course.CURR_GRADE_LVL = '011' THEN '11'
      WHEN course.CURR_GRADE_LVL = '012' THEN '12'
      ELSE NULL
    END AS grade_band,
    crs.course_group,
    course.STUDENT_GENDER_CD,
    course.RACE_ETH,
    course.PLAN_504,
    course.SPL_ED_STATUS,
    course.PRIMARY_DISABIITY,
    course.POVERTY_CODE,
    course.HOMELESS_STATUS,
    course.EL_STATUS
  FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course
  JOIN courses_labeled crs ON course.COURSE_CODE_ALT = crs."Course Code"
  WHERE course.CURR_GRADE_LVL IS NOT NULL
)

-- Final Aggregation
SELECT
  CASE
    WHEN course_group = 'foundational CS courses' THEN 'foundational CS courses'
    WHEN course_group IN ('foundational CS courses', 'advanced CS courses') THEN 'all CS courses (foundational and non-foundational)'
    WHEN course_group = 'data science courses' THEN 'data science courses'
  END AS "CS course type",
  CASE
    WHEN grade_band IN ('k-5', '6-8') THEN grade_band
    WHEN course_group = 'data science courses' AND grade_band IN ('9','10','11','12') THEN '9-12'
    ELSE grade_band
  END AS Grade,
  COUNT(*) AS "Total Number of Students Enrolled",
  COUNT(CASE WHEN STUDENT_GENDER_CD = 'M' THEN 1 END) AS "Number of Students: Male",
  COUNT(CASE WHEN STUDENT_GENDER_CD = 'F' THEN 1 END) AS "Number of Students: Female",
  COUNT(CASE WHEN STUDENT_GENDER_CD NOT IN ('M', 'F') THEN 1 END) AS "Number of Students: Gender neither male nor female",
  COUNT(CASE WHEN RACE_ETH = 'WHITE' THEN 1 END) AS "Number of Students: White",
  COUNT(CASE WHEN RACE_ETH = 'ASIAN' THEN 1 END) AS "Number of Students: Asian",
  COUNT(CASE WHEN RACE_ETH = 'BLK/AF_AMER' THEN 1 END) AS "Number of Students: Black/African American",
  COUNT(CASE WHEN RACE_ETH = 'HISP' THEN 1 END) AS "Number of Students: Hispanic/Latino/ Latina",
  COUNT(CASE WHEN RACE_ETH = 'AI/AN' THEN 1 END) AS "Number of Students: Native American/Alaska Native",
  COUNT(CASE WHEN RACE_ETH = 'NH/PI' THEN 1 END) AS "Number of Students: Native Hawaiian/Other Pacific Islander",
  COUNT(CASE WHEN RACE_ETH = 'MULTI_RACIAL' THEN 1 END) AS "Number of Students: Two or More Races",
  COUNT(CASE WHEN RACE_ETH IS NULL OR RACE_ETH = '' THEN 1 END) AS "Number of Students: Race/Ethnicity not reported",
  COUNT(CASE WHEN POVERTY_CODE = 'Y' OR HOMELESS_STATUS = 'Y' THEN 1 END) AS "Number of Students: Qualify for Free and Reduced Lunch",
  COUNT(CASE WHEN SPL_ED_STATUS = 'Y' OR PRIMARY_DISABIITY IS NOT NULL THEN 1 END) AS "Number of Students with IEPs under IDEA",
  COUNT(CASE WHEN PLAN_504 = 'Yes' THEN 1 END) AS "Number of Students: Qualify for services under section 504 of the Rehabilitation Act",
  COUNT(CASE WHEN EL_STATUS = 'Y' THEN 1 END) AS "Number of Students: English language learner"
FROM banded
WHERE
  (
    (course_group = 'foundational CS courses' AND grade_band IN ('k-5', '6-8', '9', '10', '11', '12'))
    OR
    (course_group IN ('foundational CS courses', 'advanced CS courses') AND grade_band IN ('k-5', '6-8', '9', '10', '11', '12'))
    OR
    (course_group = 'data science courses' AND grade_band IN ('9','10','11','12'))
  )
GROUP BY
  "CS course type",
  Grade
ORDER BY
  CASE "CS course type"
    WHEN 'foundational CS courses' THEN 1
    WHEN 'all CS courses (foundational and non-foundational)' THEN 2
    WHEN 'data science courses' THEN 3
  END,
  CASE Grade
    WHEN 'k-5' THEN 1
    WHEN '6-8' THEN 2
    WHEN '9' THEN 3
    WHEN '10' THEN 4
    WHEN '11' THEN 5
    WHEN '12' THEN 6
    WHEN '9-12' THEN 7
  END;
