SELECT  
   '{{ school_year_dash }}' AS "School Year",
   course.LOCATION_ID,
   demographics.LOCATION_NAME AS "School Name", 
   demographics.DISTRICT_NAME AS "District Name",
   course.COURSE_CODE_ALT AS "Course ID",
   course.PDECOURSENAME AS "Course Name",   
   demographics."School Number (NCES)" AS "School Number (NCES)",
   demographics."District Number (NCES)" AS "District Number (NCES)",
   courselist.Category AS "Category",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'M' AND courselist.Level = 'Basic'
            THEN 1 
        END) AS "Basic_Boy",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'F' AND courselist.Level = 'Basic'
            THEN 1 
        END) AS "Basic_Girl",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'M' AND courselist.Level = 'Advanced'
            THEN 1 
        END) AS "Adv_Boy",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'F' AND courselist.Level = 'Advanced'
            THEN 1 
        END) AS "Adv_Girl",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD IN ('M', 'F') AND courselist.Level = 'Basic'
            THEN 1 
        END) AS "Basic_Total",
        
   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD IN ('M', 'F') AND courselist.Level = 'Advanced'
            THEN 1 
        END) AS "Adv_Total",
        
   COUNT(DISTINCT CASE 
                     WHEN courselist.Level = 'Basic'
                     THEN course.COURSE_CODE_ALT 
                 END) AS "Basic_Courses",

   COUNT(DISTINCT CASE 
                     WHEN courselist.Level = 'Advanced'
                     THEN course.COURSE_CODE_ALT 
                 END) AS "Adv_Courses"

FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course

INNER JOIN "{{ school_year_splat }}_Student_School_Demographics" AS demographics 
    ON course.LOCATION_ID = demographics.LOCATION_ID

LEFT JOIN "Courses" AS courselist 
    ON courselist."Course Code" = course.COURSE_CODE_ALT

{% if high_school_only %}
WHERE course.CURR_GRADE_LVL IN ('009', '010', '011', '012') 
{% endif %}

GROUP BY 
   course.LOCATION_ID,
   course.COURSE_CODE_ALT,
   course.PDECOURSENAME,
   demographics.LOCATION_NAME,
   demographics.DISTRICT_NAME,
   demographics."School Number (NCES)",
   demographics."District Number (NCES)",
   courselist.Category
