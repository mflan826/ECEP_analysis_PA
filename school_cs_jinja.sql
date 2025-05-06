SELECT  
   '{{ school_year_dash }}' AS "School Year",
   course.LOCATION_ID,
   demographics.LOCATION_NAME AS "School Name", 
   demographics.DISTRICT_NAME AS "District Name",
   course.COURSE_CODE_ALT AS "Course ID",
   course.PDECOURSENAME AS "Course Name",   
   demographics."School Number (NCES)" AS "School Number (NCES)",
   demographics."District Number (NCES)" AS "District Number (NCES)",
    
   (SELECT courselist.Category 
    FROM "Courses" courselist 
    WHERE courselist."Course Code" = course.COURSE_CODE_ALT
    LIMIT 1) AS "Category",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'M' AND
                 (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Basic'
            THEN 1 
        END) AS "Basic_Boy",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'F' AND
                 (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Basic'
            THEN 1 
        END) AS "Basic_Girl",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'M' AND
                 (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Advanced'
            THEN 1 
        END) AS "Adv_Boy",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD = 'F' AND
                 (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Advanced'
            THEN 1 
        END) AS "Adv_Girl",

   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD IN ('M', 'F') AND
                 (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Basic'
            THEN 1 
        END) AS "Basic_Total",
        
   COUNT(CASE 
            WHEN course.STUDENT_GENDER_CD IN ('M', 'F') AND
                 (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Advanced'
            THEN 1 
        END) AS "Adv_Total",
        
   COUNT(DISTINCT CASE 
                     WHEN (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Basic'
                     THEN course.COURSE_CODE_ALT 
                 END) AS "Basic_Courses",

   COUNT(DISTINCT CASE 
                     WHEN (SELECT Level FROM "Courses" c WHERE c."Course Code" = course.COURSE_CODE_ALT LIMIT 1) = 'Advanced'
                     THEN course.COURSE_CODE_ALT 
                 END) AS "Adv_Courses"

FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course
INNER JOIN "{{ school_year_splat }}_Student_School_Demographics" AS demographics 
    ON course.LOCATION_ID = demographics.LOCATION_ID

{% if high_school_only %}
WHERE course.CURR_GRADE_LVL IN ('009', '010', '011', '012') 
{% endif %}

GROUP BY 
   course.LOCATION_ID;
