-- School Offerings: Course enrollments by school and course
SELECT
    demo.LOCATION_NAME AS "School Name",
    demo."School Number (NCES)" AS "NCES ID (12-digit)",
    course.COURSE_CODE_ALT AS "Course ID",
    course.PDECOURSENAME AS "Course Name",
    COUNT(*) AS "Course Enrollment"
FROM "{{ school_year_splat }}_Student_Teacher_Course" AS course
JOIN "{{ school_year_splat }}_Student_School_Demographics" AS demo
    ON course.LOCATION_ID = demo.LOCATION_ID
GROUP BY
    demo.LOCATION_NAME,
    demo."School Number (NCES)",
    course.COURSE_CODE_ALT,
    course.PDECOURSENAME
ORDER BY
    demo.LOCATION_NAME,
    course.COURSE_CODE_ALT;
