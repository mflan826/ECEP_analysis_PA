SELECT  
    "23-24" AS "School Year",
    LOCATION_ID, 
    LOCATION_NAME AS "School Name", 
    DISTRICT_NAME AS "District Name",
    ENROLL,
    CAST(ROUND(ENROLL * "AI/AN" / 100) AS INT) AS "Amer. Indian or Alaska Native", 
    CAST(ROUND(ENROLL * "ASIAN" / 100) AS INT) AS "Asian",
    CAST(ROUND(ENROLL * "BLK/AF_AMER" / 100) AS INT) AS "Black or African Amer.",
    CAST(ROUND(ENROLL * "HISP" / 100) AS INT) AS "Hisp. or Latino",
    CAST(ROUND(ENROLL * "NH/PI" / 100) AS INT) AS "Native Hawaiian or Pacific Islander",
    CAST(ROUND(ENROLL * "MULTI-RACIAL" / 100) AS INT) AS "Two or more races",
    CAST(ROUND(ENROLL * "WHITE" / 100) AS INT) AS "White",
    CAST(ROUND(ENROLL * "F" / 100) AS INT) AS "Girls",
    CAST(ROUND(ENROLL * "M" / 100) AS INT) AS "Boys",
    CAST(ROUND(ENROLL * "PLAN_504" / 100) AS INT) AS "S504",
    CAST(ROUND(ENROLL * ("SPL_ED" + "AUT" + "DEAF-BLIND" + "DEV DELAY" + "EMPTL DIST" + "GIFT-DIS" + "HI" + "INF-TOD" + "INTELL DIS" + "MULTI" + "ORTHO" + "SPEC LRN DIS" + "SPCH LANG" + "TBI" + "VI" + "OTHER") / 100) AS INT) AS "Disability",
    CAST(ROUND(ENROLL * "ED" / 100) AS INT) AS "Eco. Dis.",
    CAST(ROUND(ENROLL * "EL" / 100) AS INT) AS "EL"
FROM 
    "23_24_Student_School_Demographics";
