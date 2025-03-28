-- School List: Basic school info with grade band
SELECT
    demo.LOCATION_NAME AS "School Name",
    demo."School Number (NCES)" AS "NCES ID (full 12-digit)",
    
    -- Convert low and high grade levels to formatted grade band
    CASE
        WHEN demo."Lowest Grade Level Served" = 'Prekindergarten' THEN 'PreK'
        WHEN demo."Lowest Grade Level Served" = 'Kindergarten' THEN 'K'
        WHEN demo."Lowest Grade Level Served" = '1st Grade' THEN '1'
        WHEN demo."Lowest Grade Level Served" = '2nd Grade' THEN '2'
        WHEN demo."Lowest Grade Level Served" = '3rd Grade' THEN '3'
        WHEN demo."Lowest Grade Level Served" LIKE '%th Grade' THEN 
            REPLACE(demo."Lowest Grade Level Served", 'th Grade', '')
        ELSE demo."Lowest Grade Level Served"
    END || '-' ||
    CASE
        WHEN demo."Highest Grade Level Served" = 'Prekindergarten' THEN 'PreK'
        WHEN demo."Highest Grade Level Served" = 'Kindergarten' THEN 'K'
        WHEN demo."Highest Grade Level Served" = '1st Grade' THEN '1'
        WHEN demo."Highest Grade Level Served" = '2nd Grade' THEN '2'
        WHEN demo."Highest Grade Level Served" = '3rd Grade' THEN '3'
        WHEN demo."Highest Grade Level Served" LIKE '%th Grade' THEN 
            REPLACE(demo."Highest Grade Level Served", 'th Grade', '')
        ELSE demo."Highest Grade Level Served"
    END AS "Grade Band",
    
    '' AS "Did this school report CS data to the state?"  -- leave blank for now

FROM "{{ school_year_splat }}_Student_School_Demographics" AS demo
ORDER BY demo.LOCATION_NAME;
