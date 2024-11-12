USE DATABASE "TICKETMASTER";
USE SCHEMA "PUBLIC";
-- ######################## Have to highlight and run individually after establishing snowflake connection in VS Code^^^^
SELECT * from "Events" LIMIT 100;
select distinct "CLASSIFICATIONS_SEGMENT_NAME" from "Events";

SELECT * FROM "Events" WHERE "dates_start_localDate" BETWEEN '2024-01-01' AND '2025-01-01' AND "CLASSIFICATIONS_SEGMENT_NAME" = 'Music' LIMIT 1000;
