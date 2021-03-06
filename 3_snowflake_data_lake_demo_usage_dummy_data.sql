-- 
-- default database should be STG, STAGE or STAGING
--
USE DATABASE STG;

/********************************************************************
 ** Populate dummy demo data manually
 ********************************************************************/--
-- execute context
--
USE SCHEMA _METADATA;

/*
USE INT;
CREATE SCHEMA IF NOT EXISTS REFERENCE;
MERGE INTO REFERENCE.TILE_SCORE D
USING (
    --CREATE OR REPLACE TABLE REFERENCE.TILE_SCORE AS
    SELECT $1::VARCHAR TITLE,
        $2::FLOAT SCORE
    FROM VALUES 
      ('C-level', 20),
      ('VP', 15),
      ('Director', 10),
      ('Manager', 5)
) S
ON D.TITLE = S.TITLE
WHEN NOT MATCHED THEN INSERT(TITLE, SCORE) VALUES(S.TITLE, S.SCORE)
WHEN MATCHED THEN UPDATE SET SCORE = S.SCORE;

MERGE INTO REFERENCE.PLATFORM D
USING (
    --CREATE OR REPLACE TABLE REFERENCE.PLATFORM AS
    SELECT $1::NUMBER PLATFORM_ID,
        $2::VARCHAR PLATFORM_NAME ,
        $3::VARCHAR PLATFORM_TYPE
    FROM VALUES 
      (1, 'CILENT1_EMAIL_SALES', 'SALES'),
      (2, 'CILENT2_EMAIL_TECHS', 'TECHS')
) S
ON D.PLATFORM_NAME = S.PLATFORM_NAME
AND D.PLATFORM_TYPE = S.PLATFORM_TYPE
WHEN NOT MATCHED THEN 
    INSERT(PLATFORM_ID, PLATFORM_NAME, PLATFORM_TYPE) 
    VALUES(S.PLATFORM_ID, S.PLATFORM_NAME, S.PLATFORM_TYPE)
WHEN MATCHED THEN 
    UPDATE SET 
        PLATFORM_ID = S.PLATFORM_ID,
        PLATFORM_NAME = S.PLATFORM_NAME,
        PLATFORM_TYPE = S.PLATFORM_TYPE;

MERGE INTO REFERENCE.ORGANIZATION D
USING (
    --CREATE OR REPLACE TABLE REFERENCE.ORGANIZATION AS
    SELECT $1::NUMBER ORGANIZATION_ID,
        $2::VARCHAR ORGANIZATION_NAME,
        $3::VARCHAR SHORT_NAME
    FROM VALUES 
      (1, 'CILENT1 INC', 'CILENT1'),
      (2, 'CILENT2 ORG', 'CILENT2')
) S
ON D.ORGANIZATION_NAME = S.ORGANIZATION_NAME
WHEN NOT MATCHED THEN 
    INSERT(ORGANIZATION_ID, ORGANIZATION_NAME, SHORT_NAME) 
    VALUES(S.ORGANIZATION_ID, S.ORGANIZATION_NAME, S.SHORT_NAME)
WHEN MATCHED THEN 
    UPDATE SET 
        ORGANIZATION_ID = S.ORGANIZATION_ID,
        ORGANIZATION_NAME = S.ORGANIZATION_NAME,
        SHORT_NAME = S.SHORT_NAME;
*/

USE SCHEMA _METADATA;

INSERT INTO STG.CILENT1_EMAIL_SALES.RAW_PERSON
SELECT FIRST_NAME, 
    LAST_NAME, 
    ARRAY_CONSTRUCT('C-level','VP','Director','Manager','Employee')[UNIFORM(0, 4, RANDOM(11))::NUMBER] TITLE, 
    'CILENT1' ORGANIZATION, 
    ARRAY_CONSTRUCT('Sales','Product','Engineering','HR','IT')[UNIFORM(0, 4, RANDOM(11))::NUMBER] DEPARTMENT,
    ARRAY_AGG(EMAIL_ADDRESS) EMAIL_ADDRESS,
    NULL PHOTO_URL,
    '' DB_ACTION,
    MIN(FILE_NAME) FILE_NAME,
    MIN(FILE_ROW_NUMBER) FILE_ROW_NUMBER,
    DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ LOAD_TIME
FROM (
    SELECT 
        UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) FIRST_NAME,
        UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) LAST_NAME,
        LOWER(CONCAT(FIRST_NAME, ' ', LAST_NAME, ' <', FIRST_NAME, '.', LAST_NAME, '@CILENT1.COM>')) EMAIL_ADDRESS,
        UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) FILE_NAME,
        UNIFORM(0, 15, RANDOM(11))::NUMBER FILE_ROW_NUMBER
    FROM TABLE(GENERATOR(ROWCOUNT => 50)) V
    )
GROUP BY 1,2
ORDER BY 1,2
;

INSERT INTO STG.CILENT2_EMAIL_TECHS.RAW_PERSON
SELECT FIRST_NAME, 
    LAST_NAME, 
    ARRAY_CONSTRUCT('C-level','VP','Director','Manager','Employee')[UNIFORM(0, 4, RANDOM(11))::NUMBER] TITLE, 
    'CILENT2' ORGANIZATION, 
    ARRAY_CONSTRUCT('Sales','Product','Engineering','HR','IT')[UNIFORM(0, 4, RANDOM(11))::NUMBER] DEPARTMENT,
    ARRAY_AGG(EMAIL_ADDRESS) EMAIL_ADDRESS,
    NULL PHOTO_URL,
    '' DB_ACTION,
    MIN(FILE_NAME) FILE_NAME,
    MIN(FILE_ROW_NUMBER) FILE_ROW_NUMBER,
    DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ LOAD_TIME
FROM (
    SELECT 
        UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) FIRST_NAME,
        UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) LAST_NAME,
        LOWER(CONCAT(FIRST_NAME, ' ', LAST_NAME, ' <', FIRST_NAME, '.', LAST_NAME, '@CILENT2.COM>')) EMAIL_ADDRESS,
        UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) FILE_NAME,
        UNIFORM(0, 15, RANDOM(11))::NUMBER FILE_ROW_NUMBER
    FROM TABLE(GENERATOR(ROWCOUNT => 50)) V
    )
GROUP BY 1,2
ORDER BY 1,2
;

INSERT INTO STG.CILENT1_EMAIL_SALES.RAW_MESSAGE
SELECT M.SUBJECT, 
    ARRAY_AGG(S.EMAIL_ADDRESS[0])[0] SENDER, 
    ARRAY_SLICE(
          ARRAY_AGG(DISTINCT A.EMAIL_ADDRESS[0]),
          UNIFORM(1, 3, RANDOM(11))::NUMBER, 
          UNIFORM(3, 10, RANDOM(11))::NUMBER
        ) RECIPIENTS,
    M.BODY,
    NULL REFERENCES,
    DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ SENT_AT,
    NULL PROBABLY_REPLY,
    NULL AUTO_RESPONSE_TYPE,
    NULL IN_REPLY_TO,
    NULL ORIGINAL_ID,
    NULL THREAD_ID,
    '' DB_ACTION,
    M.FILE_NAME,
    M.FILE_ROW_NUMBER,
    DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ LOAD_TIME
FROM (
    SELECT SUBJECT, 
        --SENDER, 
        --ARRAY_AGG(EMAIL_ADDRESS) RECIPIENTS,
        BODY,
        '' DB_ACTION,
        MIN(FILE_NAME) FILE_NAME,
        MIN(FILE_ROW_NUMBER) FILE_ROW_NUMBER,
        DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ LOAD_TIME
    FROM (
        SELECT 
            UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) SUBJECT,
            LOWER(RANDSTR(UNIFORM(10, 30, RANDOM()), RANDOM())::VARCHAR) BODY,
            UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) FILE_NAME,
            UNIFORM(0, 15, RANDOM(11))::NUMBER FILE_ROW_NUMBER
        FROM TABLE(GENERATOR(ROWCOUNT => 100)) V
        )
    GROUP BY 1,2
) M
JOIN STG.CILENT1_EMAIL_SALES.RAW_PERSON S
ON M.FILE_ROW_NUMBER = S.FILE_ROW_NUMBER
LEFT JOIN STG.CILENT1_EMAIL_SALES.RAW_PERSON A
ON NOT ARRAY_CONTAINS(S.EMAIL_ADDRESS, A.EMAIL_ADDRESS)
GROUP BY 1,4,13,14
;

INSERT INTO STG.CILENT2_EMAIL_TECHS.RAW_MESSAGE
SELECT M.SUBJECT, 
    ARRAY_AGG(S.EMAIL_ADDRESS[0])[0] SENDER, 
    ARRAY_SLICE(
          ARRAY_AGG(DISTINCT A.EMAIL_ADDRESS[0]),
          UNIFORM(1, 3, RANDOM(11))::NUMBER, 
          UNIFORM(3, 10, RANDOM(11))::NUMBER
        ) RECIPIENTS,
    M.BODY,
    NULL REFERENCES,
    DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ SENT_AT,
    NULL PROBABLY_REPLY,
    NULL AUTO_RESPONSE_TYPE,
    NULL IN_REPLY_TO,
    NULL ORIGINAL_ID,
    NULL THREAD_ID,
    '' DB_ACTION,
    M.FILE_NAME,
    M.FILE_ROW_NUMBER,
    DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ LOAD_TIME
FROM (
    SELECT SUBJECT, 
        --SENDER, 
        --ARRAY_AGG(EMAIL_ADDRESS) RECIPIENTS,
        BODY,
        '' DB_ACTION,
        MIN(FILE_NAME) FILE_NAME,
        MIN(FILE_ROW_NUMBER) FILE_ROW_NUMBER,
        DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ LOAD_TIME
    FROM (
        SELECT 
            UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) SUBJECT,
            LOWER(RANDSTR(UNIFORM(10, 30, RANDOM()), RANDOM())::VARCHAR) BODY,
            UPPER(RANDSTR(UNIFORM(3, 3, RANDOM()), RANDOM())::VARCHAR) FILE_NAME,
            UNIFORM(0, 15, RANDOM(11))::NUMBER FILE_ROW_NUMBER
        FROM TABLE(GENERATOR(ROWCOUNT => 100)) V
        )
    GROUP BY 1,2
) M
JOIN STG.CILENT2_EMAIL_TECHS.RAW_PERSON S
ON M.FILE_ROW_NUMBER = S.FILE_ROW_NUMBER
LEFT JOIN STG.CILENT2_EMAIL_TECHS.RAW_PERSON A
ON NOT ARRAY_CONTAINS(S.EMAIL_ADDRESS, A.EMAIL_ADDRESS)
GROUP BY 1,4,13,14
;

/*
--
-- Check how the stage data are flowing into raw tables
--
USE ROLE TRANSFORMER;
USE DATABASE STG;
USE SCHEMA _METADATA;
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');

-- Check the raw data
SELECT * FROM CILENT1_EMAIL_SALES.RAW_PERSON;
SELECT * FROM CILENT1_EMAIL_SALES.RAW_MESSAGE;
SELECT * FROM CILENT1_EMAIL_SALES.RAW_DATA3;

SELECT * FROM CILENT2_EMAIL_TECHS.RAW_PERSON;
SELECT * FROM STG.CILENT2_EMAIL_TECHS.RAW_MESSAGE;

-- Check the loaded stage data
SELECT * FROM CILENT1_EMAIL_SALES.PERSON;
SELECT * FROM CILENT1_EMAIL_SALES.MESSAGE;
SELECT * FROM CILENT1_EMAIL_SALES.DATA3;

SELECT * FROM CILENT2_EMAIL_TECHS.PERSON;
SELECT * FROM CILENT2_EMAIL_TECHS.MESSAGE;


--
-- Check how the raw data are merged in
--
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','WORK');

-- Check the digested data
SELECT * FROM CILENT1_EMAIL_SALES.DIGEST_PERSON;
SELECT * FROM CILENT1_EMAIL_SALES.DIGEST_MESSAGE;
SELECT * FROM STG.CILENT1_EMAIL_SALES.DIGEST_DATA3;

SELECT * FROM CILENT2_EMAIL_TECHS.DIGEST_PERSON;
SELECT * FROM STG.CILENT2_EMAIL_TECHS.DIGEST_MESSAGE;

-- Check the cross reference data
SELECT * FROM CILENT1_EMAIL_SALES.XREF_PERSON;
SELECT * FROM CILENT1_EMAIL_SALES.XREF_MESSAGE;
SELECT * FROM CILENT1_EMAIL_SALES.XREF_DATA3;

SELECT * FROM CILENT2_EMAIL_TECHS.XREF_PERSON;
SELECT * FROM CILENT2_EMAIL_TECHS.XREF_MESSAGE;


--
-- Check how the data load into INT database
--
USE ROLE TRANSFORMER;
USE DATABASE INT;
USE SCHEMA _METADATA;
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');

-- Check the digested data
SELECT * FROM CILENT1_EMAIL_SALES.DIGEST_PERSON;
SELECT * FROM CILENT1_EMAIL_SALES.DIGEST_MESSAGE;
SELECT * FROM CILENT1_EMAIL_SALES.DIGEST_PERSON_EMAIL;
SELECT * FROM CILENT1_EMAIL_SALES.DIGEST_MESSAGE_EMAIL;

SELECT * FROM CILENT2_EMAIL_TECHS.DIGEST_PERSON;
SELECT * FROM CILENT2_EMAIL_TECHS.DIGEST_MESSAGE;
SELECT * FROM CILENT2_EMAIL_TECHS.DIGEST_PERSON_EMAIL;
SELECT * FROM CILENT2_EMAIL_TECHS.DIGEST_MESSAGE_EMAIL;

-- Check the cross reference data
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','WORK');

SELECT * FROM CILENT1_EMAIL_SALES.XREF_PERSON;
SELECT * FROM CILENT1_EMAIL_SALES.XREF_MESSAGE;
SELECT * FROM CILENT1_EMAIL_SALES.XREF_PERSON_EMAIL;
SELECT * FROM CILENT1_EMAIL_SALES.XREF_MESSAGE_EMAIL;

SELECT * FROM CILENT2_EMAIL_TECHS.XREF_PERSON;
SELECT * FROM CILENT2_EMAIL_TECHS.XREF_MESSAGE;
SELECT * FROM CILENT2_EMAIL_TECHS.XREF_PERSON_EMAIL;
SELECT * FROM CILENT2_EMAIL_TECHS.XREF_MESSAGE_EMAIL;
*/


/********************************************************************
 ** Process dummy demo data manually
 ********************************************************************/--
USE SCHEMA STG._METADATA;

--CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');
