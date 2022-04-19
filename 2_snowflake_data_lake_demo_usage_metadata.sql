-- 
-- default database should be STG, STAGE or STAGING
--
USE DATABASE STG;

/********************************************************************
 ** Schema Configuration Section
 ********************************************************************/
--
-- execute context
--
USE SCHEMA _METADATA;

--GRANT ALL PRIVILEGES ON SCHEMA _METADATA TO ROLE LOADER;
--GRANT ALL PRIVILEGES ON SEQUENCE _METADATA.SG_KEY_GEN TO ROLE LOADER;

-- Clear any existing test data
TRUNCATE TABLE CTRL_IMPORT;
TRUNCATE TABLE CTRL_CURRENT;
TRUNCATE TABLE CTRL_LOG;

--
-- Create some test config data
--
MERGE INTO CTRL_IMPORT D
USING (
    SELECT $1 CLIENT_NAME,
        $2 PLATFORM_NAME,
        $3 PLATFORM_TYPE,
        'STG' DATA_CATALOG,
        CONCAT_WS('_', $1, $2, $3) DATA_SCHEMA,
        $4 DATA_NAME,
        NULL /*PARSE_JSON($5)*/ DATA_STAGE,
        NULL /*PARSE_JSON($6)*/ DATA_FORMAT,
        PARSE_JSON('{"DATA_KEY_FIELD": "DATA_KEY", "DATA_HASH_FIELD": "DATA_HASH","DATA_TIME_FIELD": "LOAD_TIME"}') CTRL_FIELD,
        PARSE_JSON($7) DATA_FIELD,
        PARSE_JSON($8) META_FIELD,
        MD5(TO_VARCHAR(ARRAY_CONSTRUCT(*))) CONFIG_HASH,
        TRUE AUTOMATED
    FROM VALUES 
      (
        'CILENT1',
        'EMAIL',
        'SALES',
        'PERSON',
        $$['URL = \'s3://path/to/client1/person/data/\'', 'STORAGE_INTEGRATION = <integration_name>']$$,
        $$['TYPE = <data_format_type>']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FIRST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "TITLE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "ORGANIZATION",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DEPARTMENT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PHOTO_URL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DB_ACTION",
                "FIELD_TRANS": "DB_ACTION",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_NAME",
                "FIELD_TRANS": "FILE_NAME",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_ROW_NUMBER",
                "FIELD_TRANS": "FILE_ROW_NUMBER",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "CURRENT_TIMESTAMP",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BATCH_ID",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{DATA_NAME}}, TO_VARCHAR(CURRENT_DATE))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{PLATFORM_TYPE}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$
      ),
      (
        'CILENT1',
        'EMAIL',
        'SALES',
        'MESSAGE',
        $$['URL = \'s3://path/to/client1/message/data/\'', 'STORAGE_INTEGRATION = <integration_name>']$$,
        $$['TYPE = <data_format_type>']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SUBJECT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RECIPIENTS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BODY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "REFERENCES",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENT_AT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PROBABLY_REPLY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "AUTO_RESPONSE_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "IN_REPLY_TO",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "ORIGINAL_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "THREAD_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DB_ACTION",
                "FIELD_TRANS": "DB_ACTION",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_NAME",
                "FIELD_TRANS": "FILE_NAME",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_ROW_NUMBER",
                "FIELD_TRANS": "FILE_ROW_NUMBER",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "CURRENT_TIMESTAMP",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BATCH_ID",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{DATA_NAME}}, TO_VARCHAR(CURRENT_DATE))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{PLATFORM_TYPE}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$
      ),
      (
        'CILENT1',
        'EMAIL',
        'SALES',
        'DATA3',
        $$['URL = \'s3://path/to/client1/data3/folder/\'', 'STORAGE_INTEGRATION = <integration_name>']$$,
        $$['TYPE = <data_format_type>']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_NAME",
                "FIELD_TRANS": "?",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DATA_SOURCE",
                "FIELD_TRANS": "?",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DATA_JSON",
                "FIELD_TRANS": "?",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DB_ACTION",
                "FIELD_TRANS": "DB_ACTION",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_NAME",
                "FIELD_TRANS": "FILE_NAME",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_ROW_NUMBER",
                "FIELD_TRANS": "FILE_ROW_NUMBER",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "CURRENT_TIMESTAMP",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DATA3_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "SCD_TYPE",
                "FIELD_TRANS": "1",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BATCH_ID",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{DATA_NAME}}, TO_VARCHAR(CURRENT_DATE))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{PLATFORM_TYPE}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$
      ),
      (
        'CILENT2',
        'EMAIL',
        'TECHS',
        'PERSON',
        $$['URL = \'s3://path/to/client2/person/data/\'', 'STORAGE_INTEGRATION = <integration_name>']$$,
        $$['TYPE = <data_format_type>']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FIRST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "TITLE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "ORGANIZATION",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DEPARTMENT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PHOTO_URL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DB_ACTION",
                "FIELD_TRANS": "DB_ACTION",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_NAME",
                "FIELD_TRANS": "FILE_NAME",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_ROW_NUMBER",
                "FIELD_TRANS": "FILE_ROW_NUMBER",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "CURRENT_TIMESTAMP",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BATCH_ID",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{DATA_NAME}}, TO_VARCHAR(CURRENT_DATE))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{PLATFORM_TYPE}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$
      ),
      (
        'CILENT2',
        'EMAIL',
        'TECHS',
        'MESSAGE',
        $$['URL = \'s3://path/to/client2/message/data/\'', 'STORAGE_INTEGRATION = <integration_name>']$$,
        $$['TYPE = <data_format_type>']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SUBJECT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RECIPIENTS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BODY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "REFERENCES",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENT_AT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PROBABLY_REPLY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "AUTO_RESPONSE_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "IN_REPLY_TO",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "ORIGINAL_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "THREAD_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DB_ACTION",
                "FIELD_TRANS": "DB_ACTION",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_NAME",
                "FIELD_TRANS": "FILE_NAME",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FILE_ROW_NUMBER",
                "FIELD_TRANS": "FILE_ROW_NUMBER",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "CURRENT_TIMESTAMP",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BATCH_ID",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{DATA_NAME}}, TO_VARCHAR(CURRENT_DATE))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM",
                "FIELD_TRANS": "CONCAT_WS(\'_\', {{CLIENT_NAME}}, {{PLATFORM_NAME}}, {{PLATFORM_TYPE}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$
      )
    ) S
ON D.CLIENT_NAME = S.CLIENT_NAME
    AND D.PLATFORM_NAME = S.PLATFORM_NAME
    AND D.PLATFORM_TYPE = S.PLATFORM_TYPE
    AND D.DATA_CATALOG = S.DATA_CATALOG
    AND D.DATA_SCHEMA = S.DATA_SCHEMA
    AND D.DATA_NAME = S.DATA_NAME
WHEN MATCHED AND D.CONFIG_HASH != S.CONFIG_HASH THEN
    UPDATE SET
        DATA_STAGE = S.DATA_STAGE,
        DATA_FORMAT = S.DATA_FORMAT,
        CTRL_FIELD = S.CTRL_FIELD,
        DATA_FIELD = S.DATA_FIELD,
        META_FIELD = S.META_FIELD,
        CONFIG_HASH = S.CONFIG_HASH
WHEN NOT MATCHED THEN 
    INSERT (
        CLIENT_NAME,
        PLATFORM_NAME,
        PLATFORM_TYPE,
        DATA_CATALOG,
        DATA_SCHEMA,
        DATA_NAME,
        DATA_STAGE,
        DATA_FORMAT,
        CTRL_FIELD,
        DATA_FIELD,
        META_FIELD,
        CONFIG_HASH,
        AUTOMATED
    )
    VALUES (
        S.CLIENT_NAME,
        S.PLATFORM_NAME,
        S.PLATFORM_TYPE,
        S.DATA_CATALOG,
        S.DATA_SCHEMA,
        S.DATA_NAME,
        S.DATA_STAGE,
        S.DATA_FORMAT,
        S.CTRL_FIELD,
        S.DATA_FIELD,
        S.META_FIELD,
        S.CONFIG_HASH,
        S.AUTOMATED
    );

/*
--
-- Generate stage database schema
--
USE SCHEMA _METADATA;
SELECT * FROM CTRL_SCHEMA_UPDATE ORDER BY 1,2,3,4,5,6;
CALL CTRL_SCHEMA_UPDATER('DEBUG');
CALL CTRL_SCHEMA_UPDATER('WORK');
--INSERT INTO CTRL_CURRENT SELECT * FROM CTRL_IMPORT;




--
-- Goto Script-5 to generate some dummy raw data and check them flowing through
--




--
-- Demo for data source changes.
--
USE SCHEMA _METADATA;

DELETE FROM CTRL_IMPORT 
WHERE CLIENT_NAME = 'CILENT1' 
AND PLATFORM_NAME = 'EMAIL'
--AND PLATFORM_TYPE = 'TYPE1'
AND DATA_NAME = 'MESSAGE';

SELECT * FROM CTRL_IMPORT;
SELECT * FROM CTRL_CURRENT;

SELECT * FROM CTRL_SCHEMA_UPDATE ORDER BY 1,2,3,4,5,6;
CALL CTRL_SCHEMA_UPDATER('DEBUG');
CALL CTRL_SCHEMA_UPDATER('WORK');

--
-- Modify stage database schema
--
SELECT * FROM CTRL_TASK_SCHEDULE;

USE SCHEMA _METADATA;
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','WORK');
*/


/********************************************************************
 ** Schema Update Manually
 ********************************************************************/
--
-- excution context
--
USE SCHEMA STG._METADATA;

--
-- Update data lake schema
--
CALL CTRL_SCHEMA_UPDATER('WORK');

--CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
--CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');
