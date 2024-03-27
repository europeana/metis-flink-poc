CREATE TABLE execution_record
(
    DATASET_ID     varchar,
    EXECUTION_ID   varchar,
    EXECUTION_NAME varchar,
    RECORD_ID      varchar,
    RECORD_DATA    text,
    PRIMARY KEY ((DATASET_ID, EXECUTION_ID), RECORD_ID)
);


CREATE TABLE execution_record_exception_log
(
    DATASET_ID     varchar,
    EXECUTION_ID   varchar,
    EXECUTION_NAME varchar,
    RECORD_ID      varchar,
    EXCEPTION      text,
    PRIMARY KEY ((DATASET_ID, EXECUTION_ID), RECORD_ID)
);
