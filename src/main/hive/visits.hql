CREATE TABLE visits (
    user_id string,
    session_id string,
    page string,
    time bigint
)
COMMENT 'A sample text file based visits table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY '#'
STORED AS TEXTFILE;