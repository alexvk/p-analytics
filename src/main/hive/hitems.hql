CREATE TABLE hitems (
    serial_num string,
    status string,
    attr map <string, string>
)
COMMENT 'A sample text based items table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY '#'
STORED AS TEXTFILE;