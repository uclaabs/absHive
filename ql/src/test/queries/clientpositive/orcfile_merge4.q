set hive.merge.orc.block.level=true;
set hive.mapper.cannot.span.multiple.partitions=true;

-- Test merge when reading from ORC file and writing to text file

DROP TABLE orcfile_merge3a;
DROP TABLE orcfile_merge3b;

CREATE TABLE orcfile_merge3a (key int, value string) 
    PARTITIONED BY (ds string) STORED AS ORC;
CREATE TABLE orcfile_merge3b (key int, value string) STORED AS TEXTFILE;

INSERT OVERWRITE TABLE orcfile_merge3a PARTITION (ds='1')
    SELECT * FROM src;
INSERT OVERWRITE TABLE orcfile_merge3a PARTITION (ds='2')
    SELECT * FROM src;

EXPLAIN INSERT OVERWRITE TABLE orcfile_merge3b
    SELECT key, value FROM orcfile_merge3a;
INSERT OVERWRITE TABLE orcfile_merge3b
    SELECT key, value FROM orcfile_merge3a;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM orcfile_merge3a
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM orcfile_merge3b
) t;

DROP TABLE orcfile_merge3a;
DROP TABLE orcfile_merge3b;
