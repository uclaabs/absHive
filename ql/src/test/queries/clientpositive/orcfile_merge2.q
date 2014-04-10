set hive.merge.orc.block.level=false;
set hive.exec.dynamic.partition=true;
set hive.mapper.cannot.span.multiple.partitions=true;

-- Tests merge when writing into multiple levels of partitions

DROP TABLE IF EXISTS orcfile_merge2;
DROP TABLE IF EXISTS orcfile_merge2a;
DROP TABLE IF EXISTS orcfile_merge2b;

CREATE TABLE orcfile_merge2 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge2a (key INT, value STRING) 
    PARTITIONED BY (ds STRING, one STRING, two STRING) STORED AS ORC;
CREATE TABLE orcfile_merge2b (key INT, value STRING) 
    PARTITIONED BY (ds STRING, one STRING, two STRING) STORED AS ORC;

INSERT OVERWRITE TABLE orcfile_merge2 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 9) as part
    FROM src;

-- Use non block-level merge
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge2a PARTITION (ds='1', one, two)
        SELECT key, value, PMOD(HASH(key), 3) as one,
        PMOD(HASH(value), 3) as two
        FROM orcfile_merge2 where ds='1';
INSERT OVERWRITE TABLE orcfile_merge2b PARTITION (ds='1', one, two)
    SELECT key, value, PMOD(HASH(key), 3) as one,
    PMOD(HASH(value), 3) as two
    FROM orcfile_merge2 where ds='1';

set hive.merge.orc.block.level=true;
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge2b PARTITION (ds='1', one, two)
        SELECT key, value, PMOD(HASH(key), 3) as one,
        PMOD(HASH(value), 3) as two
        FROM orcfile_merge2 where ds='1';
INSERT OVERWRITE TABLE orcfile_merge2b PARTITION (ds='1', one, two)
    SELECT key, value, PMOD(HASH(key), 3) as one,
    PMOD(HASH(value), 3) as two
    FROM orcfile_merge2 where ds='1';

-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge2a WHERE ds='1'
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge2b WHERE ds='1'
) t;

DROP TABLE orcfile_merge2;
DROP TABLE orcfile_merge2a;
DROP TABLE orcfile_merge2b;
