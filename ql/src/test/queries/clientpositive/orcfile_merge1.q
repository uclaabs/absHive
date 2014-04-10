set hive.merge.orc.block.level=false;
set hive.exec.dynamic.partition=true;
set hive.mapper.cannot.span.multiple.partitions=true;

-- Tests merge when writing into partitioned table

DROP TABLE IF EXISTS orcfile_merge1;
DROP TABLE IF EXISTS orcfile_merge1a;
DROP TABLE IF EXISTS orcfile_merge1b;

CREATE TABLE orcfile_merge1 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1a (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1b (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

-- Write into multiple partitions to generate multiple input splits
INSERT OVERWRITE TABLE orcfile_merge1 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 9) as part
    FROM src;

-- Use non block-level merge
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1a PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 3) as part
        FROM orcfile_merge1 where ds='1';
INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 3) as part
    FROM orcfile_merge1 where ds='1';

set hive.merge.orc.block.level=true;
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 3) as part
        FROM orcfile_merge1 where ds='1';
INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 3) as part
    FROM orcfile_merge1 where ds='1';

-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1a WHERE ds='1'
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1b WHERE ds='1'
) t;

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1a;
DROP TABLE orcfile_merge1b;
