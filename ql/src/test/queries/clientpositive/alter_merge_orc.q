create table src_orc_merge_test(key int, value string) stored as orc;

load data local inpath '../data/files/smbbucket_1.orc' into table src_orc_merge_test;
load data local inpath '../data/files/smbbucket_2.orc' into table src_orc_merge_test;
load data local inpath '../data/files/smbbucket_3.orc' into table src_orc_merge_test;

show table extended like `src_orc_merge_test`;

select count(1) from src_orc_merge_test;
select sum(hash(key)), sum(hash(value)) from src_orc_merge_test;

alter table src_orc_merge_test concatenate;

show table extended like `src_orc_merge_test`;

select count(1) from src_orc_merge_test;
select sum(hash(key)), sum(hash(value)) from src_orc_merge_test;


create table src_orc_merge_test_part(key int, value string) partitioned by (ds string) stored as orc;

alter table src_orc_merge_test_part add partition (ds='2011');

load data local inpath '../data/files/smbbucket_1.orc' into table src_orc_merge_test_part partition (ds='2011');
load data local inpath '../data/files/smbbucket_2.orc' into table src_orc_merge_test_part partition (ds='2011');
load data local inpath '../data/files/smbbucket_3.orc' into table src_orc_merge_test_part partition (ds='2011');

show table extended like `src_orc_merge_test_part` partition (ds='2011');

select count(1) from src_orc_merge_test_part;
select sum(hash(key)), sum(hash(value)) from src_orc_merge_test_part;

alter table src_orc_merge_test_part partition (ds='2011') concatenate;

show table extended like `src_orc_merge_test_part` partition (ds='2011');

select count(1) from src_orc_merge_test_part;
select sum(hash(key)), sum(hash(value)) from src_orc_merge_test_part;

drop table src_orc_merge_test;
drop table src_orc_merge_test_part;
