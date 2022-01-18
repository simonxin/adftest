create database if not exists default; use default;
create database if not exists information_schema; use information_schema;
create database if not exists rawdb1; use rawdb1;
CREATE EXTERNAL TABLE `names3`(
  `employeeid` int, 
  `firstname` string, 
  `title` string, 
  `state` string, 
  `laptop` string)
COMMENT 'Employee Names'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/external/rawdb1.db/names3'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1584927275');
CREATE TABLE `names5`(
  `employeeid` int, 
  `firstname` string, 
  `title` string, 
  `state` string, 
  `laptop` string)
COMMENT 'Employee Names'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/managed/rawdb1.db/names5'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='insert_only', 
  'transient_lastDdlTime'='1584927284');
load data inpath 'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/external/rawdb1.db/ext_names5' into table names5;
CREATE TABLE `names6`(
  `employeeid` int, 
  `firstname` string, 
  `title` string, 
  `state` string, 
  `laptop` string)
COMMENT 'Employee Names'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/managed/rawdb1.db/names6'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1584927292');
load data inpath 'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/external/rawdb1.db/ext_names6' into table names6;
create database if not exists rawdb2; use rawdb2;
CREATE EXTERNAL TABLE `rw_vhcl_aqua_claimes_asra_details`(
  `country_code_vega` string, 
  `internal_vega_claim_number` string, 
  `compl_num` string, 
  `work_unit_position` string, 
  `asra_dg_cd` string, 
  `asra_op_cd` string, 
  `asra_categ_cd` string, 
  `asra_nm` string, 
  `wage_costs` decimal(20,2), 
  `work_unit_cnt` decimal(20,2), 
  `freq_num_of_wp` decimal(20,2), 
  `hash_value` int, 
  `last_mdf_dt` timestamp)
PARTITIONED BY ( 
  `partition_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'escape.delim'='\\', 
  'field.delim'='\001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/external/rawdb2.db/rw_vhcl_aqua_claimes_asra_details'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'last_modified_by'='anonymous', 
  'last_modified_time'='1565315562', 
  'transient_lastDdlTime'='1584949781','transactional'='false');
MSCK REPAIR TABLE rw_vhcl_aqua_claimes_asra_details;
CREATE TABLE `tf_cube_bm_result`(
  `d_month` string, 
  `region` string, 
  `category` string, 
  `kpi_name` string, 
  `kpi_total` double, 
  `t_date` date)
PARTITIONED BY ( 
  `partition_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/managed/rawdb2.db/tf_cube_bm_result'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='insert_only', 
  'transient_lastDdlTime'='1584927531');
create table flattened_tf_cube_bm_result ( d_month string, region string, category string, kpi_name string, kpi_total double, t_date date , partition_date string ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\t', 'serialization.format'='\t') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ;
load data inpath 'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/external/rawdb2.db/ext_tf_cube_bm_result' into table flattened_tf_cube_bm_result;
insert overwrite table tf_cube_bm_result partition(partition_date ) select * from flattened_tf_cube_bm_result;
drop table flattened_tf_cube_bm_result;
MSCK REPAIR TABLE tf_cube_bm_result;
CREATE TABLE `tf_cube_bm_result2`(
  `d_month` string, 
  `region` string, 
  `category` string, 
  `kpi_name` string, 
  `kpi_total` double, 
  `t_date` date)
PARTITIONED BY ( 
  `partition_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/managed/rawdb2.db/tf_cube_bm_result2'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1584928647');
MSCK REPAIR TABLE tf_cube_bm_result2;
CREATE TABLE `tf_cube_bm_result3`(
  `d_month` string, 
  `region` string, 
  `category` string, 
  `kpi_name` string, 
  `kpi_total` double, 
  `t_date` date)
PARTITIONED BY ( 
  `partition_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/managed/rawdb2.db/tf_cube_bm_result3'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1584928742');
MSCK REPAIR TABLE tf_cube_bm_result3;
CREATE EXTERNAL TABLE `vdc_miles_parsing`(
  `score` map<string,string>)
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'collection.delim'=',', 
  'mapkey.delim'=':') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'abfs://sourceidgmcspark001@idgmchdfs001.dfs.core.chinacloudapi.cn/hive/warehouse/external/rawdb2.db/vdc_miles_parsing'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1584927389');
MSCK REPAIR TABLE vdc_miles_parsing;
