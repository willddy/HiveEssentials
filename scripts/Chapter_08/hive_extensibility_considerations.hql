--Apache Hive Essentials
--Chapter 8 Code - Extensibility Considerations
   
--UDF deployment 
ADD JAR /home/dayongd/hive/lib/hive-to-upper-udf.jar;
CREATE TEMPORARY FUNCTION toUpper AS 'com.packtpub.hive.essentials.hiveudf.ToUpper';
SHOW FUNCTIONS ToUpper;
DESCRIBE FUNCTION ToUpper;
DESCRIBE FUNCTION EXTENDED ToUpper;
SELECT toUpper(name) FROM employee LIMIT 1000;
DROP TEMPORARY FUNCTION IF EXISTS toUpper;
 
--Streaming, call the script in Hive CLI from HQL.
ADD FILE /home/dayongd/Downloads/upper.py;
SELECT TRANSFORM (name,work_place[0]) 
USING 'python upper.py' AS (CAP_NAME,CAP_PLACE) 
FROM employee;

--LazySimpleSerDe
CREATE TABLE test_serde_lz
STORED AS TEXTFILE AS
SELECT name from employee;

--ColumnarSerDe
CREATE TABLE test_serde_cs
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFile AS
SELECT name from employee;

--RegexSerDe-Parse , seperate fields
CREATE TABLE test_serde_rex(
name string,
sex string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES(
'input.regex' = '([^,]*),([^,]*),([^,]*)',
'output.format.string' = '%1$s %2$s %3$s'
)
STORED AS TEXTFILE;

--HBaseSerDe. Make sure you have HBase installed before running query below.
CREATE TABLE test_serde_hb(
id string,
name string,
sex string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY
'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping"=
":key,info:name,info:sex,info:age"
)
TBLPROPERTIES("hbase.table.name" = "test_serde");

--AvroSerDe
CREATE TABLE test_serde_avro(
name string,
sex string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';

--ParquetHiveSerDe
CREATE TABLE test_serde_parquet
STORED AS PARQUET AS
SELECT name from employee;

--OpenCSVSerDe. Before Hive 0.14.0, You can also install the implementation from https://github.com/ogrodnek/csv-serde.
CREATE TABLE test_serde_csv(
name string,
sex string,
age string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

--JSONSerDe, make sure you install it (https://github.com/rcongiu/Hive-JSON-Serde) before running query below.
CREATE TABLE test_serde_js(
name string,
sex string,
age string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;
