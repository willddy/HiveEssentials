--Apache Hive Essentials 
--Chapter 3 Code - Hive Partition and Buckets DDL

--Create partition table DDL
CREATE TABLE employee_partitioned
(
  name string,
  work_place ARRAY<string>,
  sex_age STRUCT<sex:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
PARTITIONED BY (Year INT, Month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

--Show partitions
SHOW PARTITIONS employee_partitioned;

--Add multiple partitions
ALTER TABLE employee_partitioned ADD 
PARTITION (year=2014, month=11)        
PARTITION (year=2014, month=12);

SHOW PARTITIONS employee_partitioned;

ALTER TABLE employee_partitioned DROP PARTITION (year=2014, month=11);

SHOW PARTITIONS employee_partitioned;

--Load data to the partition
LOAD DATA LOCAL INPATH '/home/dayongd/Downloads/employee.txt' 
OVERWRITE INTO TABLE employee_partitioned
PARTITION (year=2014, month=12);

--Verify data loaded
SELECT name, year, month FROM employee_partitioned;

--Create a table with bucketing
--Prepare data for backet tables
CREATE TABLE employee_id                         
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  sex_age STRUCT<sex:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '/home/dayongd/Downloads/employee_id.txt' 
OVERWRITE INTO TABLE employee_id

--Create the bucket table
CREATE TABLE employee_id_buckets                         
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  sex_age STRUCT<sex:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
CLUSTERED BY (employee_id) INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

set map.reduce.tasks = 2;

set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE employee_id_buckets SELECT * FROM employee_id;
