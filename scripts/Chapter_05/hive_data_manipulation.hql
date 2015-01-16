--Apache Hive Essentials
--Chapter 5 Code - Hive data manupulation

--Create partition table DDL.
--Load local data to table
LOAD DATA LOCAL INPATH '/home/dayongd/Downloads/employee_hr.txt' OVERWRITE INTO TABLE employee_hr;

--Load local data to partition table
LOAD DATA LOCAL INPATH '/home/dayongd/Downloads/employee.txt'
OVERWRITE INTO TABLE employee_partitioned
PARTITION (year=2014, month=12);

--Load HDFS data to table using default system path
LOAD DATA INPATH '/user/dayongd/employee/employee.txt' 
OVERWRITE INTO TABLE employee;

--Load HDFS data to table with full URI
LOAD DATA INPATH 
'hdfs://[dfs_hostname]:8020/user/dayongd/employee/employee.txt' 
OVERWRITE INTO TABLE employee;

--Data Exchange - INSERT
--Check the target table
SELECT name, work_place, sex_age FROM employee;              

--Populate data from SELECT
INSERT INTO TABLE employee
SELECT * FROM ctas_employee;

--Verify the data loaded
SELECT name, work_place, sex_age FROM employee;    

--INSERT from CTE
WITH a as (SELECT * FROM ctas_employee )
FROM a
INSERT OVERWRITE TABLE employee
SELECT *;

--Multiple INSERTS by only scanning the source table once
FROM ctas_employee
INSERT OVERWRITE TABLE employee
SELECT *
INSERT OVERWRITE TABLE employee_internal
SELECT * ;

--Dynamic partition is not enabled by default. We need to set following to make it work.
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

--Dynamic partition insert
INSERT INTO TABLE employee_partitioned PARTITION(year, month)
SELECT name, array('Toronto') as work_place, 
named_struct("sex","Male","age",30) as sex_age, 
map("Python",90) as skills_score,
map("R&D",array('Developer')) as depart_title, 
year(start_date) as year, month(start_date) as month
FROM employee_hr eh
WHERE eh.employee_id = 102;

--Verify the inserted row
SELECT name,depart_title,year,month FROM employee_partitioned
WHERE name = 'Steven';

--Insert to local files with default row separators
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output1' 
SELECT * FROM employee;

--Insert to local files with specified row separators
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output2' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT * FROM employee;

--Multiple INSERT
FROM employee
INSERT OVERWRITE DIRECTORY '/user/dayongd/output'
SELECT *
INSERT OVERWRITE DIRECTORY '/user/dayongd/output1'
SELECT * ;

--Export data and metadata of table
EXPORT TABLE employee TO '/user/dayongd/output3';

--Import table with the same name
IMPORT FROM '/user/dayongd/output3';              

--Import as new table
IMPORT TABLE empolyee_imported FROM '/user/dayongd/output3';

--Import as external table 
IMPORT EXTERNAL TABLE empolyee_imported_external 
FROM '/user/dayongd/output3'
LOCATION '/user/dayongd/output4' ; --Note, LOCATION property is optional.

--Export and import to partitions
EXPORT TABLE employee_partitioned partition 
(year=2014, month=11) TO '/user/dayongd/output5';

IMPORT TABLE employee_partitioned_imported 
FROM '/user/dayongd/output5';                     

--ORDER, SORT
SELECT name FROM employee ORDER BY NAME DESC;

--Use more than 1 reducer
SET mapred.reduce.tasks = 2;

SELECT name FROM employee SORT BY NAME DESC;   

--Use only 1 reducer
SET mapred.reduce.tasks = 1; 

SELECT name FROM employee SORT BY NAME DESC;   

SELECT name, employee_id 
FROM employee_hr DISTRIBUTE BY employee_id ; 

--Used with SORT BY
SELECT name, employee_id  
FROM employee_hr DISTRIBUTE BY employee_id SORT BY name; 

SELECT name, employee_id FROM employee_hr CLUSTER BY name ;   

--Complex datatype function
SELECT work_place, skills_score, depart_title FROM employee;

SELECT SIZE(work_place) AS array_size, 
SIZE(skills_score) AS map_size, 
SIZE(depart_title) AS complex_size, 
SIZE(depart_title["Product"]) AS nest_size 
FROM employee;

--Arrary functions
SELECT ARRAY_CONTAINS(work_place, 'Toronto') AS is_Toronto,
SORT_ARRAY(work_place) AS sorted_array FROM employee;

--Date and time functions
SELECT FROM_UNIXTIME(UNIX_TIMESTAMP()) AS current_time FROM employee LIMIT 1;

SELECT TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) AS current_date FROM employee LIMIT 1;

--To compare the difference of two date.
SELECT (unix_timestamp('2015-01-21 18:00:00') - unix_timestamp('2015-01-10 11:00:00'))/60/60/24 AS daydiff
FROM employee LIMIT 1;

--CASE for different data type
SELECT CASE WHEN 1 IS NULL THEN 'TRUE' ELSE 0 END AS case_result
FROM employee LIMIT 1;

--Parser and search tips
--Prepare data
INSERT INTO TABLE employee
SELECT 'Steven' AS name, array(null) as work_place,
named_struct("sex","Male","age",30) as sex_age, 
map("Python",90) as skills_score, 
map("R&D",array('Developer')) as depart_title
FROM employee LIMIT 1;

--Check what's inserted
SELECT name, work_place, skills_score FROM employee;

--LATERAL VIEW ignore the whole rows when EXPLORE returns NULL
SELECT name, workplace, skills, score
FROM employee
LATERAL VIEW explode(work_place) wp AS workplace
LATERAL VIEW explode(skills_score) ss AS skills, score;

--OUTER LATERAL VIEW keep rows when EXPLORE returns NULL
SELECT name, workplace, skills, score
FROM employee
LATERAL VIEW OUTER explode(work_place) wp AS workplace
LATERAL VIEW explode(skills_score) ss AS skills, score;

--Get the file name form a Linux path
SELECT
reverse(split(reverse('/home/user/employee.txt'),'/')[0])
AS linux_file_name FROM employee LIMIT 1;

--Functions not mentioned in the Hive WIKI
--Functions to check whether the value is null or not
SELECT work_place, isnull(work_place) is_null, 
isnotnull(work_place) is_not_null FROM employee;

--assert_true, throw an exception if 'condition' is not true. 
SELECT assert_true(work_place IS NULL) FROM employee;

--elt(n, str1, str2, ...),returns the n-th string
SELECT elt(2,'New York','Montreal','Toronto') FROM employee LIMIT 1;             

--Return the name of current_database
SELECT current_database();    

--Transactions
--Below configuration parameters must be set appropriately to turn on transaction support in Hive.
SET hive.support.concurrency = true;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.compactor.initiator.on = true;
SET hive.compactor.worker.threads = 1;

--Show avaliable transactions
SHOW TRANSACTIONS;
