--Apache Hive Essentials 
--Chapter 3 Code - Hive Table DDL

--Create internal table and load the data
CREATE TABLE IF NOT EXISTS employee_internal 
(
  name string,
  work_place ARRAY<string>,
  sex_age STRUCT<sex:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an internal table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/hadoop/employee.txt' OVERWRITE INTO TABLE employee_internal;

--Create external table and load the data
CREATE EXTERNAL TABLE IF NOT EXISTS employee_external
 (
   name string,
   work_place ARRAY<string>,
   sex_age STRUCT<sex:string,age:int>,
   skills_score MAP<string,int>,
   depart_title MAP<STRING,ARRAY<STRING>>
 )
COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/user/dayongd/employee';

LOAD DATA LOCAL INPATH '/home/hadoop/employee.txt' OVERWRITE INTO TABLE employee_external;

--Create Table With Data - CREATE TABLE AS SELECT (CTAS)
CREATE TABLE ctas_employee AS SELECT * FROM employee_external;

--Create Table As SELECT (CTAS) with Common Table Expression (CTE) 
CREATE TABLE cte_employee AS
WITH r1 AS (SELECT name FROM r2 WHERE name = 'Michael'),
r2 AS (SELECT name FROM employee WHERE sex_age.sex= 'Male'),
r3 AS (SELECT name FROM employee WHERE sex_age.sex= 'Female')
SELECT * FROM r1 UNION ALL select * FROM r3;

SELECT * FROM cte_employee;

--Create Table Without Data - TWO ways 
--With CTAS
CREATE TABLE empty_ctas_employee AS SELECT * FROM employee_internal WHERE 1=2;

--With LIKE
CREATE TABLE empty_like_employee LIKE employee_internal;

--Check row count for both tables
SELECT COUNT(*) AS row_cnt FROM empty_ctas_employee;
SELECT COUNT(*) AS row_cnt FROM empty_like_employee;

--Drop table 
DROP TABLE IF EXISTS empty_ctas_employee;

DROP TABLE IF EXISTS empty_like_employee;

--Truncate table
SELECT * FROM cte_employee;

TRUNCATE TABLE cte_employee;

SELECT * FROM cte_employee;

--Alter table statements
--Alter table name
ALTER TABLE cte_employee RENAME TO c_employee;

--Alter table properties, such as comments
ALTER TABLE c_employee SET TBLPROPERTIES ('comment' = 'New name with new comments');

--Alter table delimiter through SerDe properties
ALTER TABLE employee_internal SET SERDEPROPERTIES ('field.delim' = '$');

--Alter Table File Format
ALTER TABLE c_employee SET FILEFORMAT RCFILE;

--Alter Table Location
ALTER TABLE c_employee SET LOCATION 'hdfs://localhost:8020/user/dayongd/employee'; 

--Alter Table Location
ALTER TABLE c_employee ENABLE NO_DROP; 
ALTER TABLE c_employee DISABLE NO_DROP; 
ALTER TABLE c_employee ENABLE OFFLINE;
ALTER TABLE c_employee DISABLE OFFLINE;

--Alter Table Concatenate to merge small files into larger files
--convert to the file format supported
ALTER TABLE c_employee SET FILEFORMAT ORC;
 
--concatenate files
ALTER TABLE c_employee CONCATENATE;

--convert to the regular file format
ALTER TABLE c_employee SET FILEFORMAT TEXTFILE;


--Alter columns
--Change column type - before changes
DESC employee_internal; 

--Change column type
ALTER TABLE employee_internal CHANGE name employee_name string AFTER sex_age;

--Verify the changes 
DESC employee_internal; 

--Change column type
ALTER TABLE employee_internal CHANGE employee_name name string FIRST;

--Verify the changes 
DESC employee_internal; 

--Add/Replace Columns-before add
DESC c_employee;      

--Add columns to the table
ALTER TABLE c_employee ADD COLUMNS (work string);

--Verify the added columns
DESC c_employee;      

--Replace all columns
ALTER TABLE c_employee REPLACE COLUMNS (name string);

--Verify the replaced all columns
DESC c_employee;   