--Apache Hive Essentials 
--Chapter 03 - Hive Data Types 

--Create table using ARRAY, MAP, STRUCT, and Composite data type
CREATE TABLE employee
(
  name string,
  work_place ARRAY<string>,
  sex_age STRUCT<sex:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

--Verify tables creations run in Beeline
!table employee

--Load data
LOAD DATA LOCAL INPATH '/home/hadoop/employee.txt' OVERWRITE INTO TABLE employee;

--Query the whole table
SELECT * FROM employee;

--Query the ARRAY in the table
SELECT work_place FROM employee;

SELECT work_place[0] AS col_1, work_place[1] AS col_2, work_place[2] AS col_3 FROM employee;

--Query the STRUCT in the table
SELECT sex_age FROM employee;

SELECT sex_age.sex, sex_age.age FROM employee;

--Query the MAP in the table
SELECT skills_score FROM employee;

SELECT name, skills_score['DB'] AS DB, 
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python, 
skills_score['Sales'] as Sales, skills_score['HR'] as HR FROM employee;

SELECT depart_title FROM employee;

SELECT name, depart_title['Product'] AS Product, depart_title['Test'] AS Test,
depart_title['COE'] AS COE, depart_title['Sales'] AS Sales  
FROM employee;

SELECT name, 
depart_title['Product'][0] AS product_col0, 
depart_title['Test'][0] AS test_col0 
FROM employee;