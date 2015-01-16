--Apache Hive Essentials 
--Chapter 4 Code - Hive SELECT, JOIN, and UNION
--Query all columns in the table
SELECT * FROM employee;

--Select only one column
SELECT name FROM employee;

--Select unique rows
SELECT DISTINCT name FROM employee LIMIT 2;

--Enable fetch
SET hive.fetch.task.conversion=more;

--Verify the improvement, which is less than one second
SELECT name FROM employee;

--Use LIMIT and WHERE keywords
SELECT name, work_place FROM employee WHERE name = 'Michael';

--Nest SELECT using CTE
WITH t1 AS (
SELECT * FROM employee
WHERE sex_age.sex = 'Male')
SELECT name, sex_age.sex AS sex FROM t1;

--Nest SELECT after the FROM
SELECT name, sex_age.sex AS sex
FROM
(
  SELECT * FROM employee
  WHERE sex_age.sex = 'Male'
) t1;

--Subquery
SELECT name, sex_age.sex AS sex
FROM employee a
WHERE a.name IN
(SELECT name FROM employee
WHERE sex_age.sex = 'Male'
);

SELECT name, sex_age.sex AS sex
FROM employee a
WHERE EXISTS
(SELECT * FROM employee b
WHERE a.sex_age.sex = b.sex_age.sex AND b.sex_age.sex = 'Male'
);
 
--Prepare another table for join and load data
CREATE TABLE IF NOT EXISTS employee_hr
(
  name string,
  employee_id int,
  sin_number string,
  start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/Dayongd/employee_hr.txt' OVERWRITE INTO TABLE employee_hr;

--JOIN between two tables
SELECT emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name;

--JOIN between more tables
SELECT emp.name, empi.employee_id, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
JOIN employee_id empi ON emp.name = empi.name;

--Self join is used when the data in the table has nest logic
SELECT emp.name
FROM employee emp
JOIN employee emp_b
ON emp.name = emp_b.name;

--Implicit join, which support since Hive 0.13.0
SELECT emp.name, emph.sin_number
FROM employee emp, employee_hr emph
WHERE emp.name = emph.name;

--Join using different columns will create additional mapreduce
SELECT emp.name, empi.employee_id, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
JOIN employee_id empi ON emph.employee_id = empi.employee_id;

--Streaming tables 
SELECT /*+ STREAMTABLE(employee_hr) */
emp.name, empi.employee_id, emph.sin_number
FROM employee emp
JOIN employee_hr emph ON emp.name = emph.name
JOIN employee_id empi ON emph.employee_id = empi.employee_id;

--Left JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
LEFT JOIN employee_hr emph ON emp.name = emph.name;

--Right JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
RIGHT JOIN employee_hr emph ON emp.name = emph.name;

--Full OUTER JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
FULL JOIN employee_hr emph ON emp.name = emph.name;

--CROSS JOIN in different ways
SELECT emp.name, emph.sin_number
FROM employee emp
CROSS JOIN employee_hr emph;

SELECT emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph;

SELECT emp.name, emph.sin_number
FROM employee emp
JOIN employee_hr emph on 1=1;

--unequal JOIN
SELECT emp.name, emph.sin_number
FROM employee emp
CROSS JOIN employee_hr emph WHERE emp.name <> emph.name;

--An example MAP JOIN enabled by query hint
SELECT /*+ MAPJOIN(employee) */ emp.name, emph.sin_number
FROM employee emp
CROSS JOIN employee_hr emph WHERE emp.name <> emph.name;

--BUCKET Map Join settings
SET hive.optimize.bucketmapjoin = true; 
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 

--LEFT SEMI JOIN
SELECT a.name
FROM employee a
WHERE EXISTS
(SELECT * FROM employee_id b
WHERE a.name = b.name);

SELECT a.name
FROM employee a
LEFT SEMI JOIN employee_id b
ON a.name = b.name;

--UNION ALL
--Names in employee_hr table
SELECT name FROM employee_hr;

--Names in employee table
SELECT name FROM employee;   

--UNION ALL including duplications
SELECT a.name 
FROM employee a
UNION ALL
SELECT b.name 
FROM employee_hr b;

--Implement UNION between two tables without duplications
SELECT DISTINCT name
FROM
(
   SELECT a.name AS name
   FROM employee a
   UNION ALL
   SELECT b.name AS name
   FROM employee_hr b
) union_set;

--Table employee implements INTERCEPT employee_hr
SELECT a.name 
FROM employee a
JOIN employee_hr b
ON a.name = b.name;

--Table employee implements MINUS employee_hr
SELECT a.name 
FROM employee a
LEFT JOIN employee_hr b
ON a.name = b.name
WHERE b.name IS NULL;