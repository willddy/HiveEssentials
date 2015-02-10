--Apache Hive Essentials
--Chapter 6 Code - Hive Data Aggregation and Sampling

--Aggregation without GROUP BY columns
SELECT count(*) AS row_cnt FROM employee;

--Aggregation with GROUP BY columns
SELECT sex_age.sex, count(*) AS row_cnt FROM employee 
GROUP BY sex_age.sex;

--The column age is not in the group by columns, 
--FAILED: SemanticException [Error 10002]: Line 1:15 Invalid column reference 'age'
--SELECT sex_age.age, sex_age.sex, count(*) AS row_cnt 
--FROM employee GROUP BY sex_age.sex;

--Find row count by sex and random age for each sex
SELECT sex_age.sex,collect_set(sex_age.age)[0] AS random_age, 
count(*) AS row_cnt FROM employee GROUP BY sex_age.sex;

--Multiple aggregate functions are called in the same SELECT
SELECT sex_age.sex, AVG(sex_age.age) AS avg_age, 
count(*) AS row_cnt FROM employee GROUP BY sex_age.sex; 

--Aggregate functions are used with CASE WHEN 
SELECT sum(CASE WHEN sex_age.sex = 'Male' THEN sex_age.age
ELSE 0 END)/count(CASE WHEN sex_age.sex = 'Male' THEN 1 
ELSE NULL END) AS male_age_avg FROM employee;

--Aggregate functions are used with COALESCE and IF 
SELECT
sum(coalesce(sex_age.age,0)) AS age_sum,
sum(if(sex_age.sex = 'Female',sex_age.age,0)) 
AS female_age_sum FROM employee;

--Nested aggregate functions are not allowed
--FAILED: SemanticException [Error 10128]: Line 1:11 Not yet supported place for UDAF 'count'
--SELECT avg(count(*)) AS row_cnt FROM employee;                    

--Aggregate functions can be also used with DISTINCT keyword to do aggregation on unique values.
SELECT count(distinct sex_age.sex) AS sex_uni_cnt,
count(distinct name) AS name_uni_cnt FROM employee;     

--Trigger single reducer during the whole processing
SELECT count(distinct sex_age.sex) AS sex_uni_cnt FROM employee;

--Use subquery to select unique value before aggregations for better performance
SELECT count(*) AS sex_uni_cnt FROM (SELECT distinct sex_age.sex FROM employee) a;

--Aggregation across columns with NULL value.
--Prepare a table for testing
CREATE TABLE t
AS
SELECT * FROM
(SELECT employee_id-99 AS val1, (employee_id-98) AS val2 FROM employee_hr 
WHERE employee_id <= 101
UNION ALL
SELECT null val1, 2 AS val2 FROM employee_hr 
WHERE employee_id = 100) a;

--Check the table rows 
SELECT * FROM t;

--The 2nd row (NULL, 2) are ignored when doing sum(val1+val2)
SELECT sum(val1), sum(val1+val2) FROM t;                   

SELECT sum(coalesce(val1,0)), sum(coalesce(val1,0)+val2) FROM t;

--hive.map.aggr property controls whether to do aggregations in the map task. 
SET hive.map.aggr=true;

--GROUPING__ID
SELECT GROUPING__ID, 
BIN(CAST(GROUPING__ID AS BIGINT)) AS bit_vector, 
name, start_date, count(employee_id) emp_id_cnt 
FROM employee_hr 
GROUP BY start_date, name WITH CUBE ORDER BY start_date;

--Aggregation condition – HAVING
SELECT sex_age.age FROM employee 
GROUP BY sex_age.age HAVING count(*)<=1;

--If we do not use HAVING, we can use subquery as follows. 
SELECT a.age
FROM
(SELECT count(*) as cnt, sex_age.age 
FROM employee GROUP BY sex_age.age
) a WHERE a.cnt<=1;

--Prepare table and data for demonstration
CREATE TABLE IF NOT EXISTS employee_contract
(
name string,
dept_num int,
employee_id int,
salary int,
type string,
start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH
'/home/dayongd/Downloads/employee_contract.txt' 
OVERWRITE INTO TABLE employee_contract;

--The regular aggregations are used as analytic functions
SELECT name, dept_num, salary,
COUNT(*) OVER (PARTITION BY dept_num) AS row_cnt,
SUM(salary) OVER(PARTITION BY dept_num ORDER BY dept_num) AS deptTotal,
SUM(salary) OVER(ORDER BY dept_num) AS runningTotal1,
SUM(salary) OVER(ORDER BY dept_num, name rows unbounded 
preceding) AS runningTotal2
FROM employee_contract
ORDER BY dept_num, name;

--Other analytic functions
SELECT name, dept_num, salary,
RANK() OVER (PARTITION BY dept_num ORDER BY salary) AS rank, 
DENSE_RANK() OVER (PARTITION BY dept_num ORDER BY salary) 
AS dense_rank,
ROW_NUMBER() OVER () AS row_num,
ROUND((CUME_DIST() OVER (PARTITION BY dept_num 
ORDER BY salary)), 1) AS cume_dist,
PERCENT_RANK() OVER(PARTITION BY dept_num 
ORDER BY salary) AS percent_rank,
NTILE(4) OVER(PARTITION BY dept_num ORDER BY salary) AS ntile
FROM employee_contract
ORDER BY dept_num;

SELECT name, dept_num, salary,
LEAD(salary, 2) OVER(PARTITION BY dept_num 
ORDER BY salary) AS lead,
LAG(salary, 2, 0) OVER(PARTITION BY dept_num 
ORDER BY salary) AS lag,
FIRST_VALUE(salary) OVER (PARTITION BY dept_num 
ORDER BY salary) AS first_value,
LAST_VALUE(salary) OVER (PARTITION BY dept_num 
ORDER BY salary) AS last_value_default,
LAST_VALUE(salary) OVER (PARTITION BY dept_num 
ORDER BY salary 
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
AS last_value
FROM employee_contract ORDER BY dept_num;

SELECT name, dept_num AS dept, salary AS sal,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY
name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) win1,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) win2,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) win3,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) win4,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) win5,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN CURRENT ROW AND CURRENT ROW) win7,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) win8,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) win9,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) win10,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) win11,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING) win12,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
name ROWS 2 PRECEDING) win13
FROM employee_contract
ORDER BY dept_num, name;

SELECT name, dept_num, salary,
MAX(salary) OVER w1 AS win1,
MAX(salary) OVER w1 AS win2,
MAX(salary) OVER w1 AS win3
FROM employee_contract
ORDER BY dept_num, name
WINDOW
w1 AS (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 
2 PRECEDING AND CURRENT ROW),
w2 AS w3,
w3 AS (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 
1 PRECEDING AND 2 FOLLOWING);

SELECT name, salary, start_year,
MAX(salary) OVER (PARTITION BY dept_num ORDER BY 
start_year RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) win1
FROM
(
  SELECT name, salary, dept_num, 
  YEAR(start_date) AS start_year
  FROM employee_contract
) a;

--Bucket table sampling example
SELECT name FROM employee_id_buckets 
TABLESAMPLE(BUCKET 1 OUT OF 2 ON rand()) a;

--Block sampling - Sample by rows
SELECT name FROM employee_id_buckets TABLESAMPLE(4 ROWS) a;

--Sample by percentage of data size
SELECT name FROM employee_id_buckets TABLESAMPLE(10 PERCENT) a;

--Sample by data size
SELECT name FROM employee_id_buckets TABLESAMPLE(3M) a;   