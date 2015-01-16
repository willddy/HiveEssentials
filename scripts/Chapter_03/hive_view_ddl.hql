--Apache Hive Essentials 
--Chapter 3 Code - Hive View DDL

--Create Hive view
CREATE VIEW employee_skills
AS
SELECT name, skills_score['DB'] AS DB,
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python,
skills_score['Sales'] as Sales, skills_score['HR'] as HR 
FROM employee;

--Alter views properties
ALTER VIEW employee_skills SET TBLPROPERTIES ('comment' = 'This is a view');

--Redefine views
ALTER VIEW employee_skills AS SELECT * from employee ;

--Drop views
DROP VIEW employee_skills; 
