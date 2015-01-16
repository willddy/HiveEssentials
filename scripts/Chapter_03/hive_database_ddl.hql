--Apache Hive Essentials 
--Chapter 3 Code - Hive Database DDL

--Create database without checking if the database already exists.
CREATE DATABASE myhivebook;

–-Create database and checking if the database already exists.
CREATE DATABASE IF NOT EXISTS myhivebook;

--Create database with location, comments, and metadata information
CREATE DATABASE IF NOT EXISTS myhivebook
COMMENT 'hive database demo'
LOCATION '/hdfs/directory'
WITH DBPROPERTIES ('creator'='dayongd','date'='2015-01-01');

--Show and describe database with wildcards
SHOW DATABASES;
SHOW DATABASES LIKE 'my.*';
DESCRIBE DATABASE default;

--Use the database
USE myhivebook;

--Drop the empty database.
DROP DATABASE IF EXISTS myhivebook;

--Drop database with CASCADE
DROP DATABASE IF EXISTS myhivebook CASCADE;

--metadata about database could not be changed.
ALTER DATABASE myhivebook SET DBPROPERTIES ('edited-by' = 'Dayong');

ALTER DATABASE myhivebook SET OWNER user dayongd;