--Apache Hive Essentials 
--Chapter 10 Code - Work with Other Tools

--Hive HBase integration
CREATE TABLE hbase_table_sample(
id int,
value1 string,
value2 string,
map_value map<string, string>
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val,cf2:val,cf3:")
TBLPROPERTIES ("hbase.table.name" = "table_name_in_hbase");

--ZooKeeper and Locks
--Lock table and specify lock type
LOCK TABLE employee shared;

--Show the lock information on the specific tables
SHOW LOCKS employee EXTENDED;

--Release lock on the table
UNLOCK TABLE employee;

--Show all locks in the database
SHOW LOCKS;

LOCK TABLE employee exclusive;

SHOW LOCKS employee EXTENDED;

SELECT * FROM employee;