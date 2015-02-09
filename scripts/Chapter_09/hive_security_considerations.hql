--Apache Hive Essentials
--Chapter 9 Code - Security Considerations
   
--Use encryption and decryption UDF 
ADD JAR /home/dayongd/Downloads/hiveessentials-1.0-SNAPSHOT.jar;                    

CREATE TEMPORARY FUNCTION aesencrypt AS 'com.packtpub.hive.essentials.hiveudf.AESEncrypt';
CREATE TEMPORARY FUNCTION aesdecrypt AS 'com.packtpub.hive.essentials.hiveudf.AESDecrypt';

SELECT aesencrypt('Will') AS encrypt_name FROM employee LIMIT 1;                         
SELECT aesdecrypt('YGvo54QIahpb+CVOwv9OkQ==') AS decrypt_name FROM employee LIMIT 1;   
