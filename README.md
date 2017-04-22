# DavisBase
A Database Engine with terminal access to perform CRUD operations on database.

ReadMe for DavisBase:

Author Name: Parag Pravin Dakle

******************************
     Build and Run Steps
******************************
The system was tested successfully on a Ubuntu 16.04 LTS system.

Kindly follow the following steps to build the project on a Linux system:
1. clone the repo
2. cd <complete_path_to>/src/
3. javac DavisBasePrompt.java

The above steps will create an executable DavisBasePrompt.class file.
To run the project use the following command from the src folder:

java DavisBasePrompt

On running the above command successfully the following text should be seen:
--------------------------------------------------------------------------------
Welcome to DavisBaseLite
DavisBaseLite Version v1.0
©2017 Parag Pravin Dakle

Type 'help;' to display supported commands.
--------------------------------------------------------------------------------
daklesql> 

************************************
     List of Supported Commands
************************************

The following list can be seen by running the help; command:

SUPPORTED COMMANDS
All commands below are case insensitive

	USE DATABASE database_name;                      Changes current database.
	CREATE DATABASE database_name;                   Creates an empty database.
	SHOW DATABASES;                                  Displays all databases.
	DROP DATABASE database_name;                     Deletes a database.
	SHOW TABLES;                                     Displays all tables in current database.
	DESC table_name;                                 Displays table schema.
	CREATE TABLE table_name (                        Creates a table in current database.
		<column_name> <datatype> [PRIMARY KEY | NOT NULL]
		...);
	DROP TABLE table_name;                           Deletes a table data and its schema.
	SELECT <column_list> FROM table_name             Display records whose rowid is <id>.
		[WHERE rowid = <value>];
	INSERT INTO table_name                           Inserts a record into the table.
		[(<column1>, ...)] VALUES (<value1>, <value2>, ...);
	DELETE FROM table_name [WHERE condition];        Deletes a record from a table.
	UPDATE table_name SET <conditions>               Updates a record from a table.
		[WHERE condition];
	VERSION;                                         Display current database engine version.
	HELP;                                            Displays help information
	EXIT;                                            Exits the program

************************************************
     Few Sample Commands Tested Successfully
************************************************
Basic commands are with respect to Catalog Database;


1. show databases;

2. use catalog;

3. show tables;

4. desc davisbase_columns;

5. select * from davisbase_columns;

6. select * from davisbase_columns where table_name="davisbase_tables";

7. create database sample;

8. CREATE TABLE sample_table1 (ssn INT PRIMARY KEY, fName TEXT NOT NULL, nName TEXT, address TEXT, DOB DATE NOT NULL,  DOB1 DATETIME, ti TINYINT, si SMALLINT, i INT, bi BIGINT, r REAL, d DOUBLE);

9. insert into sample_table1 values (116 , 'r23aeqw', 'jtryrty', 'nmbn', '1989-11-12', '1989-11-12 12:12:12', 12, 256, 500, 50000, 45.56, 45.7989);

10. update sample_table1 set i=1000 where ssn=116;

11. delete from sample_table1;

12. drop table sample_table1;

13. drop database sample;

14. exit;

*********************
     Assumptions
*********************

1. When updating string values, values of same length should be given, else system can behave erratically.

2. When using conditions on TEXT columns, only '=' is supported.

3. The directory structure for all databases is as follows:
     database_name/
          +-/table_name1.tbl
          +-/table_name2.tbl
          
4. Supported Data Types [Database Names (Java Class Names)]:
     a. TinyInt (Byte)
     b. SmallInt (Short)
     c. Int (Integer)
     d. BigInt (Long)
     e. Real (Float)
     f. Double (Double)
     g. Date
     h. DateTime
     i. Text (String)
     
5. Only one condition in SELECT, UPDATE and DELETE command is supported.

6. Page size is fixed to 512 bytes.

7. Page Splitting is done using two logics:
     a. If the new record to be inserted is at the end, existing records go to the new left page and new record goes to the new right page. Here table is using auto increment is assumed.
     b. If the new record to be inserted in anywhere else then the records are split into half from the center.
     
8. On Page split the pointer record in INTERNAL PAGE of BTree uses the followin logic - Everything less than key is in the left child and rest in right.

9. Currently only records of size <= 250 bytes are supported. If record of size > 250 bytes is added, behaviour has not been tested.

10. Explicitly NULL value cannot be set to a column in INSERT query.

*********************
     Known Bugs
*********************

1. In update query, setting the primary key value to an existing one works. Not fixed due to time restrictions.

***************************Thank you! Hope you enjoy using the database!***************************

