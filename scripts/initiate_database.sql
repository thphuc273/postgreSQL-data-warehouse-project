/*
====================================================
Create Database and Schemas
====================================================
Script purpose:
    This script creates a new database named 'DataWareHouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.
WARNING:
    Running this script will delete the existing 'datawarehouse' database and all its contents.
    Ensure that you have backups of any important data before executing this script.
*/


DROP DATABASE IF EXISTS datawarehouse;

CREATE DATABASE DataWareHouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO




