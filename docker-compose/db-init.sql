-- Use master database to ensure the necessary changes are made
USE master;
GO

-- Check if the database 'mydb' exists. If not, create it
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'mydb')
BEGIN
    CREATE DATABASE mydb;
END
GO

-- Switch to the 'mydb' database
USE mydb;
GO


CREATE TABLE MessageQueue (
    id INT IDENTITY(1,1) PRIMARY KEY,
    message NVARCHAR(MAX),
    status NVARCHAR(50) NOT NULL DEFAULT 'PENDING',  -- 'PENDING', 'PROCESSED'
    created_at DATETIME2 NOT NULL,
    processed_at DATETIME2 NULL
);
