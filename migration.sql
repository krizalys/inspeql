CREATE DATABASE db
GO

USE db
GO

CREATE TABLE tbl1(
    i int,
    s varchar
)
GO

CREATE TABLE tbl2(
    j int,
    c char
)
GO

INSERT INTO tbl1
VALUES
(1, 'A'),
(11, 'AA')
GO

INSERT INTO tbl2
VALUES
(2, 'B')
GO

SELECT * FROM tbl1
GO

SELECT * FROM tbl2
GO
