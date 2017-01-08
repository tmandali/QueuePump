ALTER PROCEDURE [dbo].[sp_ImportTest]
	@From NVARCHAR(900),
	@Xml xml
AS
	INSERT INTO Test_Log (Xml) VALUES (@Xml)
GO

ALTER PROCEDURE [dbo].[sp_ExportTest]
	@To NVARCHAR(900),
	@Id int
AS
	SELECT * FROM (SELECT @Id Id) Export FOR XML AUTO, TYPE
GO

CREATE TABLE [dbo].[Test_Log]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
	[Xml] XML NOT NULL 	
)

CREATE TABLE [dbo].[Test]
(
	[Id] INT NOT NULL PRIMARY KEY, 
    [ReplyTo] NVARCHAR(900) NOT NULL, 
    [EndPoint] NVARCHAR(900) NOT NULL
)

DELETE Test

INSERT INTO [dbo].[Test] (Id, ReplyTo, EndPoint) 
VALUES (1, 'dbo.sp_ExportTest','mssql://localhost/testdb.dbo.sp_ImportTest')

EXEC sp_ExportTest 'xx',111

DELETE Test_Log
SELECT * FROM Test_Log