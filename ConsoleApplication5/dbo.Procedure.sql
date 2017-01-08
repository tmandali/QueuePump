ALTER PROCEDURE [dbo].[sp_ImportTest]
	@EndPoint NVARCHAR(900),
	@Xml xml
AS
	INSERT INTO Test_Log (Xml) VALUES (@Xml)
GO

ALTER PROCEDURE [dbo].[sp_ExportTest]
	@Id int
AS
	SELECT Id FROM Test where Id=@Id FOR XML AUTO, TYPE
GO

CREATE TABLE [dbo].[Test_Log]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
	[Xml] XML NOT NULL 	
)

CREATE TABLE [dbo].[Test]
(
	[Id] INT NOT NULL PRIMARY KEY, 
    [ReplyToAdress] NVARCHAR(900) NOT NULL, 
    [EndPoint] NVARCHAR(900) NOT NULL
)

DELETE Test

INSERT INTO [dbo].[Test] (Id, ReplyToAdress, EndPoint) 
VALUES (1, 'dbo.sp_ExportTest','mssql://localhost/testdb.dbo.sp_ImportTest')

EXEC sp_ExportTest 1


SELECT * FROM Test_Log


