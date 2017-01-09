ALTER PROCEDURE [dbo].[sp_ImportTest]
	@MessageId uniqueidentifier,	
	@From NVARCHAR(900),
	@Xml xml
AS	
	WAITFOR DELAY '00:00:10';
	INSERT INTO Test_Log (Id, Xml) VALUES (@MessageId, @Xml)
GO

ALTER PROCEDURE [dbo].[sp_ExportTest]
	@MessageId uniqueidentifier out,
	@To NVARCHAR(900),	
	@Id int
AS	
	-- change messageId
	set @MessageId = CAST(HASHBYTES('MD5', cast(@Id as varchar) + 'XX') AS UNIQUEIDENTIFIER) 
	WAITFOR DELAY '00:00:05';
	SELECT * FROM (SELECT @MessageId MessageId, @Id Id) Export FOR XML AUTO, TYPE
GO

CREATE TABLE [dbo].[Test_Log]
(
	[Id] UNIQUEIDENTIFIER NOT NULL,
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
VALUES (1, 'dbo.sp_ExportTest','mssql://localhost/nservicebus.dbo.sp_ImportTest')

EXEC sp_ExportTest 'xx',111

DELETE Test_Log
SELECT * FROM Test_Log