CREATE TABLE [Queue]
(
	[Table] SYSNAME NOT NULL,	
	CONSTRAINT PK_Queue PRIMARY KEY ([Table])
)

INSERT INTO [Queue]
SELECT 'dbo.Test'


CREATE TABLE [dbo].[Test_Log]
(
	[MessageId] UNIQUEIDENTIFIER NOT NULL,
	[Xml] XML NOT NULL 	
)

GO

CREATE TABLE [dbo].[Test]
(
	[Id] INT NOT NULL PRIMARY KEY, 
    [ReplyTo] SYSNAME NOT NULL, 
    [EndPoint] SYSNAME NOT NULL
)

GO

CREATE PROCEDURE [dbo].[sp_ImportTest]
	@MessageId UNIQUEIDENTIFIER,	
	@From SYSNAME,
	@Xml XML
AS	
	WAITFOR DELAY '00:00:10';
	INSERT INTO Test_Log (MessageId, Xml) VALUES (@MessageId, @Xml)
GO

CREATE PROCEDURE [dbo].[sp_ExportTest]
	@MessageId UNIQUEIDENTIFIER = NULL OUT,
	@To NVARCHAR(900),	
	@Id INT
AS	
	-- change messageId
	SET @MessageId = CAST(HASHBYTES('MD5', cast(@Id as varchar)) AS UNIQUEIDENTIFIER) 
	--WAITFOR DELAY '00:00:05';
	SELECT * FROM (SELECT @MessageId MessageId, @Id Id) Export FOR XML AUTO, TYPE
GO

DELETE Test

INSERT INTO [dbo].[Test] (Id, ReplyTo, EndPoint) 
VALUES (1, 'dbo.sp_ExportTest','mssql://QueueHost.Local/nservicebus.dbo.sp_ImportTest')

EXEC sp_ExportTest @To='xx',@Id=111

DELETE Test_Log
SELECT * FROM Test_Log
