CREATE TABLE QueuePump
(	
	QueueName SYSNAME,
	ConnectionString VARCHAR(400),
)

INSERT INTO QueuePump
SELECT 'dbo.Test', 'Data Source=.\SQLEXPRESS;Initial Catalog=nservicebus;Integrated Security=True'

GO

CREATE TABLE [dbo].[Test_Log]
(
	[MessageId] UNIQUEIDENTIFIER NOT NULL,
	[Xml] XML NOT NULL 	
)

GO

CREATE TABLE [dbo].[Test]
(
	[Id] INT NOT NULL PRIMARY KEY, 
    [ReplyTo] NVARCHAR(900) NOT NULL, 
    [EndPoint] NVARCHAR(900) NOT NULL
)

GO

CREATE PROCEDURE [dbo].[sp_ImportTest]
	@MessageId uniqueidentifier,	
	@From NVARCHAR(900),
	@Xml xml
AS	
	WAITFOR DELAY '00:00:10';
	INSERT INTO Test_Log (MessageId, Xml) VALUES (@MessageId, @Xml)
GO

CREATE PROCEDURE [dbo].[sp_ExportTest]
	@MessageId uniqueidentifier out,
	@To NVARCHAR(900),	
	@Id int
AS	
	-- change messageId
	set @MessageId = CAST(HASHBYTES('MD5', cast(@Id as varchar) + 'XX') AS UNIQUEIDENTIFIER) 
	WAITFOR DELAY '00:00:05';
	SELECT * FROM (SELECT @MessageId MessageId, @Id Id) Export FOR XML AUTO, TYPE
GO

DELETE Test

INSERT INTO [dbo].[Test] (Id, ReplyTo, EndPoint) 
VALUES (1, 'dbo.sp_ExportTest','mssql://localhost/nservicebus.dbo.sp_ImportTest')

EXEC sp_ExportTest 'xx',111

DELETE Test_Log
SELECT * FROM Test_Log