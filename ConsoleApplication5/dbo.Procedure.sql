CREATE SEQUENCE [dbo].[Queue_Seq] 
 AS [bigint]
 START WITH 1
 INCREMENT BY 1
 MINVALUE 1
 CACHE 
GO

CREATE TABLE [Queue]
(
	[Table] SYSNAME NOT NULL,	
	CONSTRAINT PK_Queue PRIMARY KEY ([Table])
)

INSERT INTO [Queue]
SELECT 'dbo.Test'


CREATE TABLE [dbo].[Test]
(
	[TableKey] INT NOT NULL, 
    [ReplyTo] SYSNAME NOT NULL, 
    [EndPoint] SYSNAME NOT NULL,
	[DeliveryDate] DATETIME NOT NULL,
	[Error] NVARCHAR(4000),
	[RowVersion] BIGINT NOT NULL PRIMARY KEY DEFAULT NEXT VALUE FOR Queue_Seq,
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

INSERT INTO [dbo].[Test] (Id, ReplyTo, EndPoint, DeliveryDate) 
VALUES (3, 'dbo.sp_ExportTest','mssql://QueueHost.Local1/nservicebus.dbo.sp_ImportTest', GETUTCDATE())

EXEC sp_ExportTest @To='xx',@Id=111

SELECT * FROM Test
