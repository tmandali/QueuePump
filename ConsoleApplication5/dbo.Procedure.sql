CREATE TABLE [Queue]
(
	[Table] SYSNAME NOT NULL,	
	[ConnectionString] NVARCHAR(900) NOT NULL,
	CONSTRAINT PK_Queue PRIMARY KEY ([Table])
)

CREATE TABLE [Transform]
(
	[Table] SYSNAME NOT NULL,	
	[EndPoint] SYSNAME NOT NULL,
	[Transform] XML NOT NULL,
	CONSTRAINT PK_QueueTransform PRIMARY KEY ([Table], [EndPoint])
)


INSERT INTO [Queue]
SELECT 'dbo.Test', 'Data Source=.;Initial Catalog=nservicebus;Integrated Security=True',
'<xsd:schema xmlns:schema="http://www.lcwaikiki.com/queue" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:sqltypes="http://schemas.microsoft.com/sqlserver/2004/sqltypes" elementFormDefault="qualified">
  <xsd:import namespace="http://schemas.microsoft.com/sqlserver/2004/sqltypes" schemaLocation="http://schemas.microsoft.com/sqlserver/2004/sqltypes/sqltypes.xsd" />
  <xsd:element name="Export">
    <xsd:complexType>
      <xsd:attribute name="MessageId" type="sqltypes:uniqueidentifier" />
      <xsd:attribute name="Id" type="sqltypes:int" />
    </xsd:complexType>
  </xsd:element>
</xsd:schema>'
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
VALUES (1, 'dbo.sp_ExportTest','mssql://localhost/nservicebus.dbo.sp_ImportTest')

EXEC sp_ExportTest @To='xx',@Id=111

DELETE Test_Log
SELECT * FROM Test_Log