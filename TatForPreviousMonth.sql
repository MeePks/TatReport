Use GiantEagleDPGroupTracker; --DB having clb trackers and file inventory table --replace this according to your client
GO
--GiantEagleDPGroupLoadLogger  --trackerloadlog table db --replace this according to your client
Create or Alter Procedure dbo.__getTat
@AuditName Varchar(100)='walmartmx',
@startDate Date='1900-01-01',
@EndDate date='1900-01-01'
AS
Begin
SET NOCOUNT ON

--Declare @startdate date, @enddate date
DECLARE @functionName VARCHAR(MAX) = 'WmtDecompress-File,WmtCopy-File';

/* Setting up Default Value incase no any value is passed in StartDate and EndDate */
--IF @startDate='1900-01-01'
	Set @startDate= (SELECT DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0))
--IF @EndDate='1900-01-01'
	set	@EndDate= (SELECT EOMONTH(GETDATE(), -1))

/* Setting up denormalized Tracking tables */
Drop table if exists dbo.DeNormTrackingTBL
	Select	d.DatasetID  
			,ds.schedule
			,[DataSetName]  
			,[DatasetDetailName]  
			,[FilePatterns]  
			,[FileExt]  
			,d.[DetailID]  
			,[MapID]  
			,[MapName]  
			,[MapFolder]  
			,[ServerName]  
			,[DatabaseName]  
			,[TableName]  
			,[AdvTracking]  
			,[ActiveMapBit] 
			into dbo.DeNormTrackingTBL
	FROM [dbo].[Tracking_Datasets] ds  
	inner join [dbo].[Tracking_DatasetDetail]  d on  d.DatasetID  = ds.DatasetID   
	inner join [dbo].[Tracking_Maps] m   
	on d.DetailID = m.DetailID 
	where ActiveMapBit=1  --Any files loaded with inactive Map will not generate Tat


/* Getting File Inventory between startdate and Enddate 
File Inventory stores the details of every files landed in our system along with datecreated and DateModified
*/

Drop table If exists dbo.tat_Inventory;
PRINT('Create dbo.tat_Inventory Table Initiated')
SELECT 
		InvType,
		Replace(FileFullPath,'Y:\','\\ccaintranet.com\dfs-dc-01\') as FileFullPath,
		Replace(FileFolderPath,'Y:\','\\ccaintranet.com\dfs-dc-01\') as FileFolderPath,
		SrcFileName,
		SrcFileExt,
		FileCreateDate,
		FileModifiedDate,
		FileSize,
		FileMasterID,
		CreatedDate,
		FileLandedDate=
			Case when FileCreateDate>FileModifiedDate and DATEDIFF(day,FileModifiedDate,FileCreateDate)<5
				 Then FileModifiedDate
			Else FileCreateDate
			End, -- This logic is for the file which is loaded from split but has no any previous Logs -- taking file modifed date
		AutoID
		INTO dbo.tat_Inventory
	FROM dbo.__FileInventory_CLEAN 
	WHERE 
		FileCreateDate between DATEADD(DAY, -5, @StartDate) and @EndDate;
PRINT('Create dbo.tat_Inventory Table Completed')

/* ALL the files loaded details from TrackerLoadLog between StartDate and EndDate */

Drop table If exists dbo.tat_TrackerLog;
PRINT('Create dbo.tat_TrackerLog Table Initiated');
SELECT
	REPLACE(Case when len(sfilename)>len(filename) then sfilename else filename end,'Y:\','\\ccaintranet.com\dfs-dc-01\') as FullFileName,
	FileSize,
	Min(Mapname) MapName,
	SFileName,
	MAX(RecordsWritten) as RecordsWritten, 
	MIN(LoadStartDate) as LoadStartDate,
	MIN(LoadEndDate ) as LoadEndDate
		INTO  dbo.tat_TrackerLog
	FROM 
		GiantEagleDPGroupLoadLogger.dbo.Trackerloadlog 
	WHERE LoadStartDate Between @StartDate and @EndDate
	and Case when len(sfilename)>len(filename) then sfilename else filename end not like '%sunset%'
	GROUP BY REPLACE(Case when len(sfilename)>len(filename) then sfilename else filename end,'Y:\','\\ccaintranet.com\dfs-dc-01\'),
	FileSize, SFileName;
PRINT('Create dbo.tat_TrackerLog Table Completed');

/* CLB_FileLog Details Greater than StartDate */
Drop table if exists dbo.tat_CLBFileLog;
PRINT('Create dbo.tat_CLBFileLog Table Initiated')
SELECT  DatasetID,
		FunctionName,
		InFileName,
		Replace(InFolder,'Y:\','\\ccaintranet.com\dfs-dc-01\')+'\'+InFileName as InFile, 
		Replace(OutFolder,'Y:\','\\ccaintranet.com\dfs-dc-01\')+'\'+OutFileName  as OutFile, 
		InFileDateCreated, 
		InFileSize 
		INTO dbo.tat_CLBFileLog 
	FROM dbo.CLB_FileLog 
	WHERE FunctionName <> 'Inventory-FTP' 
	AND DateStart >DATEADD(DAY, -5, @StartDate)
PRINT('Create dbo.tat_CLBFileLog Table Completed')

/* CLB_DJLog Details Greater than StartDate */
Drop table if exists dbo.tat_DJLog
	select Datasetid,Replace(DJLogFolder,'Y:\','\\ccaintranet.com\dfs-dc-01\')+'\'+LoadedFile FullFilePath
	into dbo.tat_DJLog
	From [dbo].[CLB_DJLog]
	where DateStart >DATEADD(DAY, -5, DATEADD(DAY, -10, (SELECT DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0))))
	and FunctionName<>'SSIS Package'

--File at Initial Process and Final Process
Drop table if exists dbo.tat_CLBLog;
PRINT('Create dbo.tat_CLBLog Table Initiated');

WITH
	fp AS 
	(
		SELECT 
		 DatasetID,FunctionName,InFileName,InFile,OutFile,InFileDateCreated, InFileSize ,  1 as Lvl 
		 FROM dbo.tat_CLBFileLog l
		UNION ALL 
		SELECT 
			l.DatasetID,l.FunctionName,fp.InFileName,l.InFile,l.OutFile,l.InFileDateCreated, l.InFileSize, Lvl+1 AS Lvl 
		FROM dbo.tat_CLBFileLog l 
			INNER JOIN fp 
		ON 
			l.InFile = fp.OutFile 
	)
	, 
	--Raw File Identifier Process
	ri AS (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY OUTFILE,datasetid,infile ORDER BY LVL DESC) AS rn 
		FROM fp
	),
	--Raw Identified
	rid AS (
		SELECT * 
		FROM ri 
		WHERE rn = 1
	)
	SELECT * 
	INTO dbo.tat_CLBLog 
	FROM rid;
PRINT('Create dbo.tat_CLBLog Table Completed')

--Get date for files with same name in raw and split with same size
Drop table if exists dbo.InvFileSize;
PRINT('Create dbo.InvFileSize Table Initiated')
select srcfilename,filesize,
	 min(case when InvType = 'RAW' then FileLandedDate END)  as RAWFileCreateDate,
	 min(case when InvType = 'SPLIT' then FileLandedDate END) as splitFileCreateDate,
	 min(case when InvType = 'FTP' then FileLandedDate END) AS FTPFileCreateDate,
	 count(1) as filesizecount
	 into dbo.InvFileSize
	from dbo.tat_Inventory
group by SrcFileName,filesize
HAVING  min(case when InvType = 'RAW' then filecreateDate END)  is not null and
min(case when InvType = 'SPLIT' then filecreateDate END)  is not null

/* updating the details which was missed by control-m */
update cfl set outfile=l.FullFileName
from dbo.tat_TrackerLog l 
inner join 
dbo.tat_CLBLog cfl on l.FullFileName like '%'+cfl.InFileName+'%'
WHERE OutFile IS NULL
  AND FunctionName IN (SELECT value FROM STRING_SPLIT(@functionName, ','));


Drop table if exists dbo.tat
Select	datediff(HOUR,coalesce(i.FileLandedDate, ir.FileLandedDate,ifs.RAWFileCreateDate,isp.FileLandedDate ) , LoadEndDate) as TAT,
		coalesce(dnt.schedule,dnLog.schedule,dntt.schedule) as Frequency,
		FullFilename,
		l.Mapname,
		infile,
		i.FileCreateDate as FileCreateDate,
		ir.FileCreateDate RAWFileCreateDate,
		isp.FileCreateDate splitFileCreateDate,
		ifs.FTPFileCreateDate FTPFileCreateDate, 
		LoadEndDate
		into dbo.tat
From dbo.tat_TrackerLog l 
Left JOIN 
	dbo.tat_CLBLog r
on 
	Replace(l.FullFileName,'_fixed.txt','') = r.OutFile
Left Join
	dbo.tat_Inventory i
on 
	r.InFile = i.FileFullPath
left join 
	dbo.tat_Inventory ir --data loaded from raw
on 
	l.FullFileName = ir.FileFullPath
and 
	ir.InvType = 'RAW'
left join 
	dbo.tat_Inventory isp -- data loaded from split and no logging in the table
on 
	l.FullFileName = isp.FileFullPath
and 
	isp.invtype = 'SPLIT'
left join 
	InvFileSize ifs
on 
	l.SFileName = ifs.SrcFileName
and 
	l.FileSize = ifs.FileSize
left join 
	(Select distinct datasetid,schedule from dbo.DeNormTrackingTBL) dnt
on dnt.DatasetID=r.DatasetID
left join 
	dbo.tat_DJLog  dlog
on 
	l.FullFileName=dlog.FullFilePath
left join 
	(Select distinct datasetid,schedule from dbo.DeNormTrackingTBL) dnLog
on dnLog.DatasetID=dlog.DatasetID
left join
	dbo.DeNormTrackingTBL dntt
on 
Case WHEN CHARINDEX('\', dntt.MapName) > 0 
Then 
Replace(Replace(Replace(RIGHT(dntt.MapName, CHARINDEX('\', REVERSE(dntt.MapName)) - 1),'.dtsx',''),'.tf.xml',''),'.Map.xml','')
Else 
Replace(Replace(Replace(dntt.MapName,'.dtsx',''),'.tf.xml',''),'.Map.xml','')
END
=
Case when CHARINDEX('\',l.MapName) > 0 
Then 
Replace(Replace(RIGHT(l.MapName, CHARINDEX('\', REVERSE(l.MapName)) - 1),'.dtsx',''),'.tf.xml','')
Else
Replace(Replace(Replace(l.MapName,'.dtsx',''),'.tf.xml',''),'.Map.xml','')
END
and
Case when CHARINDEX('\',l.FullFileName) > 0 
Then 
Replace(RIGHT(l.FullFileName, CHARINDEX('\', REVERSE(l.FullFileName)) - 1),'.'+dntt.FileExt,'') 
Else
l.FullFileName
END
like Replace(dntt.FilePatterns,'*','%')

END