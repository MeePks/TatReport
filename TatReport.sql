
---FIndings in the error

--1. Inventory not being able to track some of the files due to exlude pattern in the powershel script of inventorying
--2. Fixed file not being able to be traced due to not logging in tables. SOme function are not producing oufile while should be have been same as infile
/*
select *from
dbo.CLB_FileLog r
where r.infilename is not null
and r.outfilename is null
and FunctionName<>'Inventory-FTP'
*/


--3. difference in file naming pattern( some has Y:\ and some has \\ccaintranet.com)
--4. Files arriving at the month end and getting loaded in first of Next Month only

select distinct functionname --select *
from
dbo.CLB_FileLog r
where r.infilename is not null
and r.outfilename is null
and FunctionName<>'Inventory-FTP'

select * 
from dbo.tat


Declare @StartDate Date = (SELECT DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)),
		@EndDate Date = (SELECT EOMONTH(GETDATE(), -1))
Select	Count(distinct FullFileName) TotalDistinctFileLoaded,
		Count(case when (Frequency is null or Frequency='') then fullfilename END) FrequencyUnknown,--need to update in trackingTables
		count(Case when datediff(day,coalesce(FileCreateDate,FileCreateDate,RAWFileCreateDate,splitFileCreateDate ) , LoadEndDate) is null then FullFileName end) TATUnknown,
		Count(case when infile is null then fullfilename end) [ManualLoad/Noctrlmlogs]
from dbo.tat

Go
With OrderedTbl as(
Select frequency,
		tat,
		ROW_NUMBER()over(partition by frequency order by tat) as RowNum,
		TotalCount=count(*)over(partition by frequency),
		Rem=count(*)over(partition by frequency)%2
from dbo.tat
where TAT is not null
and tat<> 0
)
,cteall as(
select *,
Medianfirst=
		Case when Rem=1
		Then (TotalCount+1)/2
		Else (TotalCount/2) END,
Mediansecond=Case when Rem=1
		Then 0
		Else (TotalCount/2)+1 END
from OrderedTbl
)
SELECT 
    frequency,
	Cast(avg(tat)/24.00 as decimal(18,4)) as [AvgTAT(Mean)],
    CASE 
        WHEN Rem = 1 THEN Cast(MAX(CASE WHEN RowNum = Medianfirst THEN tat ELSE NULL END)/24.00 as decimal(18,4))
        ELSE Cast(AVG(CASE WHEN RowNum IN (Medianfirst, Mediansecond) THEN tat ELSE NULL END)/24.00 as decimal(18,4))
    END AS TATMedian
FROM cteall
GROUP BY frequency, Rem
order by case when Frequency='Daily' then 1
				when Frequency='Weekly' then 2
				when Frequency='BiWeekly' then   3
				else 4 END


 







