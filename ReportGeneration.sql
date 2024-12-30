
With OrderedTbl as(
Select Frequency,
		tat,
		ROW_NUMBER()over(partition by Frequency order by tat) as RowNum,
		TotalCount=count(*)over(partition by Frequency),
		Rem=count(*)over(partition by Frequency)%2
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
    Frequency,
	count(*) as NoOfFiles,
	Cast(avg(tat)/24.00 as decimal(18,4)) as [AvgTAT(Mean)],
    CASE 
        WHEN Rem = 1 THEN Cast(MAX(CASE WHEN RowNum = Medianfirst THEN tat ELSE NULL END)/24.00 as decimal(18,4))
        ELSE Cast(AVG(CASE WHEN RowNum IN (Medianfirst, Mediansecond) THEN tat ELSE NULL END)/24.00 as decimal(18,4))
    END AS TATMedian
FROM cteall
GROUP BY Frequency, Rem
order by case when Frequency='Daily' then 1
				when Frequency='Weekly' then 2
				when Frequency='BiWeekly' then   3
				else 4 END