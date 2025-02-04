
With OrderedTbl as(
Select t.Frequency,
		tat,
		ROW_NUMBER()over(partition by t.Frequency order by tat) as RowNum,
		TotalCount=count(*)over(partition by t.Frequency),
		Rem=count(*)over(partition by t.Frequency)%2,
		case when tat/24<=isnull(od.Days,5) then 1 else 0 end As OlaMet
from dbo.tat t
left join dbo.___OLADetails od
on t.Frequency=od.frequency
where TAT is not null
and tat<> 0
)
,cteall as(
select *, sum(OlaMet) over(partition by OlaMet,frequency) as OlaMetPer, 
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
    Case when Isnull(Frequency,'')='' then 'Adhoc' Else Frequency end as Frequency,
	count(*) as NoOfFiles,
	Cast(avg(tat)/24.00 as decimal(18,4)) as [AvgTAT(Mean)],
    CASE 
        WHEN Rem = 1 THEN Cast(MAX(CASE WHEN RowNum = Medianfirst THEN tat ELSE NULL END)/24.00 as decimal(18,4))
        ELSE Cast(AVG(CASE WHEN RowNum IN (Medianfirst, Mediansecond) THEN tat ELSE NULL END)/24.00 as decimal(18,4))
    END AS TATMedian,
	Round(max(olametper)/Cast(count(*) as float),4)*100 as OLAMetPer,
	round((count(*)-max(olametper))/Cast(count(*) as float),4)*100 as OLANotMetPer
FROM cteall
GROUP BY Frequency, Rem
