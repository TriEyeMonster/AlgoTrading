select T.hour, avg(T.range) as avg_range from 
	(select month, date, hour, (max(high) - min(low)) * 10000 as range from M1_Data group by month, date, hour) as T
group by T.hour
order by avg_range DESC