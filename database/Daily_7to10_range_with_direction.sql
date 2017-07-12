
select T.Month, T.Date, sum(T.range) as range from
	(select month, date, hour, max(high) as high, min(low) as low , (max(high) - min(low)) * (sum(case minute when 0 then Open else 0 end) - sum(case minute when 59 then Close else 0 end)) / abs(sum(case minute when 0 then Open else 0 end) - sum(case minute when 59 then Close else 0 end)) * 10000 as range
	from M1_Data
	group by month, date, hour) T
where T.hour between 7 and 10
group by T.Month, T.Date
