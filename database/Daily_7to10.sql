select month, date,  sum(Open) as Open, (max(high) - sum(Open)) * 10000 as high, (min(low) -  sum(Open)) * 10000 as low, (max(high) - min(low)) * 10000 as range from 
		( select month, date, hour, case hour when 1 then Open else 0 end as open, high, low, range from Hourly_Table ) T
where hour between 1 and 23
group by month, T.date having (sum(Open) > 0 and (max(high) - sum(Open)) * 10000 > 35 and (min(low) -  sum(Open)) * 10000 > -25) or ((max(high) - sum(Open)) * 10000 < 25 and (min(low) -  sum(Open)) * 10000 < -35)
order by month, date