select Time from M1_Data
where Date=20160104 and 
High=
(
	select high from 
		( select Currency, Open, max(High) as high, min(Low), Close from M1_Data where Date=20160103 ) as summary
)
Union
select Time from M1_Data
where Date=20160103 and 
Low=
(
	select low from 
		( select Currency, Open, max(High), min(Low) as low, Close from M1_Data where Date=20160103 ) as summary
)