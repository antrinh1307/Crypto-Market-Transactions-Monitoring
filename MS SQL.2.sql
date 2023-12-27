with min_diff as (

select 	*,
		datediff(minute, lag(dt) over(partition by sender order by sender, dt), dt) as minute_diff
from krypto
),

	rownumber_latter as (
select rownumber
from min_diff
where minute_diff <= 60
	),
    
	rownumber_table as (
select rownumber
from krypto
where rownumber in (
  select rownumber
  from rownumber_latter
  UNION
  select rownumber - 1
  from rownumber_latter
  )
      )

SELECT 	sender,
		min(dt) as sequence_start,
        max(dt) as sequence_end,
        count(rownumber) as transactions_count,
        sum(amount) as transactions_amount
from krypto
where rownumber IN (
  select *
  from rownumber_table
  )
group BY sender
HAVING sum(amount) >= 150
order BY sender, min(dt), max(dt)