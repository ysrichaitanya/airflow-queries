select hh.id, CASE WHEN xx.result is null then 0 else xx.result end as overallgx
from ingestiondb.hotels hh 
left join
(select temp.*,case when temp.res < -30 then -30  when temp.res>20 then 20 else temp.res end as result from (
select t.*,((t.unhappy/(t.total_feedbacks*1.0000))*100) as UH_perc,
 LEAST( t.total_feedbacks/3 ,20 ) as B ,
(1- (ROUND ( ((GREATEST((((t.unhappy/(t.total_feedbacks*1.0000))*100) - 4),0))/4), 2) )) as A,
 LEAST( t.total_feedbacks/3 ,20 )*(1- (ROUND ( ((GREATEST((((t.unhappy/(t.total_feedbacks*1.0000))*100) - 4),0))/4), 2) )) as res
from (
select h.hotel_id,
sum (case when suggest_oyo in (1,2) then 1 else 0 end ) as unhappy,
sum (case when suggest_oyo in (5) then 1 else 0 end ) as delight,
sum (case when suggest_oyo in (1,2,3,4,5) then 1 else 0 end ) as total_feedbacks
from ingestiondb.feedbacks_base f
join ingestiondb.bookings_base b on f.booking_id=b.id
join ingestiondb.user_profiles_base up on up.id = b.guest_id
join aggregatedb.hotels_summary h on b.hotel_id = h.hotel_id
where date(b.checkout) between date(current_date-interval '30' day) and date(current_date-interval '1' day)
AND b.status in (2) and b.source not in (4)
and f.source not in (1) and f.suggest_oyo >=1
group by h.hotel_id
order by h.hotel_id
) as t
) as temp) xx
  on xx.hotel_id = hh.id order by hh.id asc
