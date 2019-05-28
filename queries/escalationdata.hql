select hh.id, CASE WHEN xx.ex is null then 0 else xx.ex end as escalations
from ingestiondb.hotels hh 
left join
(select a1.hotel_id,  coalesce(a1.escalations,0) as ww,coalesce(a2.checkins,10000) as tt,
case when
coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) >=0.0000 and  coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) <= 2.5 then 0
when
coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) > 2.5 and  coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) <= 5 then 1
when
coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) >5 and  coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) <= 10 then 2
when
coalesce(a1.escalations,0)*100.000/coalesce(a2.checkins,100) >10 then 50 end as ex

from (select json_extract_scalar(t.meta_data,'$.ticket_oyo_id') as OYO_id,b.hotel_id, COUNT(t.freshdesk_ticket_id) as escalations
from ingestiondb.tickets_base t
inner join ingestiondb.bookings_base b on t.resource_id = b.id
left join aggregatedb.hotels_summary  hs on json_extract_scalar(t.meta_data,'$.ticket_oyo_id')=hs.oyo_id
where json_extract_scalar(t.meta_data,'$.freshdesk_group_id') ='6000200468'
and date(cast(substr(t.created_at,1,19) as timestamp)) between date(current_date - interval '30' day) and date(current_date - interval '1'day)
group by json_extract_scalar(t.meta_data,'$.ticket_oyo_id'),b.hotel_id, hs.oyo_product) a1
left join (SELECT h.oyo_id,
-- h2.city_name,h3.hub_name,
count(b.invoice_no) as checkins
FROM ingestiondb.bookings_base b
INNER JOIN aggregatedb.hotels_summary h  on b.hotel_id = h.hotel_id
WHERE b.status in (1,2)
and h.country_id = 1
and date(b.checkin) between date(current_date - interval '30' day) and date(current_date - interval '1'day)
GROUP BY h.oyo_id) a2 on a1.oyo_id=a2.oyo_id) xx
on xx.hotel_id = hh.id order by hh.id asc
