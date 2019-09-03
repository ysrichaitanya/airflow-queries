select hh.id, CASE WHEN xx.ex is null then 0 else xx.ex end as escalations
from ingestiondb.hotels hh
left join aggregatedb.hotels_summary h on h.hotel_id=hh.id
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

from (select json_extract_scalar(t.meta_data,'$.ticket_oyo_id') as oyo_id, h.hotel_id,count(freshdesk_ticket_id) as escalations
                           from (select * from cst_ticketdb_service.tickets where 
                           cast(substr(cast((from_unixtime(created_at / 1000000)+interval '5' hour + interval '30' minute) as varchar),1,10) as date) between  date_add('day', -7,current_date) and current_date) t
                           left join aggregatedb.hotels_summary h on json_extract_scalar(t.meta_data,'$.ticket_oyo_id')=h.oyo_id
                           left join ingestiondb.clusters cl on h.cluster_id=cl.id
                           left join ingestiondb.cities c on cl.city_id=c.id
                           left join ingestiondb.hubs hb on c.hub_id=hb.id 
                           left join ingestiondb.hotel_user_profiles hup on hup.hotel_id = h.hotel_id and hup.role = 2
                           left join ingestiondb.user_profiles_base up on up.id = hup.USER_PROFILE_ID
                           left join (select json_extract_scalar(meta_data,'$.ticket_oyo_id') as oyo_id1, count(freshdesk_ticket_id) as count_tickets from cst_ticketdb_service.tickets 
                                     where cast(substr(cast((from_unixtime(created_at / 1000000)+interval '5' hour + interval '30' minute) as varchar),1,10) as date) between date_trunc('month', current_date) and current_date 
                                     group by json_extract_scalar(meta_data,'$.ticket_oyo_id')) alpha on alpha.oyo_id1=h.oyo_id
                           where 
                           json_extract_scalar(t.meta_data,'$.freshdesk_group_id')='6000200468'
                           and h.country_id in (1)
                           and split_part(t.category,'-',1) in ('Hotel Staff Issue','Security and Emergencies','Rooms','Rooms Compatibility','Shifting','Food Issues')  
                          group by json_extract_scalar(t.meta_data,'$.ticket_oyo_id'), h.hotel_id) a1
left join (SELECT h.oyo_id,
-- h2.city_name,h3.hub_name,
count(b.invoice_no) as checkins
FROM ingestiondb.bookings_base b
INNER JOIN aggregatedb.hotels_summary h  on b.hotel_id = h.hotel_id
WHERE b.status in (1,2)
and h.country_id = 1
and date(b.checkin) between date(current_date - interval '30' day) and date(current_date - interval '1'day)
GROUP BY h.oyo_id) a2 on a1.oyo_id=a2.oyo_id) xx
on xx.hotel_id = hh.id 
where  h.oyo_product in ('Collection O','SMART','CapitalO','OYO X','Townhouse','Flagship','Silverkey') 
and h.country_name in ('India') 
and h.status_id in (1,2)
order by hh.id asc
