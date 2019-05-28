select ll.id, COALESCE(ll.room_cid,0)  + COALESCE(ll.shifting_cid,0) as res,
case when COALESCE(ll.room_cid,0)  + COALESCE(ll.shifting_cid,0) >= 4 then 1
else 0 end as result
from (
(select
h.hotel_id as id, count(b.invoice_no) as shifting_cid
from cstshifting_service.shifting as s
inner join ingestiondb.bookings_base as b on s.booking_id = b.id
left join aggregatedb.hotels_summary as h on s.current_hotel_id = h.hotel_id
left join aggregatedb.hotels_summary as h1 on s.new_hotel_id = h1.hotel_id
left join ingestiondb.crs_enums e1 on s.reason = e1.enum_key and e1.table_name = 'shiftings' and e1.column_name = 'reason'
where date(b.checkin) between date(current_date-interval '15' day) and date(current_date-interval '1' day)
and s.reason not  in (2,9,10,15)
and h.country_id = 1
and s.status = 0 and b.source not in (13,14)
and s.checkin_denied = true and s.reason in (1,7,12,5,13,6,8,0,18)
group by 1) xx
left join
(
select
h.hotel_id,count(b.invoice_no) as room_cid
from ingestiondb.bookings_base as b
left join aggregatedb.hotels_summary as h on b.hotel_id = h.hotel_id
left join ingestiondb.crs_enums e1 on b.no_show_reason = e1.enum_key and e1.table_name = 'bookings' and e1.column_name = 'no_show_reason'
left join ingestiondb.crs_enums e2 on b.cancellation_reason = e2.enum_key and e2.table_name = 'bookings' and e2.column_name = 'cancellation_reason'
left join ingestiondb.crs_enums e3 on b.status = e3.enum_key and e3.table_name = 'bookings' and e3.column_name = 'status'
where  date(b.checkin) between date(current_date-interval '15' day) and date(current_date-interval '1' day) and b.status in (4,3)
and (b.no_show_reason in (6,16,19,21) or  b.cancellation_reason in (22,4,25,26,27,28,29,30,31,32,33,34))
and b.id not in (select booking_id from cstshifting_service.shifting as s inner join ingestiondb.bookings_base as b on s.booking_id = b.id
where date(b.checkin) between date(current_date-interval '15' day) and date(current_date-interval '1' day) )
and h.country_id = 1
group by 1
) tt on tt.hotel_id=xx.id  ) ll order by ll.id asc
