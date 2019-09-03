select hh.id from ingestiondb.hotels hh
left join aggregatedb.hotels_summary h on h.hotel_id=hh.id
where  h.oyo_product in ('Collection O','SMART','CapitalO','OYO X','Townhouse','Flagship','Silverkey') 
and h.country_name in ('India') 
and h.status_id in (1,2)
order by hh.id
