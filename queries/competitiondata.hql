SELECT h.id,h.name,tb.tag_name, case
    when tb.taggable_id is null then 0
    else 1
    end as score
    FROM "ingestiondb"."hotels" h left join "ingestiondb"."taggings_base" tb
    on h.id=tb.taggable_id and tb.tag_name='strategic_acquired' and tb.taggable_type='Hotel'
    left join aggregatedb.hotels_summary d on d.hotel_id=h.id
    where  d.oyo_product in ('Collection O','SMART','CapitalO','OYO X','Townhouse','Flagship','Silverkey') 
and d.country_name in ('India') 
and d.status_id in (1,2)
    order by id
