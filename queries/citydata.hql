SELECT h.id,h.name,h.city,h.city_id,c.city_type, case c.city_type
    when 0 then 0
    else 1
    end
    as score
    FROM "ingestiondb"."hotels" h inner join "ingestiondb"."cities" c
    on h.city_id=c.id