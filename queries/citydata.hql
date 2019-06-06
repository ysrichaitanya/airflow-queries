SELECT h.id,h.name,h.city,h.city_id,c.city_type, case
when c.city_type is null then 0
when c.city_type=0 then 0
else 1
end
as score
FROM "ingestiondb"."hotels" h left join "ingestiondb"."cities" c
on h.city_id=c.id
order by h.id asc
