select id, xx.level, case WHEN xx.score1 is null then 0 else xx.score1 end as recon
from ingestiondb.hotels hh 
left join aggregatedb.hotels_summary h on h.hotel_id=hh.id
left join
(SELECT
  hotel_id, level,
  case WHEN level >=4 then 1
  ELSE 0 end as score1
  from
(SELECT
  hotel_id,
  case WHEN score <= 0 THEN 0
  when score>0 and score<=2 then 1
  when score>2 and score<=6 then 2
  when score>6 and score<10 then 3
  ELSE 4 end  as level 
  from
(SELECT
  hotel_id,
    case when
      (CASE WHEN curr_gmv > 0 THEN least(-4 * curr_recovery / curr_gmv, 1) ELSE 1 END)  = 1
      and (-1 * curr_recovery > 10000) then 10
when least(curr_age / 3, 1) is null then 10
when least(-1 * curr_recovery / 50000, 1) is null then 10
  else ((CASE WHEN curr_gmv > 0
    THEN least(-4 * curr_recovery / curr_gmv, 1)
    ELSE 1 END) * least(-1 * curr_recovery / 50000, 1) * least(curr_age / 3, 1)) * 10 end AS score
from coinguard_service.hotel_financials WHERE month=extract(month from current_date) and year=extract(year from current_date)))) xx
  on xx.hotel_id = hh.id 
  where  h.oyo_product in ('Collection O','SMART','CapitalO','OYO X','Townhouse','Flagship','Silverkey') 
and h.country_name in ('India') 
and h.status_id in (1,2)
  order by hh.id asc
