select id, question, answer, score,
    case
    when xx.score is null then 1
    else xx.score end as score1
    from ingestiondb.hotels hh
    left join aggregatedb.hotels_summary h on h.hotel_id=hh.id
    left join
    (select temp1.*,case temp1.answer when'Yes' then 1
    else 0 end as score from
    (
    select entityid as hotel_id,case when questionid like '%Room%' then element_at(split (questionid,'_'),-3) else element_at(split (questionid,'_'),-2) end as room,
    element_at(split (questionid,'_'),-1) as question, answer
    from (select from_unixtime(cast((cast(json_parse(createdon) as row("$date" varchar))."$date") as bigint)/1000) as created_at,
    entityid, taskconfigid, x.questionId,x.answer,x.mandatory
    from task_service.audittask_base
    cross join unnest(CAST(json_parse( qlist ) as ARRAY(ROW(questionId VARCHAR, answer VARCHAR,mandatory BOOLEAN)))) as x(questionId,answer,mandatory)
    where taskconfigid = '5c89eff9d37be31c04e22bdd'--and questionnaireid = 'AWmbZ8NOxaW94AmTDmK4'
    and qlist != '[e,m,p,t,y]')
    where answer is not null
    ) as temp1
      where temp1.question = 'Iron and Iron Board') xx
  on xx.hotel_id = cast(hh.id as VARCHAR(10000)) 
  where  h.oyo_product in ('Collection O','SMART','CapitalO','OYO X','Townhouse','Flagship','Silverkey') 
and h.country_name in ('India') 
and h.status_id in (1,2)
  order by id asc
