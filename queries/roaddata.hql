select id, name, hotel_id, question, answer, score ,
    case
    when xx.score is null then 0
    else xx.score end as score1
    from ingestiondb.hotels hh
    left join
    (select temp1.*,case temp1.answer when'less than 20 ft' then 0
    when '20-40 ft' then 0.5
    else 1 end as score from
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
      where temp1.question = 'Road width (in front of property)' ) xx
      on xx.hotel_id = cast(hh.id as varchar(10000)) order by id asc
