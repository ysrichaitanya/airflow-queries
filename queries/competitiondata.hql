SELECT h.id,h.name,tb.tag_name, case
    when tb.taggable_id is null then 0
    else 1
    end as score
    FROM "ingestiondb"."hotels" h left outer join "ingestiondb"."taggings_base" tb
    on h.id=tb.taggable_id and tb.tag_name='strategic_acquired' and tb.taggable_type='Hotel'
    order by id