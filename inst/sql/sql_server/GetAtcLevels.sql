with classes as (

    select
        cd.concept_id,
        ca.ancestor_concept_id as atc_concept_id
    from @vocabulary_database_schema.concept cd
    inner join @vocabulary_database_schema.concept_ancestor ca
        on cd.concept_id = ca.descendant_concept_id
    inner join @vocabulary_database_schema.concept c
        on ca.ancestor_concept_id = c.concept_id and c.vocabulary_id = 'ATC'

     WHERE cd.vocabulary_id = 'RxNorm'
),

classmates as (

    select
        c1.concept_id as concept_id_1,
        c2.concept_id as concept_id_2,
        c1.atc_concept_id,
        case
            when c.concept_class_id = 'ATC 1st' then 1
            when c.concept_class_id = 'ATC 2nd' then 2
            when c.concept_class_id = 'ATC 3rd' then 3
            when c.concept_class_id = 'ATC 4th' then 4
            when c.concept_class_id = 'ATC 5th' then 5
            else null
        end as atc_level
    from classes c1
    inner join classes c2
        on c1.atc_concept_id = c2.atc_concept_id and
           c1.concept_id <> c2.concept_id
    left join @vocabulary_database_schema.concept as c
        on c1.atc_concept_id = c.concept_id
)

select
    concept_id_1 as drug_concept_id_1,
    concept_id_2 as drug_concept_id_2,
    max(atc_level) as level_closest_atc_relation,
    min(atc_level) as level_furthest_atc_relation,
    case
        when sum(case when atc_level = 1 then 1 else 0 end) > 0 then 1
        else 0
    end as atc_1_related,
    case
        when sum(case when atc_level = 2 then 1 else 0 end) > 0 then 1
        else 0
    end as atc_2_related,
    case
        when sum(case when atc_level = 3 then 1 else 0 end) > 0 then 1
        else 0
    end as atc_3_related,
    case
        when sum(case when atc_level = 4 then 1 else 0 end) > 0 then 1
        else 0
    end as atc_4_related,
    case
        when sum(case when atc_level = 5 then 1 else 0 end) > 0 then 1
        else 0
    end as atc_5_related
from classmates
group by concept_id_1, concept_id_2
