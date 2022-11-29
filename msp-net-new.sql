with all_msp_contracts as (

    select 
            c.accountid as sf_account_id
            ,a.parentid
            ,a.partner_type
            ,o.id as opportunity_id
            ,u.name as account_owner_name
            ,u1.name as opportunity_owner_name
            ,c.id as contract_id
            ,right(c.name,8) as contract_name
            ,c.status
            ,c.sub_status
            ,c.mspbillingtype
            ,date(case when o.name like '%Evergreen Migration%' then coalesce(nullif(c.startdate,''),nullif(o.closedate,''))
                  when o.stagename in ('Closed Won','Closed Won-Finance') then coalesce(nullif(o.closedate,''),nullif(c.startdate,'')) 
                else nullif(c.startdate,'') end) as contract_startdate
            ,coalesce(nullif(c.date_of_cancellation,''),nullif(c.enddate,'')) as contract_enddate
            ,lag(c.sub_status,1) over (partition by sf_account_id order by contract_name asc) as previous_substatus
        from cds_prod.dm_sales.sf_contract_cds c
        left join cds_prod.dm_sales.sf_opportunity_cds o on c.sbqq__opportunity = o.id
        left join cds_prod.dm_sales.sf_account_cds a on a.id = c.accountid
        left join cds_prod.dm_sales.sf_user_cds u on a.ownerid = u.id
        left join cds_prod.dm_sales.sf_user_cds u1 on o.ownerid = u1.id

        where
            /*sf_account_id = '0012H00001Z3NKUQA3'
            and */(a.test_account = false or a.test_account is null)
            and (lower(o.po) not like '%usage%' or lower(o.po) is null)
            and (contract_enddate >= '2020-07-01' or nullif(contract_enddate,'') is null)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13

)
,

dynamicusage_rank as (

    select *
    from (
        select 
        date(cds_history_insert_ts)
        ,id
        ,msp_type
        ,dynamicusage_tier
        ,dense_rank() over (partition by id order by date(cds_history_insert_ts) asc) as rank 
        from cds_prod.dm_sales.sf_account_cds_history 
        where nullif(dynamicusage_tier,'') is not null and nullif(msp_type,'') is not null
        )
    where rank = 1
    
)
,

all_msp_contracts_details as (

    select 
        main.*
        ,coalesce(d.msp_type,a.msp_type) as msp_type_new
        ,case when msp_type_new ilike '%child%' then coalesce(nullif(d.dynamicusage_tier,''),'Registered') 
              else a.partner_tier1 end 
                        as partner_tier
        ,a.geo
        ,lead(partner_tier,1) over (partition by sf_account_id order by contract_startdate asc) as next_partner_tier
        ,lead(a.geo,1) over (partition by sf_account_id order by contract_startdate asc) as next_geo
        ,lag(contract_startdate,1) over (partition by sf_account_id order by contract_name asc) as previous_startdate
        ,case when previous_substatus = 'Converted to Evergreen' then lag(contract_enddate,2) over (partition by sf_account_id order by contract_name asc)
        else lag(contract_enddate,1) over (partition by sf_account_id order by contract_name asc) end as previous_enddate
        ,dense_rank() over (partition by sf_account_id order by contract_name asc) as contract_rank

    from all_msp_contracts main
    
    left join cds_prod.dm_sales.sf_account_cds_history a on a.id = main.sf_account_id and date(a.cds_insert_ts) = main.contract_startdate
    
    left join dynamicusage_rank d on main.sf_account_id = d.id
    
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    
)

,

all_msp_contracts_net_new as (

    select 
        main.sf_account_id
        ,main.parentid
        ,main.opportunity_id
        ,main.contract_id
        ,case when main.contract_rank > 1 and main.previous_substatus = 'Converted to Evergreen' then coalesce(main.previous_startdate,main.contract_startdate) 
            when main.sf_account_id = '0012H00001fo2OIQAY' then '2022-06-20' else main.contract_startdate end as effective_startdate
        ,main.contract_enddate
        ,main.mspbillingtype as contract_billingtype
        ,main.status as contract_status
        ,main.sub_status as contract_substatus
        ,main.account_owner_name
        ,main.opportunity_owner_name
        ,coalesce(main.partner_tier,main.next_partner_tier) as partner_tier
        ,coalesce(main.geo,main.next_geo) as geo
        ,case 
            when contract_status = 'Cancelled' and last_day(date(effective_startdate)) = last_day(date(contract_enddate)) then false
            when contract_substatus in ('Moved to Direct to 2-Tier','Moved to 2-Tier to Direct','2-Tier Parent Switch') then false
            when main.contract_rank = 1 and contract_substatus not in ('Migration') then true
            when datediff(day,main.previous_enddate,main.contract_startdate) >= 90 and contract_substatus not in ('Moved to Direct to 2-Tier','Moved to 2-Tier to Direct','2-Tier Parent Switch') then true
            when main.contract_rank = 2 and main.previous_substatus = 'Converted to Evergreen' and contract_substatus not in ('Moved to Direct to 2-Tier','Moved to 2-Tier to Direct','2-Tier Parent Switch') then true
            when main.contract_id = '8002H000002Za9UQAS' and contract_substatus not in ('Migration') then true --one off data hygiene issue
            else false end 
            as net_new_qualify
        ,main.msp_type_new
    
    from all_msp_contracts_details main
    
    where main.mspbillingtype in ('Usage','Upfront') and main.partner_type = 'MSP'
   
)

select
    a.*
    ,b.name as parent_company_name
    ,coalesce((case when a.msp_type_new in ('Strategic Master','Strategic Child') then 'Strategic'
                    when a.msp_type_new in ('Direct','Non-Strategic Master','Non-Strategic Child') then 'Direct'
                    else null end),
              (case when a.parentid in ('0012H00001fov5jQAA','0018000000n2YAIAA2','0012H00001fq3IaQAI','0013400001Nj6crAAB','0012H00001fqP9aQAE','0012H00001ayK6SQAU','0012H00001h5xhKQAQ') then 'Strategic' else 'Direct' end)) 
                as msp_account_type
    
from all_msp_contracts_net_new a

left join cds_prod.dm_sales.sf_account_cds b on a.parentid = b.id

;

