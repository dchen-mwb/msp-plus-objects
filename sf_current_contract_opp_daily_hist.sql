
select * from (

Select  DISTINCT
  date(c.cds_history_insert_ts)             as CDS_insert_date,
  c.id                                      as contract_id,
  c.accountid                               as SF_ACCOUNT_ID,  
  c.status                                  as Status, 
  date(nullif(c.startdate,''))              as latest_contract_start_date, 
  date(nullif(c.enddate,''))                as latest_contract_end_date,
  date(nullif(c.date_of_cancellation,''))   as date_of_cancellation,
  c.createddate,
  c.currencyisocode,
  c.paymentmethod,
  c.paymentterms,
  c.name                                    as contract_name,
  a.parentid                                as parent_id,
  a.name                                    as msp_company_name,
  a.nebula_account_id                       as parent_nebula_account_id,
  u.name                                    as owner_name,
  coalesce(nullif(c.mspbillingtype,''),a.mspbillingtype)     as mspbillingtype1,
  a.geo,
  a.partner_consultant,
  a.partner_consultant_assigned_date,
  a.msp_type,
  a.dynamicusage_tier,
  a.partner_tier1                           as partner_tier,
  o2.orderid,
  dense_rank() OVER (partition by c.accountid,date(c.cds_insert_ts) order by right(c.name,8) desc) as contract_rank
        -- find latest contract within same enddate contracts
  
from 
  (select * from CDS_PROD.DM_SALES.sf_contract_cds_history where sub_status <> 'Converted to Evergreen') c 
    
left join (
    select 
      id, 
      accountid,
      name,
      status,
      STARTDATE,
      ENDDATE,
      cds_history_insert_ts,
      cds_insert_ts,
      MAX(CASE WHEN ENDDATE = '' or ENDDATE is null THEN '2100-01-01'
               WHEN accountid = '0013400001NM0xFAAT' THEN '2022-10-31' -- special case 
               ELSE ENDDATE END) OVER (PARTITION BY accountid,date(cds_history_insert_ts),status) as max_end_dt
    from 
      CDS_PROD.DM_SALES.sf_contract_cds_history 
      where sub_status <> 'Converted to Evergreen' 
            and nullif(startdate,'') <= date(cds_insert_ts) -- exclude early starting contracts from replacing existing
            and date(nullif(createddate,'')) <= date(cds_insert_ts) -- exclude backfill from missed billings
            ) mx_dt on c.id = mx_dt.id and date(mx_dt.cds_history_insert_ts) = date(c.cds_history_insert_ts)
    
  left join cds_prod.dm_sales.sf_account_cds_history a on c.accountid = a.id and date(a.cds_history_insert_ts) = date(c.cds_history_insert_ts)
    
  left join cds_prod.dm_sales.sf_user_cds_history u on a.ownerid = u.id and date(a.cds_history_insert_ts) = date(u.cds_history_insert_ts)
    
  left join (
    select * 
        from (
            select 
                o.accountid
                ,try_to_date(o.activateddate) as activated_date
                -- ,try_to_date(o.effectivedate) as effective_date
                ,o.status
                ,o.contract_id
                ,o.id as orderid
                ,dense_rank() over (partition by o.accountid,o.contract_id order by activated_date asc) as order_rank

            from cds_prod.dm_sales.sf_order_cds o

            left join cds_prod.dm_sales.sf_account_cds a on o.accountid = a.id
                where 
                -- o.contract_id = '8002H000002J72EQAS' and 
                o.status = 'Activated'
                and o.type = 'New'
                and a.partner_type = 'MSP'
                and a.test_account = false

            )
            where order_rank = 1
  ) o2 on o2.contract_id = c.id
    
where 
  ((c.enddate = mx_dt.max_end_dt or nullif(c.enddate,'') is null) and date(nullif(c.createddate,'')) <= date(c.cds_insert_ts)) -- exclude backfill from missed billings
  and c.test_account = 'false' 
  and a.partner_type = 'MSP'
  and c.sub_status <> 'Converted to Evergreen'
  and (c.status = 'Activated' or (c.status in ('Cancelled','Expired') and c.cds_insert_ts <= dateadd(month, 2, date(coalesce(nullif(c.date_of_cancellation,''),nullif(c.enddate,'')))))) --keep cancelled/expired contracts up to a month then exclude
    
order by 
  1 DESC, 8 DESC
  
)

where contract_rank = 1
--and (cds_insert_date = last_day(cds_insert_date) or last_day(cds_insert_date) = last_day(current_date()))

UNION ALL

select 
    date(o2.cds_history_insert_ts)                      as CDS_insert_date
    ,null                                               as contract_id
    ,a.id                                               as sf_account_id
    ,null                                               as status
    ,try_to_date(o2.effectivedate)                      as latest_contract_start_date
    ,date(null)                                         as latest_contract_end_date
    ,date(null)                                         as date_of_cancellation
    ,null                                               as createddate
    ,o2.currencyisocode
    ,o2.payment_method                                  as paymentmethod
    ,o2.sbqq__paymentterm                               as paymentterms
    ,null                                               as contract_name
    ,a.parentid
    ,a.name                                             as msp_company_name
    ,a.nebula_account_id                                as parent_nebula_account_id
    ,u.name                                             as owner_name
    ,coalesce(nullif(o2.msp_billing_type,''),a.mspbillingtype)     as mspbillingtype1
    ,a.geo
    ,a.partner_consultant
    ,a.partner_consultant_assigned_date
    ,a.msp_type
    ,a.dynamicusage_tier
    ,a.partner_tier1                                     as partner_tier
    ,o2.id                                               as orderid
    ,1                                                  as contract_rank

    from cds_prod.dm_sales.sf_order_cds_history o2
    
    left join cds_prod.dm_sales.sf_opportunity_cds_history o on o.id = o2.opportunityid and date(o.cds_history_insert_ts) = date(o2.cds_history_insert_ts)
    
    left join cds_prod.dm_sales.sf_account_cds_history a on o2.accountid = a.id and date(o2.cds_history_insert_ts) = date(a.cds_history_insert_ts)
    
    left join cds_prod.dm_sales.sf_user_cds_history u on a.ownerid = u.id and date(a.cds_history_insert_ts) = date(u.cds_history_insert_ts)
    
where 
    o.type = 'New Business'
    and o.stagename in ('Closed Won', 'Closed Won-Finance')
    and o2.status = 'Activated'
    and a.partner_type = 'MSP'
    and try_to_date(o2.effectivedate) > try_to_date(o.closedate)
    and try_to_date(o2.effectivedate) > '2021-01-31'
    and cds_insert_date < try_to_date(o2.effectivedate)
    --and (cds_insert_date = last_day(cds_insert_date) or last_day(cds_insert_date) = last_day(current_date()))
    
    ;
   
