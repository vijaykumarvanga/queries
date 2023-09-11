with weightage_tab as 
(
select
    distinct context as context_w, priority, try(cast(bike_ctxt_wtge as real)) as bike_ctxt_wtge, try(cast(auto_ctxt_wtge as real)) as auto_ctxt_wtge,
    try(cast(fc_wtge as int)) as fc_wtge, try(cast(mf_wtge as int)) as mf_wtge, try(cast(esc_wtge as int)) as esc_wtge,
    try(cast(female_wtge as int)) as female_wtge, try(cast(male_blank_wtge as int)) as male_blank_wtge 
from    
    experiments.hsc_risk_identifiers_new
where
    length(priority) >= 1 
)

,rfrnc_tab as 
(
select
    distinct feedback_identfier as chip, context
from    
    experiments.hsc_risk_identifiers_new
where 
    channel = 'feedback_chip'
)

,orders_raw as 
(
select
    *, cast(dense_rank() over(order by week_fin) as double)/2 as week_wtge 
from 
    (
    select 
        date_format(date_trunc('week',date_parse(yyyymmdd,'%Y%m%d')), '%Y-%m-%d') as week_fin,
        yyyymmdd, service_obj_service_name as service, cast(json_parse(customer_feedback_rate_service) as array<varchar>) as feedback_chips,
        service_obj_city_display_name as city, captain_id, order_id, unique_id, service_detail_id,
        (case when customer_obj_gender = '1' then 'female' else 'male_blank' end) as customer_gender
    from 
        orders.order_logs_snapshot
    where 
        yyyymmdd >= date_format(date('2023-01-23'),'%Y%m%d')
        and yyyymmdd <= date_format(date('2023-07-23'),'%Y%m%d')
        and ((service_obj_service_name = 'Link' and service_obj_city_display_name IN ('Bangalore','Hyderabad','Chennai','Delhi','Jaipur','Kolkata')) 
        or (service_obj_service_name = 'Auto' and service_obj_city_display_name IN ('Bangalore','Hyderabad','Chennai','Delhi','Jaipur','Mumbai')))
        and order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    )
)

,fc_wtge_tab as 
(
select
    service, city, captain_id, 
    sum(negative_count) as negative_count, 
    
    sum(case when context = 'safe_behaviour_no' then p0_negative_count end) as unsafe_behave_count,
    sum(case when context = 'safe_ride_no' then p0_negative_count end) as unsafe_ride_count,
    
    sum(p0_negative_count) as p0_negative_count,
    
    sum(p1_negative_count) as p1_negative_count, 
    
    sum(p2_negative_count) as p2_negative_count,
    
    sum(coalesce(negative_count, 0) * coalesce(gender_wtge, 0) * coalesce(fc_wtge, 0) * coalesce(ctxt_wtge, 0) * coalesce(week_wtge, 0)) as ngtv_fc_wge
from 
    (
    select
        week_fin, service, city, captain_id, context, ctxt_wtge, fc_wtge, gender_wtge, week_wtge,
        count(order_id) as negative_count,
        
        count(case when priority = 'P0' then order_id end) as p0_negative_count, 
        count(case when priority = 'P1' then order_id end) as p1_negative_count, 
        count(case when priority = 'P2' then order_id end) as p2_negative_count
        
    from 
        (
        select
            week_fin, service, feedback_chips, city, captain_id, order_id, unique_id, week_wtge,
            context, priority, (case when service = 'Link' then bike_ctxt_wtge else auto_ctxt_wtge end) as ctxt_wtge, 
            fc_wtge, customer_gender, (case when customer_gender = 'female' then female_wtge else male_blank_wtge end) as gender_wtge
        from
            (
            select
                pr_tab.*, context
            from 
                orders_raw pr_tab join rfrnc_tab
            on
                contains(pr_tab.feedback_chips, rfrnc_tab.chip)
            ) prt_ref
            
            left join weightage_tab
            on prt_ref.context = weightage_tab.context_w
        )
    group by 
        1, 2, 3, 4, 5, 6, 7, 8, 9
    )
group by 
    1, 2, 3 
)

,freshdesk_tab as 
(
select  
    distinct custom_fields_cf_rd_order_id as rd_order_id, custom_fields_cf_calling_priority, 
    custom_fields_cf_reason, custom_fields_cf_sub_reason918254, custom_fields_cf_booking_id as booking_id
from
    freshdesk.tickets_snapshot
where 
    yyyymmdd >= date_format(date('2023-01-23') - interval '1' day,'%Y%m%d')       ---- start date (before 6 months)
    and yyyymmdd <= date_format(date('2023-09-07'),'%Y%m%d')                      ---- Date is fixed, don't change this
    and custom_fields_cf_ticketing_disposition = 'Customer Support'
    and custom_fields_cf_sub_reason918254 is not null
    and custom_fields_cf_reason IN (select distinct feedback_identfier from experiments.hsc_risk_identifiers_new where channel = 'escalation_ticket')
    and tags not like '%customer_live_support%' 
)

,tckts_tab as 
(
select
    service, city, captain_id, 
    sum(negative_count) as negative_count, 
    
    sum(case when esc_context_new = 'safe_behaviour_no' then p0_negative_count end) as unsafe_behave_count,
    sum(case when esc_context_new = 'safe_ride_no' then p0_negative_count end) as unsafe_ride_count,
    
    sum(p0_negative_count) as p0_negative_count,
    
    sum(p1_negative_count) as p1_negative_count, 
    
    sum(p2_negative_count) as p2_negative_count,
    
    sum(p0_escalations_count) as p0_escalations_count,
    
    sum(p1_escalations_count) as p1_escalations_count,
    
    sum(p2_escalations_count) as p2_escalations_count,
    
    sum(coalesce(ctxt_wtge, 0) * coalesce(negative_count, 0) * coalesce(gender_wtge, 0) * coalesce(esc_wtge, 0) * coalesce(week_wtge, 0) * priority_wtge) as ngtv_esc_wtge
from 
    (
    select
        week_fin, service, city, captain_id, esc_context_new, (case when service = 'Link' then bike_ctxt_wtge else auto_ctxt_wtge end) as ctxt_wtge,
        esc_wtge, (case when customer_gender = 'female' then female_wtge else male_blank_wtge end) as gender_wtge, week_wtge, priority_wtge,
        
        count(case when priority is not null then order_id end) as negative_count, 
        
        count(case when priority = 'P0' then order_id end) as p0_negative_count, 
        count(case when priority = 'P1' then order_id end) as p1_negative_count, 
        count(case when priority = 'P2' then order_id end) as p2_negative_count,
        
        count(distinct case when custom_fields_cf_calling_priority = 'P0' then order_id end) as p0_escalations_count, 
        count(distinct case when custom_fields_cf_calling_priority = 'P1' then order_id end) as p1_escalations_count, 
        count(distinct case when custom_fields_cf_calling_priority = 'P2' then order_id end) as p2_escalations_count
    from 
        (
        select 
            distinct tfrsh.order_id, tfrsh.custom_fields_cf_calling_priority,
            
            (case 
                when tfrsh.custom_fields_cf_calling_priority = 'P0' then 6
                when tfrsh.custom_fields_cf_calling_priority = 'P1' then 3
            else 
                1 
            end) as priority_wtge, tfrsh.week_fin, tfrsh.service, tfrsh.city, tfrsh.captain_id, tfrsh.week_wtge, tfrsh.customer_gender,
            
            coalesce(esc_refrnc_ph1.context, esc_refrnc_ph22.context) as esc_context_new
        from 
            (
            select
                distinct order_id, custom_fields_cf_calling_priority, custom_fields_cf_reason,
                custom_fields_cf_sub_reason918254, week_fin, service, city, captain_id, week_wtge, customer_gender
            from 
                (
                (
                select
                    orders_raw.order_id, custom_fields_cf_calling_priority, custom_fields_cf_reason,
                    custom_fields_cf_sub_reason918254, week_fin, service, city, captain_id, week_wtge, customer_gender
                from 
                    freshdesk_tab
                    
                    inner join orders_raw
                    on freshdesk_tab.rd_order_id = orders_raw.unique_id
                ) 
                UNION
                (
                select
                    orders_raw.order_id, custom_fields_cf_calling_priority, custom_fields_cf_reason,
                    custom_fields_cf_sub_reason918254, week_fin, service, city, captain_id, week_wtge, customer_gender
                from 
                    freshdesk_tab
                    
                    inner join orders_raw
                    on freshdesk_tab.booking_id = orders_raw.order_id
                )
                UNION
                (
                select
                    orders_raw.order_id, custom_fields_cf_calling_priority, custom_fields_cf_reason,
                    custom_fields_cf_sub_reason918254, week_fin, service, city, captain_id, week_wtge, customer_gender
                from    
                    (
                    select
                        distinct booking_id as book_id, json_extract_scalar(data,'$.calling_priority') as custom_fields_cf_calling_priority, 
                        reason as custom_fields_cf_reason, sub_reason918254 as custom_fields_cf_sub_reason918254
                    from 
                        raw.kafka_domain_support_tickets_v2_immutable
                    where 
                        yyyymmdd >= date_format(date('2023-09-08'),'%Y%m%d')       ---- Date is fixed, don't change this
                        and yyyymmdd <= date_format(date('2023-07-23') + interval '10' day,'%Y%m%d')                      ---- end date (after 6 months)
                        and booking_id IN (select order_id from orders_raw) 
                        and sub_reason918254 is not null
                        and not contains(cast(tags as array<varchar>), 'customer_live_support')
                        and reason IN (select distinct feedback_identfier from experiments.hsc_risk_identifiers_new where channel = 'escalation_ticket')
                    ) new_esc_tab 
                    
                    inner join orders_raw
                    on new_esc_tab.book_id = orders_raw.order_id
                )
                )
            
            ) tfrsh 
            
            left join 
            (select distinct * from experiments.hsc_risk_identifiers_new 
            where channel = 'escalation_ticket' and length(feedback_extension) >= 1) esc_refrnc_ph1 
            on tfrsh.custom_fields_cf_reason = esc_refrnc_ph1.feedback_identfier
            and tfrsh.custom_fields_cf_sub_reason918254 like ('%' || esc_refrnc_ph1.feedback_extension|| '%')
            
            left join 
            (select distinct * from experiments.hsc_risk_identifiers_new 
            where channel = 'escalation_ticket' and (feedback_extension is null or feedback_extension = '')) esc_refrnc_ph22 
            on tfrsh.custom_fields_cf_reason = esc_refrnc_ph22.feedback_identfier
            
        ) tfd_tab 
        
        left join weightage_tab
        on tfd_tab.esc_context_new = weightage_tab.context_w
    where 
        esc_context_new is not null 
    group by 
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10 
    )
group by 
    1, 2, 3
)

,q_logs as 
(
select
    service, city, captain_id,
    sum(negative_count) as negative_count, 
    
    sum(case when data_context_new = 'safe_behaviour_no' then p0_negative_count end) as unsafe_behave_count,
    sum(case when data_context_new = 'safe_ride_no' then p0_negative_count end) as unsafe_ride_count,
    
    sum(p0_negative_count) as p0_negative_count,
    
    sum(p1_negative_count) as p1_negative_count, 
    
    sum(p2_negative_count) as p2_negative_count,
    
    sum(coalesce(mf_wtge, 0) * coalesce(gender_wtge, 0) * coalesce(ctxt_wtge, 0) * coalesce(negative_count, 0) * coalesce(week_wtge, 0)) as ngtv_mf_wtge
from 
    (
    select
        week_fin, service, city, captain_id, data_context_new, (case when service = 'Link' then bike_ctxt_wtge else auto_ctxt_wtge end) as ctxt_wtge, 
        mf_wtge, (case when customer_gender = 'female' then female_wtge else male_blank_wtge end) as gender_wtge, week_wtge,

        count(case when priority is not null then data_orderid end) as negative_count, 
        
        count(case when priority = 'P0' then order_id end) as p0_negative_count, 
        count(case when priority = 'P1' then order_id end) as p1_negative_count, 
        count(case when priority = 'P2' then order_id end) as p2_negative_count
    from 
        (
        (
        select
            distinct data_orderid, context as data_context_new
        from 
            (
            select
                distinct data_orderid, lower(data_text) as data_text, data_feedback
            from 
                raw.kafka_quality_logs_immutable
            where 
                yyyymmdd >= '20230215'
                and yyyymmdd <= date_format(date('2023-07-23') + interval '1' day,'%Y%m%d')
                and data_orderid IN (select distinct order_id from orders_raw)
                and data_screen NOT IN ('ratings','storeRatings')
                and data_audience = 'customer'
                and data_questionid != 'none'
                and data_text != ''
            ) pr_qlogs11
            
            inner join 
            (select distinct channel, feedback_identfier, feedback_extension, context from experiments.hsc_risk_identifiers_new
            where channel = 'micro_feedback') mf_ph_11
            on pr_qlogs11.data_text like  ('%' || mf_ph_11.feedback_identfier|| '%')
            and pr_qlogs11.data_feedback =  mf_ph_11.feedback_extension
        )
        union 
        (
        select
            distinct data_orderid, context as data_context_new
        from
            (
            select
                distinct order_id as data_orderid, lower("text") as data_text, feedback as data_feedback
            from 
                raw.mongodb_rapidoqaulity_qualitylogs_immutable
            where 
                yyyymmdd >= date_format(date('2023-01-23') - interval '1' day,'%Y%m%d')
                and yyyymmdd <= '20230214'
                and audience = 'customer'
                and order_id IN (select distinct order_id from orders_raw)
                and lower(cast(screen as varchar)) not like '%ratings%'
            )  pr_qlogs22
            
            inner join 
            (select distinct channel, feedback_identfier, feedback_extension, context from experiments.hsc_risk_identifiers_new
            where channel = 'micro_feedback') mf_ph_22
            on pr_qlogs22.data_text like  ('%' || mf_ph_22.feedback_identfier|| '%')
            and pr_qlogs22.data_feedback =  mf_ph_22.feedback_extension
        )
        ) q_l_tab1 
        
        inner join orders_raw
        on q_l_tab1.data_orderid = orders_raw.order_id
        
        left join weightage_tab
        on q_l_tab1.data_context_new = weightage_tab.context_w
    where 
        data_context_new is not null
    group by 
        1, 2, 3, 4, 5, 6, 7, 8, 9  
    )
group by 
    1, 2, 3 
)

,fin_wtge_tab as 
(
select
    coalesce(fc_tckts.service, q_logs.service) as service, coalesce(fc_tckts.city, q_logs.city) as city,
    
    coalesce(fc_tckts.captain_id, q_logs.captain_id) as captain_id,
    
    coalesce(fc_negative_count, 0) + coalesce(tckts_negative_count, 0) + coalesce(q_logs.negative_count, 0) as negative_count, 
    
    coalesce(fc_p0, 0) + coalesce(tckts_p0, 0) + coalesce(q_logs.p0_negative_count, 0) as p0_negative_count,  coalesce(tckts_p0, 0) as esc_p0_negative_count,
    
    coalesce(fc_p1, 0) + coalesce(tckts_p1, 0) + coalesce(q_logs.p1_negative_count, 0) as p1_negative_count, coalesce(tckts_p1, 0) as esc_p1_negative_count,
    
    coalesce(fc_p2, 0) + coalesce(tckts_p2, 0) + coalesce(q_logs.p2_negative_count, 0) as p2_negative_count, coalesce(tckts_p2, 0) as esc_p2_negative_count,
    
    coalesce(ngtv_fc_wge, 0) + coalesce(ngtv_esc_wtge, 0) + coalesce(ngtv_mf_wtge, 0) as total_weightage, 
    
    coalesce(fc_unsafe_bhv, 0) + coalesce(tcks_unsafe_bhv, 0) + coalesce(q_logs.unsafe_behave_count, 0) as unsafe_bhv_cnt, 
    
    coalesce(fc_unsafe_rde, 0) + coalesce(tcks_unsafe_rde, 0) + coalesce(q_logs.unsafe_ride_count, 0) as unsafe_rde_cnt, 
    
    coalesce(p0_escalations_count, 0) as p0_escalations_count, coalesce(p1_escalations_count, 0) as p1_escalations_count,
    
    coalesce(p2_escalations_count, 0) as p2_escalations_count
    
from 
    (
    select
        coalesce(fc_wtge_tab.service, tckts_tab.service) as service, coalesce(fc_wtge_tab.city, tckts_tab.city) as city,
    
        coalesce(fc_wtge_tab.captain_id, tckts_tab.captain_id) as captain_id, 
        
        fc_wtge_tab.negative_count as fc_negative_count, ngtv_fc_wge, tckts_tab.negative_count as tckts_negative_count, ngtv_esc_wtge, 
        
        fc_wtge_tab.p0_negative_count as fc_p0, fc_wtge_tab.p1_negative_count as fc_p1, fc_wtge_tab.p2_negative_count as fc_p2, 
        
        tckts_tab.p0_negative_count as tckts_p0, tckts_tab.p1_negative_count as tckts_p1, tckts_tab.p2_negative_count as tckts_p2, 
        
        fc_wtge_tab.unsafe_behave_count as fc_unsafe_bhv, fc_wtge_tab.unsafe_ride_count as fc_unsafe_rde, 
        
        tckts_tab.unsafe_behave_count as tcks_unsafe_bhv, tckts_tab.unsafe_ride_count as tcks_unsafe_rde, 
        
        tckts_tab.p0_escalations_count, tckts_tab.p1_escalations_count, tckts_tab.p2_escalations_count
        
    from 
        fc_wtge_tab full outer join tckts_tab 
    on 
        fc_wtge_tab.service = tckts_tab.service
        and fc_wtge_tab.city = tckts_tab.city
        and fc_wtge_tab.captain_id = tckts_tab.captain_id
    ) fc_tckts 
    
    full outer join q_logs
    on  
        fc_tckts.service = q_logs.service
        and fc_tckts.city = q_logs.city
        and fc_tckts.captain_id = q_logs.captain_id
)

,thshlds_wt as 
(
select
    fin_wtge_tab.service, city,
    approx_percentile(total_weightage, 0.50) as wt_pct_low, 
    approx_percentile(total_weightage, 0.80) as wt_pct_mid, 
    approx_percentile(total_weightage, 0.95) as wt_pct_top 
from 
    fin_wtge_tab 
where 
    negative_count > 0 
group by 
    1, 2 
)

select
    as_of_date, service, city, service_detail_id, captain_id, net_orders, negative_count as negative_feedback_count, p0_negative_count as p0_negative_feedback_count,
    p1_negative_count as p1_negative_feedback_count, p2_negative_count as p2_negative_feedback_count, rss_weightage, unsafe_bhv_cnt as unsafe_behaviour_count,
    unsafe_rde_cnt as unsafe_ride_count, p0_escalations_count, p1_escalations_count, p2_escalations_count, 
    (case 
        when rss_weightage >= wt_pct_top then 'UHR'
        when rss_weightage >= wt_pct_mid then 'HR'
        when rss_weightage >= wt_pct_low then 'MR'
        when rss_weightage is null then 'NC'
    else 
        'LR'
    end) as safety_profile, 
    (case 
        when unsafe_bhv_cnt > 1 and unsafe_bhv_pctg >= 0.35 then 'Unsafe Behaviour'
        when unsafe_rde_cnt > 1 and unsafe_rde_pctg >= 0.35 then 'Unsafe Riding'
    else 
        'None'
    end) as safety_proximity
from 
    (
    select
        date_format(date('2023-07-23'),'%Y-%m-%d') as as_of_date, m_net_c.*, negative_count, 
        
        p0_negative_count, 
        (case when negative_count >= 1 then cast(p0_negative_count as double)/negative_count end) as p0_negative_pctg,
        
        p1_negative_count, p2_negative_count, total_weightage as rss_weightage, wt_pct_low, wt_pct_mid, wt_pct_top,
        
        unsafe_bhv_cnt, 
        (case when negative_count >= 1 then cast(unsafe_bhv_cnt as double)/negative_count end) as unsafe_bhv_pctg,
        
        unsafe_rde_cnt,
        (case when negative_count >= 1 then cast(unsafe_rde_cnt as double)/negative_count end) as unsafe_rde_pctg,
        
        p0_escalations_count, p1_escalations_count, p2_escalations_count
        
    from 
        (
        select
            service, city, service_detail_id, captain_id, count(distinct order_id) as net_orders
        from
            orders_raw
        group by 
            1, 2, 3, 4
        ) m_net_c 
        
        left join 
        (
        select
            fin_wtge_tab.*, wt_pct_low, wt_pct_mid, wt_pct_top
        from 
            fin_wtge_tab 
            
            left join thshlds_wt
            on fin_wtge_tab.service = thshlds_wt.service
            and fin_wtge_tab.city = thshlds_wt.city
        ) cv_caps 
        on 
            m_net_c.service = cv_caps.service
            and m_net_c.city = cv_caps.city
            and m_net_c.captain_id = cv_caps.captain_id
            

    )
