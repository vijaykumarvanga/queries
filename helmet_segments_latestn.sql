with segment_base as 
(
select
    captain_id, segment, city_name
from
    (
    select 
        yyyymmdd, substr(segment, 1, 2) as segment, captain_id, lower(city_name) as city_name,
        row_number() over(partition by lower(city_name), captain_id order by yyyymmdd desc) as latest_day
    from 
        datasets.poc_segments_immutable_28_days  
    where 
        yyyymmdd >= date_format(current_date - interval '2' day,'%Y%m%d')
        and yyyymmdd <= date_format(current_date - interval '1' day,'%Y%m%d')
        and lower(city_name) = 'chennai'
        and lower(service_name) = 'link'
    )
where 
    latest_day = 1 
)

,mf_tab  as
(
select 
    distinct data_orderid as order_id, data_captainid as captain_id, data_text as mf_text, data_feedback as feedback
from 
    raw.kafka_quality_logs_immutable
where 
    yyyymmdd >= date_format(current_date - interval '29' day,'%Y%m%d')
    and yyyymmdd <= date_format(current_date,'%Y%m%d')
    and data_screen != 'ratings'
    and (lower(data_text) like '%offer a helmet%' or lower(data_text) like '%receive a helmet%'
    or lower(data_text) like '%given you a helmet%')
)

,cp_dump_n as 
(
select
    city, captain_id, net_orders_28d, helmet_coverage_28d, offered_helmet_ords_28d, helmet_issue_ords_28d, segment_new,
    (case when helmet_issue_ords_28d > 0 then cast(helmet_issue_ords_28d as double)/helmet_coverage_28d end) as issue_ptg_cvrg, 
    (case 
        when net_orders_28d >= net_ords_pct_95 then '95_pct_rides' 
        when net_orders_28d >= net_ords_pct_90 then '90_pct_rides' 
        when net_orders_28d >= net_ords_pct_80 then '80_pct_rides'
        when net_orders_28d >= net_ords_pct_70 then '70_pct_rides'
    else
        'below_70_pct_rides'
    end) as ride_bucket_28d
from 
    (
    select
        cp_dmp.*, 
        (coalesce(offered_helmet_ords_28d, 0) + coalesce(helmet_issue_ords_28d, 0)) as helmet_coverage_28d,
        (case 
            when segment = 'MP' then 'MP'
            when segment = 'HP' then 'HP'
            when segment = 'UH' then 'UHP'
        else 
            'LP'
        end) as segment_new, 
        approx_percentile(net_orders_28d, 0.70) over(partition by city) as net_ords_pct_70, 
        approx_percentile(net_orders_28d, 0.80) over(partition by city) as net_ords_pct_80, 
        approx_percentile(net_orders_28d, 0.90) over(partition by city) as net_ords_pct_90, 
        approx_percentile(net_orders_28d, 0.95) over(partition by city) as net_ords_pct_95
    from
        (
        select
            snp_tb.city, snp_tb.captain_id, 
            
            count(distinct snp_tb.order_id) as net_orders_28d,
            
            count(distinct case when (customer_feedback_rate_service like '%Good helmet%' or customer_feedback_rate_service like '%Clean Helmet%'
            or mf_tab.feedback = 'Yes') then snp_tb.order_id end) as offered_helmet_ords_28d, 
            
            count(distinct case when (customer_feedback_rate_service like '%Did not offer helmet%' or customer_feedback_rate_service like '%No Helmet%'
            or mf_tab.feedback = 'No') then snp_tb.order_id end) as helmet_issue_ords_28d
            
        from
            (
            select 
                yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
                captain_id, order_id, customer_feedback_rate_service
            from 
                orders.order_logs_snapshot
            where 
                yyyymmdd >= date_format(current_date - interval '28' day,'%Y%m%d')
                and yyyymmdd <= date_format(current_date - interval '1' day,'%Y%m%d')
                and service_obj_service_name = 'Link'
                and service_obj_city_display_name = 'Chennai'
                and order_status = 'dropped'
                and (spd_fraud_flag = false or spd_fraud_flag is null)
            ) snp_tb 
            
            left join mf_tab
            on snp_tb.order_id = mf_tab.order_id
        group by 
            1, 2
        ) cp_dmp 
      
        left join segment_base
        on cp_dmp.captain_id = segment_base.captain_id
        and lower(cp_dmp.city) = segment_base.city_name
    )
)

,pctl_tab as 
(
select
    city, segment_new, ride_bucket_28d, 
    approx_percentile(issue_ptg_cvrg, 0.85) as top_issue_pct, 
    approx_percentile(issue_ptg_cvrg, 0.55) as mid_issue_pct, 
    approx_percentile(issue_ptg_cvrg, 0.25) as low_issue_pct 
from 
    cp_dump_n
where 
    helmet_issue_ords_28d > 0 
group by 
    1, 2, 3
)


select
    date_format(date_trunc('week', current_date), '%Y-%m-%d') as segment_week, cp_dump_n.city, cp_dump_n.captain_id, 
    net_orders_28d, helmet_coverage_28d, offered_helmet_ords_28d, helmet_issue_ords_28d, 
    cp_dump_n.segment_new as poc_segment, issue_ptg_cvrg as issue_prctg_cvrg, cp_dump_n.ride_bucket_28d, 
    (case
        when issue_ptg_cvrg >= top_issue_pct and helmet_issue_ords_28d > 2 then 'Highly_Inconsistent'
        when issue_ptg_cvrg >= mid_issue_pct and helmet_issue_ords_28d > 1 then 'Inconsistent'
        when issue_ptg_cvrg >= low_issue_pct then 'Consistent'
        when (issue_ptg_cvrg < low_issue_pct or offered_helmet_ords_28d > 0) then 'Highly_Consistent'
    else 
        'Low_Coverage'
    end) as helmet_segment
from    
    cp_dump_n 
    
    left join pctl_tab
    on cp_dump_n.city = pctl_tab.city
    and cp_dump_n.segment_new = pctl_tab.segment_new
    and cp_dump_n.ride_bucket_28d = pctl_tab.ride_bucket_28d