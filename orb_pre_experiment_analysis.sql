with segment_tab as 
(
select
    distinct captain_id as cap_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name, consistency
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230305'
    and lower(city_name) IN ('bangalore','chennai')
)

,pc_v2_tab as
(
select
    captain_id as cap_id_2, lower(city) as city_1, tod_affinity, lm_affinity
from    
    experiments.cap_pc_v2_auto
where 
    yyyymmdd = '20230305'
    and lower(city) IN ('bangalore','chennai')
)


select
    city, pre_tab.captain_id, performance_segment, consistency, tod_affinity, lm_affinity, total_login_hours, 
    login_days, net_rides, earnings_amount, active_days,
    (case 
        when active_days >= 1 and active_days < 4 then '1-4 AD'
        when active_days >= 4 and active_days < 9 then '4-9 AD'
        when active_days >= 9 then '9+ AD'
    end) as AD_bucket,
    (case 
        when RPD_bucket >= 1 and RPD_bucket < 4 then '1-4 RPD'
        when RPD_bucket >= 4 and RPD_bucket < 7 then '4-7 RPD'
        when RPD_bucket >= 7 then '7+ RPD'
    end) as RPD_bucket,
    (case 
        when LHPD_bucket > 0 and LHPD_bucket < 3 then '0-3 LHPD'
        when LHPD_bucket >= 3 and LHPD_bucket < 5 then '3-5 LHPD'
        when LHPD_bucket >= 5 then '5+ LHPD'
    end) as LHPD_bucket,
     (case 
        when EPD_bucket > 0 and EPD_bucket < 400 then '0-400 EPD'
        when EPD_bucket >= 400 and EPD_bucket < 700 then '400-700 EPD'
        when EPD_bucket >= 700 then '700+ EPD'
    end) as EPD_bucket, 
    
    row_number() over(partition by city order by random()) as random_row
from 
    (
    select 
        lower(city) as city, captain_id,
        sum(total_login_hours) as total_login_hours, 
        count(distinct case when total_login_hours > 0 then yyyymmdd end) as login_days,
        sum(net_rides_taxi) as net_rides, count(distinct case when net_rides_taxi > 0 then yyyymmdd end)  as active_days,
        sum(coalesce(taxi_order_earnings,0) + coalesce(daily_incentive_amount,0) + coalesce(weekly_incentive_amount,0)) as earnings_amount, 
        
        cast(sum(net_rides_taxi) as double)/count(distinct case when net_rides_taxi > 0 then yyyymmdd end) as RPD_bucket,
        
        cast(sum(total_login_hours) as double)/count(distinct case when total_login_hours > 0 then yyyymmdd end) as LHPD_bucket,
        
        cast(sum(coalesce(taxi_order_earnings,0) + coalesce(daily_incentive_amount,0) + coalesce(weekly_incentive_amount,0)) as double)
        /count(distinct case when net_rides_taxi > 0 then yyyymmdd end) as EPD_bucket
        
    from 
        datasets.captain_metrics_segments segms
    where
        yyyymmdd >= '20230220'
        and yyyymmdd <= '20230305'
        and lower(service_mode) = 'auto'
        and lower(city) IN ('bangalore','chennai')
    group by 
        1, 2 
    ) pre_tab left join segment_tab 
    on pre_tab.city = segment_tab.city_name
    and pre_tab.captain_id = segment_tab.cap_id
    
    left join pc_v2_tab
    on pre_tab.city = pc_v2_tab.city_1  
    and pre_tab.captain_id = pc_v2_tab.cap_id_2
    
where 
    performance_segment IN ('MP','HP','UHP')
    and is_nan(RPD_bucket) = false
    and is_nan(LHPD_bucket) = false
    and is_nan(EPD_bucket) = false
    and active_days > 0 
    and earnings_amount > 0
    
    order by captain_id

    limit 20 

------

nnew query - bucket list

with segment_tab as 
(
select
    distinct captain_id as cap_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name, consistency
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230305'
    and lower(city_name) IN ('bangalore','chennai')
)

,pc_v2_tab as
(
select
    captain_id as cap_id_2, lower(city) as city_1, tod_affinity, lm_affinity
from    
    experiments.cap_pc_v2_auto
where 
    yyyymmdd = '20230305'
    and lower(city) IN ('bangalore','chennai')
)

,strat_caps_dump as 
(
select
    *,
    array[AD_bucket, RPD_bucket, LHPD_bucket, EPD_bucket] as strat_bucket
from
    (
    select
        city, pre_tab.captain_id, performance_segment, consistency, tod_affinity, lm_affinity, total_login_hours, 
        login_days, net_rides, earnings_amount, active_days,
        (case 
            when active_days >= 1 and active_days < 4 then '1-4 AD'
            when active_days >= 4 and active_days < 9 then '4-9 AD'
            when active_days >= 9 then '9+ AD'
        end) as AD_bucket,
        (case 
            when RPD_bucket >= 1 and RPD_bucket < 4 then '1-4 RPD'
            when RPD_bucket >= 4 and RPD_bucket < 7 then '4-7 RPD'
            when RPD_bucket >= 7 then '7+ RPD'
        end) as RPD_bucket,
        (case 
            when LHPD_bucket > 0 and LHPD_bucket < 3 then '0-3 LHPD'
            when LHPD_bucket >= 3 and LHPD_bucket < 5 then '3-5 LHPD'
            when LHPD_bucket >= 5 then '5+ LHPD'
        end) as LHPD_bucket,
         (case 
            when EPD_bucket > 0 and EPD_bucket < 400 then '0-400 EPD'
            when EPD_bucket >= 400 and EPD_bucket < 700 then '400-700 EPD'
            when EPD_bucket >= 700 then '700+ EPD'
        end) as EPD_bucket
        
        
    from 
        (
        select 
            lower(city) as city, captain_id,
            sum(total_login_hours) as total_login_hours, 
            count(distinct case when total_login_hours > 0 then yyyymmdd end) as login_days,
            sum(net_rides_taxi) as net_rides, count(distinct case when net_rides_taxi > 0 then yyyymmdd end)  as active_days,
            sum(coalesce(taxi_order_earnings,0) + coalesce(daily_incentive_amount,0) + coalesce(weekly_incentive_amount,0)) as earnings_amount, 
            
            cast(sum(net_rides_taxi) as double)/count(distinct case when net_rides_taxi > 0 then yyyymmdd end) as RPD_bucket,
            
            cast(sum(total_login_hours) as double)/count(distinct case when total_login_hours > 0 then yyyymmdd end) as LHPD_bucket,
            
            cast(sum(coalesce(taxi_order_earnings,0) + coalesce(daily_incentive_amount,0) + coalesce(weekly_incentive_amount,0)) as double)
            /count(distinct case when net_rides_taxi > 0 then yyyymmdd end) as EPD_bucket
            
        from 
            datasets.captain_metrics_segments segms
        where
            yyyymmdd >= '20230220'
            and yyyymmdd <= '20230305'
            and lower(service_mode) = 'auto'
            and lower(city) IN ('bangalore','chennai')
        group by 
            1, 2 
        ) pre_tab left join segment_tab 
        on pre_tab.city = segment_tab.city_name
        and pre_tab.captain_id = segment_tab.cap_id
        
        left join pc_v2_tab
        on pre_tab.city = pc_v2_tab.city_1  
        and pre_tab.captain_id = pc_v2_tab.cap_id_2
        
    where 
        performance_segment IN ('MP','HP','UHP')
        and is_nan(RPD_bucket) = false
        and is_nan(LHPD_bucket) = false
        and is_nan(EPD_bucket) = false
        and active_days > 0 
        and earnings_amount > 0
    )
)

-- select
--     city, cohort, count(distinct captain_id) as count_caps
-- from
-- (
select
    *,
    (case 
        when count(captain_id) over (partition by strat_bucket) < 5 then 'holdout'
        when (random_row % 5) = 4 then 'test'
        when (random_row % 5) < 4 then 'control'
    else 
        'check'
    end) as cohort
from 
    (
    select 
        *, 
        row_number() over(partition by city, strat_bucket order by random()) as random_row 
    from 
        strat_caps_dump
    order by 1, 2 
    ) 
-- )
-- group by 
--     1, 2
-- order by 
--     1, 3 desc 


-- group by 1, 2, 3, 4 
-- order by 1, 2, 3, 4 
-- order by captain_id

limit 20 