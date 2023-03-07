opportunity frame work - auto apr
 newquery main summary

 with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment IN ('HP','UHP') then 'HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        (case 
            when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
            when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
            when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
            when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
            when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
            when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
            when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
            when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
            when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
            when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
            when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
            when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        else 
            'Blank'
        end) as fm_lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 10 then 'Bad'
            when epkm_new >= 10 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)

select
    date_disp, fm_lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
    
group by 
    1, 2, 3, 4, 5 
order by 
    1, 
    (case 
        when fm_lm_category = 'Good' then 1
        when fm_lm_category = 'Ok' then 2
        when fm_lm_category = 'Bad' then 3
    else 
        4 
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
        when epkm_category = 'Bad' then 3
    else 
        4 
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP' then 2 
        when pc_segment = 'HP_UHP' then 3
    else 
        4
    end),
    (case 
        when batch_number = '1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)
----

newquery lm category 

with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment IN ('HP','UHP') then 'HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 10 then 'Bad'
            when epkm_new >= 10 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)

select
    date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
    
group by 
    1, 2, 3, 4, 5 
order by 
    1, 
    (case 
        when lm_category = 'A_0-3_Km' then 1
        when lm_category = 'B_3-6_Km' then 2
        when lm_category = 'C_6-9_Km' then 3
        when lm_category = 'D_9+_Km' then 4
    else 
        5
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
        when epkm_category = 'Bad' then 3
    else 
        4 
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP' then 2 
        when pc_segment = 'HP_UHP' then 3
    else 
        4
    end),
    (case 
        when batch_number = '1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)


----

newquery session

with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag, quarter_hour
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment IN ('HP','UHP') then 'HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        (case 
            when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
            when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
            when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
            when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
            when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
            when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
            when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
            when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
            when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
            when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
            when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
            when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        else 
            'Blank'
        end) as fm_lm_category,
        
        -- (case 
        --     when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
        --     when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
        --     when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
        --     when (last_mile >= 9) then 'D_9+_Km' 
        -- else 
        --     'E.Blank'
        -- end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 10 then 'Bad'
            when epkm_new >= 10 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)

select
    date_disp, 
    
    (case when substr(quarter_hour, 1, 2) >= '06' and substr(quarter_hour,1,2) < '12' then 'Mrng' else 
        case when substr(quarter_hour, 1, 2) >= '12' and substr(quarter_hour,1,2) < '17' then 'Aftn' else
            case when substr(quarter_hour, 1, 2) >= '17' and substr(quarter_hour,1,2) < '22' then 'Evng' else 
                'Night' 
            end 
        end 
    end) as session,
    
    fm_lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
    
group by 
    1, 2, 3, 4, 5, 6 
order by 
    1, 
    (case when session = 'Mrng' then 1 
        when session = 'Aftn' then 2 
        when session = 'Evng' then 3
        else 4 
    end),
    (case 
        when fm_lm_category = 'Good' then 1
        when fm_lm_category = 'Ok' then 2
        when fm_lm_category = 'Bad' then 3
    else 
        4 
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
        when epkm_category = 'Bad' then 3
    else 
        4 
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP' then 2 
        when pc_segment = 'HP_UHP' then 3
    else 
        4
    end),
    (case 
        when batch_number = '1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)
-----

newquery

V2 OPPORTUNITY FRAMEWORK - APR

LM category - Total DAY

with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)


select
    orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
    accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
    accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
    busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
    rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
from 
(
select
    date_disp as orderdate, lm_category, epkm_category, pc_segment, 
    batch_number, mapping_pings, mapping_riders, accept_pings, 
    cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
    round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
    cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
    busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
    rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
from 
(
select
    date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '_1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
    
group by 
    1, 2, 3, 4, 5 
)
)
where 
    lm_category != 'Blank'
    and epkm_category != 'Blank'
    and pc_segment != 'Blank'
order by 
    1, 
    (case 
        when lm_category = 'A_0-3_Km' then 1
        when lm_category = 'B_3-6_Km' then 2
        when lm_category = 'C_6-9_Km' then 3
        when lm_category = 'D_9+_Km' then 4
    else 
        5
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
    else 
        3
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP_HP_UHP' then 2
    else 
        4
    end),
    (case 
        when batch_number = '_1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)
----
newquery - segment level login, gross, net caps

segment level basic metrics:

with segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,logs_snap as 
(
select
    lg_sn.yyyymmdd, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment = 'HP' then 'HP'
        when performance_segment = 'UHP' then 'UHP'
    else 
        'Blank'
     end) as segment_snap,
     count(distinct order_id) as net_orders,
     count(distinct cap_id) as net_captains
from 
    (
    select 
        yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, captain_id as cap_id,
        order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag, quarter_hour
    from 
        orders.order_logs_snapshot
    where 
        yyyymmdd = '20230213'
        and service_obj_city_display_name = 'Hyderabad'
        and service_obj_service_name = 'Auto'
        and order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) lg_sn left join segment_tab
    on lg_sn.cap_id = segment_tab.captain_id
group by 
    1, 2 
)

,imm_tab as 
(
select
    im_t.yyyymmdd, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment = 'HP' then 'HP'
        when performance_segment = 'UHP' then 'UHP'
    else 
        'Blank'
     end) as segment_im,
    count(distinct cap_id) as gross_captains,
    sum(total_pings) as total_pings,
    count(distinct case when accepted_pings > 0 then cap_id end) as accept_caps,
    sum(accepted_pings) as accepted_pings
from 
    (
    select 
        yyyymmdd, captain_id as cap_id,
        count(order_id) as total_pings,
        count(case when event_type = 'accepted' then order_id end) as accepted_pings
    from
        orders.order_logs_immutable
    where 
        yyyymmdd = '20230213'
        and service_obj_city_display_name = 'Hyderabad'
        and service_obj_service_name = 'Auto'
        and event_type in ('rider_reject', 'rider_busy', 'accepted')
    group by 
        1, 2 
    ) im_t left join segment_tab
    on im_t.cap_id = segment_tab.captain_id
group by 
    1, 2 

)

,log_in_tab as 
(
select
    pr_lh.yyyymmdd, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment = 'HP' then 'HP'
        when performance_segment = 'UHP' then 'UHP'
    else 
        'Blank'
     end) as segment_lh, 
     count(distinct pr_lh.captain_id) as login_caps,
     sum(login_hrs) as login_hrs,
     sum(idle_hrs) as idle_hrs
from 
    (
    select
        yyyymmdd, captain_id, 
        cast(sum(duration) as double)/3600 as login_hrs,
        cast(sum(case when status = 2 then duration end) as double)/3600 as idle_hrs
    from
        datasets.supplycursory_history
    where
        yyyymmdd = '20230213'
        and status in (2, 3, 6, 7, 8, 10)
        and servicedetailid = '5ef2bc5b85846b775f97d170'
    group by
        1, 2
    ) pr_lh left join segment_tab
    on pr_lh.captain_id = segment_tab.captain_id
group by 
    1, 2 
)




select
    logs_snap.yyyymmdd, segment_snap, login_caps, login_hrs, idle_hrs, 
    gross_captains, total_pings, accept_caps, accepted_pings, net_captains, net_orders
from 
    logs_snap left join imm_tab
    on logs_snap.yyyymmdd = imm_tab.yyyymmdd
    and logs_snap.segment_snap = imm_tab.segment_im
        
    left join log_in_tab
    on logs_snap.yyyymmdd = log_in_tab.yyyymmdd
    and logs_snap.segment_snap = log_in_tab.segment_lh
order by 
    1, 
    (case 
        when segment_snap = 'LP' then 1
        when segment_snap = 'MP' then 2
        when segment_snap = 'HP' then 3
        when segment_snap = 'UHP' then 4
    else 
        5
    end)


-----

newquery 
final - opp framework query
with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)


select
    orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
    accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
    accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
    busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
    rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
from 
(
select
    date_disp as orderdate, lm_category, epkm_category, pc_segment, 
    batch_number, mapping_pings, mapping_riders, accept_pings, 
    cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
    round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
    cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
    busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
    rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
from 
(
select
    date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '_1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
-- where 
--     order_status = 'expired'
--     and contains(map_riders_check,'true')
    
group by 
    1, 2, 3, 4, 5 
)
)
where 
    lm_category != 'Blank'
    -- and epkm_category != 'Blank'
    and pc_segment != 'Blank'
order by 
    1, 
    (case 
        when lm_category = 'A_0-3_Km' then 1
        when lm_category = 'B_3-6_Km' then 2
        when lm_category = 'C_6-9_Km' then 3
        when lm_category = 'D_9+_Km' then 4
    else 
        5
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
    else 
        3
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP_HP_UHP' then 2
    else 
        4
    end),
    (case 
        when batch_number = '_1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)
----
newquery

clusters 
with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
    and pickup_cluster IN ('Hitech City','Kukatpally 3')
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)


select
    orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
    accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
    accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
    busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
    rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
from 
(
select
    date_disp as orderdate, lm_category, epkm_category, pc_segment, 
    batch_number, mapping_pings, mapping_riders, accept_pings, 
    cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
    round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
    cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
    busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
    rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
from 
(
select
    date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '_1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
where 
    order_status = 'expired'
    and contains(map_riders_check,'true')
    
group by 
    1, 2, 3, 4, 5 
)
)
where 
    lm_category != 'Blank'
    and epkm_category != 'Blank'
    and pc_segment != 'Blank'
order by 
    1, 
    (case 
        when lm_category = 'A_0-3_Km' then 1
        when lm_category = 'B_3-6_Km' then 2
        when lm_category = 'C_6-9_Km' then 3
        when lm_category = 'D_9+_Km' then 4
    else 
        5
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
    else 
        3
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP_HP_UHP' then 2
    else 
        4
    end),
    (case 
        when batch_number = '_1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)
---
session final newquery

with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag, quarter_hour
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)


select
    orderdate, lm_category, epkm_category, pc_segment, session, mapping_pings, mapping_riders, 
    accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
    accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
    busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
    rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
from 
(
select
    date_disp as orderdate, lm_category, epkm_category, pc_segment, 
    session, mapping_pings, mapping_riders, accept_pings, 
    cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
    round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
    cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
    busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
    rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
from 
(
select
    date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
    
    (case when substr(quarter_hour, 1, 2) >= '06' and substr(quarter_hour,1,2) < '12' then 'Mrng' else 
        case when substr(quarter_hour, 1, 2) >= '12' and substr(quarter_hour,1,2) < '17' then 'Aftn' else
            case when substr(quarter_hour, 1, 2) >= '17' and substr(quarter_hour,1,2) < '22' then 'Evng' else 
                case when substr(quarter_hour, 1, 2) >= '22' or substr(quarter_hour,1,2) < '06' then 'Night' else 
                'Check'
                end
            end 
        end 
    end) as session,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
    
group by 
    1, 2, 3, 4, 5 
)
)
where 
    lm_category != 'Blank'
    and epkm_category != 'Blank'
    and pc_segment != 'Blank'
order by 
    1, 
    (case 
        when lm_category = 'A_0-3_Km' then 1
        when lm_category = 'B_3-6_Km' then 2
        when lm_category = 'C_6-9_Km' then 3
        when lm_category = 'D_9+_Km' then 4
    else 
        5
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
    else 
        3
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP_HP_UHP' then 2
    else 
        4
    end),
    (case when session = 'Mrng' then 1 
        when session = 'Aftn' then 2 
        when session = 'Evng' then 3
        when session = 'Night' then 4
        else 5
    end)

------
newquery basic segment level summary 
with segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,logs_snap as 
(
select
    lg_sn.yyyymmdd, 
    (case when substr(quarter_hour, 1, 2) >= '06' and substr(quarter_hour,1,2) < '12' then 'Mrng' else 
        case when substr(quarter_hour, 1, 2) >= '12' and substr(quarter_hour,1,2) < '17' then 'Aftn' else
            case when substr(quarter_hour, 1, 2) >= '17' and substr(quarter_hour,1,2) < '22' then 'Evng' else 
                'Night' 
            end 
        end 
    end) as session,
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment = 'HP' then 'HP'
        when performance_segment = 'UHP' then 'UHP'
    else 
        'Blank'
     end) as segment_snap,
     count(distinct order_id) as net_orders,
     count(distinct cap_id) as net_captains
from 
    (
    select 
        yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, captain_id as cap_id,
        order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag, quarter_hour
    from 
        orders.order_logs_snapshot
    where 
        yyyymmdd = '20230213'
        and service_obj_city_display_name = 'Hyderabad'
        and service_obj_service_name = 'Auto'
        and order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) lg_sn left join segment_tab
    on lg_sn.cap_id = segment_tab.captain_id
group by 
    1, 2, 3 
)

,imm_tab as 
(
select
    im_t.yyyymmdd, session,
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment = 'HP' then 'HP'
        when performance_segment = 'UHP' then 'UHP'
    else 
        'Blank'
     end) as segment_im,
    count(distinct cap_id) as gross_captains,
    sum(total_pings) as total_pings,
    count(distinct case when accepted_pings > 0 then cap_id end) as accept_caps,
    sum(accepted_pings) as accepted_pings
from 
    (
    select 
        yyyymmdd,
        (case when substr(quarter_hour, 1, 2) >= '06' and substr(quarter_hour,1,2) < '12' then 'Mrng' else 
            case when substr(quarter_hour, 1, 2) >= '12' and substr(quarter_hour,1,2) < '17' then 'Aftn' else
                case when substr(quarter_hour, 1, 2) >= '17' and substr(quarter_hour,1,2) < '22' then 'Evng' else 
                    'Night' 
                end 
            end 
        end) as session,
        captain_id as cap_id,
        count(order_id) as total_pings,
        count(case when event_type = 'accepted' then order_id end) as accepted_pings
    from
        orders.order_logs_immutable
    where 
        yyyymmdd = '20230213'
        and service_obj_city_display_name = 'Hyderabad'
        and service_obj_service_name = 'Auto'
        and event_type in ('rider_reject', 'rider_busy', 'accepted')
    group by 
        1, 2, 3 
    ) im_t left join segment_tab
    on im_t.cap_id = segment_tab.captain_id
group by 
    1, 2, 3

)

,log_in_tab as 
(
select
    pr_lh.yyyymmdd, session,
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment = 'MP' then 'MP'
        when performance_segment = 'HP' then 'HP'
        when performance_segment = 'UHP' then 'UHP'
    else 
        'Blank'
     end) as segment_lh, 
     count(distinct pr_lh.captain_id) as login_caps,
     sum(login_hrs) as login_hrs,
     sum(idle_hrs) as idle_hrs
from 
    (
    select
        yyyymmdd,
        (case when substr(quarter_hour, 1, 2) >= '06' and substr(quarter_hour,1,2) < '12' then 'Mrng' else 
            case when substr(quarter_hour, 1, 2) >= '12' and substr(quarter_hour,1,2) < '17' then 'Aftn' else
                case when substr(quarter_hour, 1, 2) >= '17' and substr(quarter_hour,1,2) < '22' then 'Evng' else 
                    'Night' 
                end 
            end 
        end) as session,
        captain_id, 
        cast(sum(duration) as double)/3600 as login_hrs,
        cast(sum(case when status = 2 then duration end) as double)/3600 as idle_hrs
    from
        datasets.supplycursory_history
    where
        yyyymmdd = '20230213'
        and status in (2, 3, 6, 7, 8, 10)
        and servicedetailid = '5ef2bc5b85846b775f97d170'
    group by
        1, 2, 3 
    ) pr_lh left join segment_tab
    on pr_lh.captain_id = segment_tab.captain_id
group by 
    1, 2, 3 
)

select
    logs_snap.yyyymmdd, logs_snap.session, segment_snap, login_caps, login_hrs, idle_hrs, 
    gross_captains, total_pings, accept_caps, accepted_pings, net_captains, net_orders
from 
    logs_snap left join imm_tab
    on logs_snap.yyyymmdd = imm_tab.yyyymmdd
    and logs_snap.segment_snap = imm_tab.segment_im
    and logs_snap.session = imm_tab.session
        
    left join log_in_tab
    on logs_snap.yyyymmdd = log_in_tab.yyyymmdd
    and logs_snap.segment_snap = log_in_tab.segment_lh
    and logs_snap.session = log_in_tab.session
order by 
    1, 
    (case when logs_snap.session = 'Mrng' then 1 
        when logs_snap.session = 'Aftn' then 2 
        when logs_snap.session = 'Evng' then 3
        else 4 
    end),
    (case 
        when segment_snap = 'LP' then 1
        when segment_snap = 'MP' then 2
        when segment_snap = 'HP' then 3
        when segment_snap = 'UHP' then 4
    else 
        5
    end)



------

final newquery - 
with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
    and pickup_cluster IN ('Hitech City','Kukatpally 3')
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,orders_multicast as 
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
)


select
    orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
    accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
    accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
    busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
    rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
from 
(
select
    date_disp as orderdate, lm_category, epkm_category, pc_segment, 
    batch_number, mapping_pings, mapping_riders, accept_pings, 
    cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
    round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
    cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
    busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
    rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
from 
(
select
    date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
    (case 
        when batch_number >= 1 and batch_number <= 2 then '_1-2'
        when batch_number >= 3 then '3+'
    else
        'Unbatched'
    end) as batch_number,
    
    count(order_id_disp) as mapping_pings,
    count(distinct rider_id_disp) as mapping_riders,
    
    count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
    approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
    count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
    count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
    count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
from    
    orders_multicast
    
    left join logs_snap
    on orders_multicast.order_id_disp = logs_snap.order_id
    
    left join imm_tab
    on orders_multicast.order_id_disp = imm_tab.order_id
where 
    order_status = 'expired'
    and contains(map_riders_check,'true')
    
group by 
    1, 2, 3, 4, 5 
)
)
where 
    lm_category != 'Blank'
    and epkm_category != 'Blank'
    and pc_segment != 'Blank'
order by 
    1, 
    (case 
        when lm_category = 'A_0-3_Km' then 1
        when lm_category = 'B_3-6_Km' then 2
        when lm_category = 'C_6-9_Km' then 3
        when lm_category = 'D_9+_Km' then 4
    else 
        5
    end),
    (case 
        when epkm_category = 'Good' then 1
        when epkm_category = 'Ok' then 2
    else 
        3
    end),
    (case 
        when pc_segment = 'LP' then 1 
        when pc_segment = 'MP_HP_UHP' then 2
    else 
        4
    end),
    (case 
        when batch_number = '_1-2' then 1 
        when batch_number = '3+' then 2 
    else 
        4
    end)

----
newquery final 2
with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
    and order_status = 'dropped'
    and (spd_fraud_flag = false or spd_fraud_flag is null)
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230213'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230213'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            -- transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.epkm')) as epkm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230213'
            and order_id IN (select distinct order_id from logs_snap)
            -- and order_id = '63e9825622327240115d4b52'
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230213'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
    -- and data_orderid = '63e9825622327240115d4b52'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,num_bt as 
(
select
    data_orderid as d_oid, count(distinct data_propagationbatchid) as num_of_batches
from 
    batch_tab_mcast
group by 
    1 
)


select
    (case 
        when num_of_batches >= 1 and num_of_batches <= 2 then '_1-2'
        when num_of_batches >= 3 then '3+'
    else
        'Unbatched'
    end) as num_of_batches,
    
    count(distinct d_oid) as orders,
    count(distinct data_riderid) as batched_captains,
    count(distinct case when performance_segment = 'LP' then data_riderid end) as LP_Caps,
    count(distinct case when performance_segment = 'MP' then data_riderid end) as MP_Caps,
    count(distinct case when performance_segment = 'HP' then data_riderid end) as HP_Caps,
    count(distinct case when performance_segment = 'UHP' then data_riderid end) as UHP_Caps
from 
    num_bt left join batch_tab_mcast
    on num_bt.d_oid = batch_tab_mcast.data_orderid
    
    left join segment_tab
    on batch_tab_mcast.data_riderid = segment_tab.captain_id
group by 
    1
order by 
    (case 
        when num_of_batches = '_1-2' then 1 
        when num_of_batches = '3+' then 2 
    else 
        4
    end)

    

-- ,orders_multicast as 
-- (
-- select
--     disp_pre.*, 
--     (case 
--         when performance_segment = 'LP' then 'LP'
--         when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
--     else 
--         'Blank'
--      end) as performance_segment
-- from    
--     (
--     select
--         coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
--         coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
--         coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
--         epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
--         -- (case 
--         --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
--         --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
--         --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
--         --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
--         --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
--         --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
--         --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
--         --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
--         --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
--         --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
--         --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
--         --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
--         -- else 
--         --     'Blank'
--         -- end) as fm_lm_category,
        
--         (case 
--             when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
--             when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
--             when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
--             when (last_mile >= 9) then 'D_9+_Km' 
--         else 
--             'E.Blank'
--         end) as lm_category,
--         (case 
--             when epkm_new >= 0 and epkm_new < 20 then 'Ok'
--             when epkm_new >= 20 then 'Good'
--         else 
--             'Blank'
--         end) as epkm_category, first_mile
--     from    
--         caps_disp_tab full outer join batch_tab_mcast
--     on 
--         caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
--         and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
--         and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
--         and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
--     ) disp_pre 
--     left join segment_tab
--     on 
--         disp_pre.date_disp = segment_tab.yyyymmdd
--         and disp_pre.rider_id_disp = segment_tab.captain_id
-- )


-- -- select
-- --     orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
-- --     accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
-- --     accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
-- --     busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
-- --     rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
-- -- from 
-- -- (
-- -- select
-- --     date_disp as orderdate, lm_category, epkm_category, pc_segment, 
-- --     batch_number, mapping_pings, mapping_riders, accept_pings, 
-- --     cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
-- --     round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
-- --     cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
-- --     busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
-- --     rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
-- -- from 
-- -- (
-- select * from (
-- select
--     date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
--     (case 
--         when batch_number >= 1 and batch_number <= 2 then '_1-2'
--         when batch_number >= 3 then '3+'
--     else
--         'Unbatched'
--     end) as batch_number,
    
--     -- count(order_id_disp) as mapping_pings,
--     count(distinct rider_id_disp) as mapping_riders,
    
--     count(distinct case when contains(eventtype,'rider_accepted') then rider_id_disp end) as accept_caps,
    
--     -- approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
--     count(distinct case when contains(eventtype,'rider_accepted') or contains(eventtype,'rider_busy') or contains(eventtype,'rider_rejected') then
--     rider_id_disp end) as gross_caps,
    
--     count(distinct case when contains(eventtype,'rider_accepted') or contains(eventtype,'rider_busy') or contains(eventtype,'rider_rejected')
--     or contains(eventtype,'rider_accept_failed') then rider_id_disp end) as gross_intent_caps,
    
--     count(distinct case when contains(eventtype,'rider_accept_failed') then rider_id_disp end) as accept_failed_caps,
    
--     count(distinct case when contains(eventtype,'rider_busy') then rider_id_disp end) as busy_caps,
    
--     count(distinct case when contains(eventtype,'rider_rejected') then rider_id_disp end) as rejected_caps
-- from    
--     orders_multicast
    
--     left join logs_snap
--     on orders_multicast.order_id_disp = logs_snap.order_id
    
--     left join imm_tab
--     on orders_multicast.order_id_disp = imm_tab.order_id
    
-- group by 
--     1, 2, 3, 4, 5 
-- -- )
-- )
-- where 
--     lm_category != 'Blank'
--     and epkm_category != 'Blank'
--     and pc_segment != 'Blank'
-- order by 
--     1, 
--     (case 
--         when lm_category = 'A_0-3_Km' then 1
--         when lm_category = 'B_3-6_Km' then 2
--         when lm_category = 'C_6-9_Km' then 3
--         when lm_category = 'D_9+_Km' then 4
--     else 
--         5
--     end),
--     (case 
--         when epkm_category = 'Good' then 1
--         when epkm_category = 'Ok' then 2
--     else 
--         3
--     end),
--     (case 
--         when pc_segment = 'LP' then 1 
--         when pc_segment = 'MP_HP_UHP' then 2
--     else 
--         4
--     end),
--     (case 
--         when batch_number = '_1-2' then 1 
--         when batch_number = '3+' then 2 
--     else 
--         4
--     end)
----

newquery
with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230215'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
    and order_id = '63ebd3a32f0d965b7157ef25'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230215'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230215'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230215'
            and order_id IN (select distinct order_id from logs_snap)
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230215'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,num_bt as 
(
select
    data_orderid as d_oid, count(distinct data_propagationbatchid) as num_of_batches
from 
    batch_tab_mcast
group by 
    1 
)

,pc_v2_tab as 
(
select 
    *
from
    experiments.cap_pc_v2_auto
where 
    yyyymmdd = '20230215'
    and city = 'Hyderabad'
)


-- select
--     performance, consistency, count(order_id_disp) as mapping_pings
-- from
-- (
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment, num_of_batches,
        
        performance, consistency, potential, last_45d_peak_performance, location_affinity, tod_affinity, lm_affinity, payment_affinity, 
        payment_affinity_morning_peak, payment_affinity_evening_peak, intra_cluster_affinity, intra_cluster_affinity_morning_peak, intra_cluster_affinity_evening_peak
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
        
        left join pc_v2_tab
        on disp_pre.rider_id_disp = pc_v2_tab.captain_id
        
        left join num_bt
        on disp_pre.order_id_disp = num_bt.d_oid

-- ) where lm_category = 'A_0-3_Km'
-- and performance is not null and consistency is not null 
-- group by 1, 2 
-- select
--     orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
--     accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
--     accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
--     busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
--     rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
-- from 
-- (
-- select
--     date_disp as orderdate, lm_category, epkm_category, pc_segment, 
--     batch_number, mapping_pings, mapping_riders, accept_pings, 
--     cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
--     round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
--     cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
--     busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
--     rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
-- from 
-- (
-- select
--     date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
--     (case 
--         when batch_number >= 1 and batch_number <= 2 then '_1-2'
--         when batch_number >= 3 then '3+'
--     else
--         'Unbatched'
--     end) as batch_number,
    
--     count(order_id_disp) as mapping_pings,
--     count(distinct rider_id_disp) as mapping_riders,
    
--     count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
--     approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
--     count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
--     count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
--     count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
-- from    
--     orders_multicast
    
--     left join logs_snap
--     on orders_multicast.order_id_disp = logs_snap.order_id
    
--     left join imm_tab
--     on orders_multicast.order_id_disp = imm_tab.order_id
-- where 
--     order_status = 'expired'
--     and contains(map_riders_check,'true')
    
-- group by 
--     1, 2, 3, 4, 5 
-- )
-- )
-- where 
--     lm_category != 'Blank'
--     and epkm_category != 'Blank'
--     and pc_segment != 'Blank'
-- order by 
--     1, 
--     (case 
--         when lm_category = 'A_0-3_Km' then 1
--         when lm_category = 'B_3-6_Km' then 2
--         when lm_category = 'C_6-9_Km' then 3
--         when lm_category = 'D_9+_Km' then 4
--     else 
--         5
--     end),
--     (case 
--         when epkm_category = 'Good' then 1
--         when epkm_category = 'Ok' then 2
--     else 
--         3
--     end),
--     (case 
--         when pc_segment = 'LP' then 1 
--         when pc_segment = 'MP_HP_UHP' then 2
--     else 
--         4
--     end),
--     (case 
--         when batch_number = '_1-2' then 1 
--         when batch_number = '3+' then 2 
--     else 
--         4
--     end)
--
newquery
with logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230215'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
    -- and order_id = '63ebd3a32f0d965b7157ef25'
)

,imm_tab as 
(
select 
    order_id, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230215'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
group by 
    1
)


,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230215'
    and lower(city_name) = 'hyderabad'
)

,caps_disp_tab as 
(
select
    *, 
    row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
from
    (
    select
        pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
        earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
    from 
        (
        select 
            city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
        from
        (
        select 
            city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
            transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
        from
            datasets.idispatch_api_response_generic_prioritizer
        where 
            yyyymmdd = '20230215'
            and order_id IN (select distinct order_id from logs_snap)
        ) 
        cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
        ) pre_tab left join logs_snap
        on pre_tab.order_id = logs_snap.order_id
    )
)

,raw_batch as 
(
select
    data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
from 
    raw.kafka_dispatch_propagation_immutable
where 
    yyyymmdd = '20230215'
    and data_orderid IN (select distinct order_id from logs_snap)
    and data_propagationbatchid is not null 
    and data_propagationtype = 'multicast'
)

,batch_tab_mcast as 
(
select
    batch_one.*, batch_number, 
    row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
from 
    (
    select 
        data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
        array_agg(distinct eventtype) as eventtype, 
        max(updated_epoch) as updated_epoch
    from 
        raw_batch
    where 
        data_riderid is not null 
    group by 
        1, 2, 3, 4 
    ) batch_one 
    left join 
    (
    select
        data_orderid, data_propagationbatchid, 
        row_number() over(partition by data_orderid order by updated_epoch) as batch_number
    from 
        raw_batch
    where 
        eventtype = 'propagation_initiated_for_batch'
    ) batch_two 
    on 
        batch_one.data_orderid = batch_two.data_orderid
        and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
)

,num_bt as 
(
select
    data_orderid as d_oid, count(distinct data_propagationbatchid) as num_of_batches
from 
    batch_tab_mcast
group by 
    1 
)

,pc_v2_tab as 
(
select 
    *
from
    experiments.cap_pc_v2_auto
where 
    yyyymmdd = '20230215'
    and city = 'Hyderabad'
)


select
    performance, payment_affinity, count(order_id_disp) as mapping_pings, 
    count(distinct rider_id_disp) as captains
from
(
select
    disp_pre.*, 
    (case 
        when performance_segment = 'LP' then 'LP'
        when performance_segment IN ('MP','HP','UHP') then 'MP_HP_UHP'
    else 
        'Blank'
     end) as performance_segment, num_of_batches,
        
        performance, consistency, potential, last_45d_peak_performance, location_affinity, tod_affinity, lm_affinity, payment_affinity, 
        payment_affinity_morning_peak, payment_affinity_evening_peak, intra_cluster_affinity, intra_cluster_affinity_morning_peak, intra_cluster_affinity_evening_peak
from    
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        epoch_timestamp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, batch_number,
        -- (case 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
        --     when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
        --     when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
        --     when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
        --     when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
        --     when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
        --     when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
        -- else 
        --     'Blank'
        -- end) as fm_lm_category,
        
        (case 
            when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
            when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 9) then 'D_9+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 20 then 'Ok'
            when epkm_new >= 20 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_pre 
    left join segment_tab
    on 
        disp_pre.date_disp = segment_tab.yyyymmdd
        and disp_pre.rider_id_disp = segment_tab.captain_id
        
        left join pc_v2_tab
        on disp_pre.rider_id_disp = pc_v2_tab.captain_id
        
        left join num_bt
        on disp_pre.order_id_disp = num_bt.d_oid

) where lm_category = 'A_0-3_Km'
and performance is not null and consistency is not null 
and num_of_batches >= 3
group by 1, 2 
-- select
--     orderdate, lm_category, epkm_category, pc_segment, batch_number, mapping_pings, mapping_riders, 
--     accept_pings, (case when is_nan(APR) then null else APR end) as APR, median_fm_kms, 
--     accept_failed_pings, (case when is_nan(New_APR) then null else New_APR end) as New_APR, 
--     busy_pings, (case when is_nan(busy_pings_perc) then null else busy_pings_perc end) as busy_pings_perc, 
--     rejected_pings, (case when is_nan(reject_pings_perc) then null else reject_pings_perc end) as reject_pings_perc
-- from 
-- (
-- select
--     date_disp as orderdate, lm_category, epkm_category, pc_segment, 
--     batch_number, mapping_pings, mapping_riders, accept_pings, 
--     cast(accept_pings as double)/(accept_pings + busy_pings + rejected_pings) as APR,
--     round(median_fm_kms,2) as median_fm_kms, accept_failed_pings, 
--     cast((accept_pings + accept_failed_pings) as double)/(accept_pings + accept_failed_pings + busy_pings + rejected_pings) as New_APR,
--     busy_pings, cast(busy_pings as double)/(accept_pings + busy_pings + rejected_pings) as busy_pings_perc,
--     rejected_pings, cast(rejected_pings as double)/(accept_pings + busy_pings + rejected_pings) as reject_pings_perc
-- from 
-- (
-- select
--     date_disp, lm_category, epkm_category, performance_segment as pc_segment, 
--     (case 
--         when batch_number >= 1 and batch_number <= 2 then '_1-2'
--         when batch_number >= 3 then '3+'
--     else
--         'Unbatched'
--     end) as batch_number,
    
--     count(order_id_disp) as mapping_pings,
--     count(distinct rider_id_disp) as mapping_riders,
    
--     count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
--     approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
--     count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
--     count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
--     count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings
-- from    
--     orders_multicast
    
--     left join logs_snap
--     on orders_multicast.order_id_disp = logs_snap.order_id
    
--     left join imm_tab
--     on orders_multicast.order_id_disp = imm_tab.order_id
-- where 
--     order_status = 'expired'
--     and contains(map_riders_check,'true')
    
-- group by 
--     1, 2, 3, 4, 5 
-- )
-- )
-- where 
--     lm_category != 'Blank'
--     and epkm_category != 'Blank'
--     and pc_segment != 'Blank'
-- order by 
--     1, 
--     (case 
--         when lm_category = 'A_0-3_Km' then 1
--         when lm_category = 'B_3-6_Km' then 2
--         when lm_category = 'C_6-9_Km' then 3
--         when lm_category = 'D_9+_Km' then 4
--     else 
--         5
--     end),
--     (case 
--         when epkm_category = 'Good' then 1
--         when epkm_category = 'Ok' then 2
--     else 
--         3
--     end),
--     (case 
--         when pc_segment = 'LP' then 1 
--         when pc_segment = 'MP_HP_UHP' then 2
--     else 
--         4
--     end),
--     (case 
--         when batch_number = '_1-2' then 1 
--         when batch_number = '3+' then 2 
--     else 
--         4
--     end)
----
