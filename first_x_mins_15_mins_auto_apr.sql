new first x mins
with clstr_ctgry_tab as 
(
select 
    distinct hotness_category, pickupclusters, temporal_id, cluster, hex_id
from
    (
    select 
        hotness_category, lower(pickupclusters) as pickupclusters, temporal_id 
    from 
        pricing.cluster_hotness_category
    where 
        run_date = '20230221'
        and service_detail_id = '5ef2bc5b85846b775f97d170' 
    ) cluster_stress
left join 
    (
    select 
        hex_id, city, lower(cluster) as cluster
    from 
        datasets.city_cluster_hex
    where 
        resolution = 8
    ) hex_cluster 
on 
    cluster_stress.pickupclusters = hex_cluster.cluster
)

,raw_login as 
(
select
    yyyymmdd, captain_id, epoch, location, quarter_hour, hhmm
from
    datasets.supplycursory_history
where
    yyyymmdd = '20230215'
    and status in (2, 3, 6, 7, 8, 10)
    and servicedetailid = '5ef2bc5b85846b775f97d170'
)

-- ,login_tab as 
-- (
select
    sch_tab.yyyymmdd, sch_tab.captain_id, first_login_hex8, sch_tab.epoch, fl_epoch_15mins, hotness_category,
    exp_pc_v2.performance as pc_segment, tod_affinity, lm_affinity, 
    (case when cp_id is null then 'Offline' else 'Online' end) as Login_Tag_in_15mins
from
    (
    select
        yyyymmdd, captain_id, location as first_login_hex8, from_unixtime(epoch / 1000, 'Asia/Kolkata') as epoch, 
        from_unixtime((epoch + 900000) / 1000, 'Asia/Kolkata') as fl_epoch_15mins,
        concat(date_format(date_parse(yyyymmdd,'%Y%m%d'),'%W'),'-',substr(quarter_hour,1,2),'00') as login_temporal_id
    from
        (
        select
            yyyymmdd, captain_id, epoch, location, quarter_hour,
            row_number() over(partition by yyyymmdd, captain_id order by epoch) as latest_info
        from
            raw_login
        ) 
    where 
        latest_info = 1 
    ) sch_tab 
    
    left join clstr_ctgry_tab
    on sch_tab.first_login_hex8 = clstr_ctgry_tab.hex_id
    and sch_tab.login_temporal_id = clstr_ctgry_tab.temporal_id

    left join 
    (select * from experiments.cap_pc_v2_auto where city = 'Hyderabad') exp_pc_v2
    on sch_tab.captain_id = exp_pc_v2.captain_id
    
    left join (select distinct yyyymmdd, captain_id as cp_id, hhmm from raw_login) raw_lh_tab
    on sch_tab.yyyymmdd = raw_lh_tab.yyyymmdd
    and sch_tab.captain_id = raw_lh_tab.cp_id
    and date_format(sch_tab.fl_epoch_15mins,'%H%i') = raw_lh_tab.hhmm
-- )

-- ,logs_snap as 
-- (
-- select 
--     yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
--     order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag
-- from 
--     orders.order_logs_snapshot
-- where 
--     yyyymmdd = '20230215'
--     and service_obj_city_display_name = 'Hyderabad'
--     and service_obj_service_name = 'Auto'
--     -- and order_id = '63ec47695d8f0725c6c550a8'
-- )

-- ,caps_disp_tab as 
-- (
-- select
--     *, 
--     row_number() over(partition by order_id, rider_id order by epoch_timestamp) as cap_row_num
-- from
--     (
--     select
--         pre_tab.order_id, pre_tab.yyyymmdd, epoch_timestamp, rider_id, cast(first_mile as double)/1000 as first_mile, lm_main as last_mile, 
--         earn_amt, cast(earn_amt as double)/(cast(first_mile as double)/1000 + lm_main) as epkm_new
--     from 
--         (
--         select 
--             city_name, service_category, yyyymmdd, order_id, epoch_timestamp, rider_id, first_mile, earn_amt 
--         from
--         (
--         select 
--             city_name, service_category, order_id, yyyymmdd, service_detail_id, epoch_timestamp,
            
--             transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.rider_id')) as rider_list,
--             transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.first_mile_distance')) as fm_list,
--             transform(cast(captains as array<json>), x->json_extract_scalar(x,'$.earnings.amount')) as earning_list
--         from
--             datasets.idispatch_api_response_generic_prioritizer
--         where 
--             yyyymmdd = '20230215'
--             and order_id IN (select distinct order_id from logs_snap)
--             -- and order_id = '63ec47695d8f0725c6c550a8'
--         ) 
--         cross join unnest(rider_list, fm_list, earning_list) as t(rider_id, first_mile, earn_amt)
--         ) pre_tab left join logs_snap
--         on pre_tab.order_id = logs_snap.order_id
--     )
-- )

-- ,raw_batch as 
-- (
-- select
--     data_orderid, yyyymmdd as data_yyyymmdd, updated_epoch, data_propagationbatchid, data_riderid, eventtype, data_propagationtype
-- from 
--     raw.kafka_dispatch_propagation_immutable
-- where 
--     yyyymmdd = '20230215'
--     and data_orderid IN (select distinct order_id from logs_snap)
--     and data_propagationbatchid is not null 
--     and data_propagationtype = 'multicast'
-- )

-- ,batch_tab_mcast as 
-- (
-- select
--     batch_one.*, batch_number, 
--     row_number() over(partition by batch_one.data_orderid, data_riderid order by updated_epoch) as row_nmbr_batch
-- from 
--     (
--     select 
--         data_yyyymmdd, data_orderid, data_propagationbatchid, data_riderid, 
--         array_agg(distinct eventtype) as eventtype, 
--         max(updated_epoch) as updated_epoch
--     from 
--         raw_batch
--     where 
--         data_riderid is not null 
--     group by 
--         1, 2, 3, 4 
--     ) batch_one 
--     left join 
--     (
--     select
--         data_orderid, data_propagationbatchid, 
--         row_number() over(partition by data_orderid order by updated_epoch) as batch_number
--     from 
--         raw_batch
--     where 
--         eventtype = 'propagation_initiated_for_batch'
--     ) batch_two 
--     on 
--         batch_one.data_orderid = batch_two.data_orderid
--         and batch_one.data_propagationbatchid = batch_two.data_propagationbatchid
-- )

-- -- ,num_bt as 
-- -- (
-- -- select
-- --     data_orderid as d_oid, count(distinct data_propagationbatchid) as num_of_batches
-- -- from 
-- --     batch_tab_mcast
-- -- group by 
-- --     1 
-- -- )

-- ,dispatch_batch as 
-- (
-- select
--     coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
--     coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
--     coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
--     from_unixtime(cast(substr(epoch_timestamp,1,13) as bigint) / 1000, 'Asia/Kolkata') as epoch_timestamp, 
--     epkm_new, data_propagationbatchid, eventtype, 
--     from_unixtime(updated_epoch / 1000, 'Asia/Kolkata') as updated_epoch, batch_number,
--     (case 
--         when (first_mile >= 0 and first_mile < 1) and (last_mile >= 0 and last_mile < 1) then 'Ok' 
--         when (first_mile >= 0 and first_mile < 0.5) and (last_mile >= 1 and last_mile < 2) then 'Good' 
--         when (first_mile >= 0.5 and first_mile < 1.5) and (last_mile >= 1 and last_mile < 2) then 'Ok' 
--         when (first_mile >= 0 and first_mile < 1) and (last_mile >= 2 and last_mile < 3) then 'Good' 
--         when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 0 and last_mile < 1) then 'Bad' 
--         when (first_mile >= 1 and first_mile < 1.5) and (last_mile >= 2 and last_mile < 3) then 'Ok' 
--         when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 0 and last_mile < 3) then 'Bad'
--         when (first_mile >= 2) and (last_mile >= 0 and last_mile < 6) then 'Bad' 
--         when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 3 and last_mile < 5) then 'Ok' 
--         when (first_mile >= 1.5 and first_mile < 2) and (last_mile >= 5) then 'Good' 
--         when (first_mile >= 2) and (last_mile >= 6) then 'Ok' 
--         when (first_mile >= 0 and first_mile < 1.5) and (last_mile >= 3) then 'Good' 
--     else 
--         'Blank'
--     end) as fm_lm_category,
    
--     (case 
--         when (last_mile >= 0 and last_mile < 3) then 'A_0-3_Km' 
--         when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
--         when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
--         when (last_mile >= 9) then 'D_9+_Km' 
--     else 
--         'E.Blank'
--     end) as lm_category,
--     (case 
--         when epkm_new >= 0 and epkm_new < 18 then 'Bad'
--         when epkm_new >= 18 then 'Good'
--     else 
--         'Blank'
--     end) as epkm_category, first_mile
-- from    
--     caps_disp_tab full outer join batch_tab_mcast
-- on 
--     caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
--     and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
--     and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
--     and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
-- )

-- select
--     yyyymmdd, 
--     (case when date_format(epoch,'%H') >= '06' and date_format(epoch,'%H') < '12' then 'Mrng' else 
--         case when date_format(epoch,'%H') >= '12' and date_format(epoch,'%H') < '17' then 'Aftn' else
--             case when date_format(epoch,'%H') >= '17' and date_format(epoch,'%H') < '22' then 'Evng' else 
--                 'Night' 
--             end 
--         end 
--     end) as session,
    
--     hotness_category as cap_login_hotness, 
    
--     (case 
--         when pc_segment = 'LP' then 'LP'
--     else 
--         'MP_HP_UHP'
--      end) as pc_segment, 
--     lm_category, epkm_category,  
    
--     count(distinct captain_id) as first_login_caps,
    
--     count(distinct case when data_propagationbatchid is not null then captain_id end) as batched_caps,
    
--     count(distinct case when contains(eventtype,'rider_accepted') or contains(eventtype,'rider_busy') 
--     or contains(eventtype,'rider_rejected') then captain_id end) as ping_rcvd_caps,
    
--     count(distinct case when not contains(eventtype,'rider_accepted') and not contains(eventtype,'rider_busy') 
--     and not contains(eventtype,'rider_rejected') and contains(eventtype,'rider_accept_failed') then captain_id end) as acc_failed_caps,
    
--     count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
--     approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
--     count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
--     count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
--     count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings,
    
--     count(distinct case when Login_Tag_in_15mins = 'Offline' then captain_id end) as offline_15mins
    
--     -- tod_affinity, lm_affinity, 
-- from 
--     (
--     select 
--         login_tab.*, order_id_disp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, 
--         batch_number, fm_lm_category, epkm_category, lm_category, first_mile
--         -- , num_of_batches
--     from 
--         login_tab left join dispatch_batch
--     on
--         login_tab.yyyymmdd = dispatch_batch.date_disp
--         and login_tab.captain_id = dispatch_batch.rider_id_disp
--         and login_tab.epoch <= dispatch_batch.updated_epoch
--         and login_tab.fl_epoch_15mins >= dispatch_batch.updated_epoch
--     )
--     -- left join num_bt
--     -- on dispatch_batch.order_id_disp = num_bt.d_oid

-- group by 
--     1, 2, 3, 4, 5, 6 
-- order by 
--     1, 
--     (case when session = 'Mrng' then 1 
--         when session = 'Aftn' then 2 
--         when session = 'Evng' then 
--         else 4 
--     end), 
--     3, 
    
--         (case 
--         when pc_segment = 'LP' then 1 
--         when pc_segment = 'MP_HP_UHP' then 2
--     else 
--         4
--     end),
    
    
-- (case 
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
--     end)



limit 10 

---

newquery - lm 
with clstr_ctgry_tab as 
(
select 
    distinct hotness_category, pickupclusters, temporal_id, cluster, hex_id
from
    (
    select 
        hotness_category, lower(pickupclusters) as pickupclusters, temporal_id 
    from 
        pricing.cluster_hotness_category
    where 
        run_date = '20230221'
        and service_detail_id = '5ef2bc5b85846b775f97d170' 
    ) cluster_stress
left join 
    (
    select 
        hex_id, city, lower(cluster) as cluster
    from 
        datasets.city_cluster_hex
    where 
        resolution = 8
    ) hex_cluster 
on 
    cluster_stress.pickupclusters = hex_cluster.cluster
)

,raw_login as 
(
select
    yyyymmdd, captain_id, epoch, location, quarter_hour, hhmm
from
    datasets.supplycursory_history
where
    yyyymmdd = '20230215'
    and status in (2, 3, 6, 7, 8, 10)
    and servicedetailid = '5ef2bc5b85846b775f97d170'
)

,login_tab as 
(
select
    sch_tab.yyyymmdd, sch_tab.captain_id, first_login_hex8, sch_tab.epoch, fl_epoch_15mins, hotness_category,
    exp_pc_v2.performance as pc_segment, tod_affinity, lm_affinity, 
    (case when cp_id is null then 'Offline' else 'Online' end) as Login_Tag_in_15mins
from
    (
    select
        yyyymmdd, captain_id, location as first_login_hex8, from_unixtime(epoch / 1000, 'Asia/Kolkata') as epoch, 
        from_unixtime((epoch + 900000) / 1000, 'Asia/Kolkata') as fl_epoch_15mins,
        concat(date_format(date_parse(yyyymmdd,'%Y%m%d'),'%W'),'-',substr(quarter_hour,1,2),'00') as login_temporal_id
    from
        (
        select
            yyyymmdd, captain_id, epoch, location, quarter_hour,
            row_number() over(partition by yyyymmdd, captain_id order by epoch) as latest_info
        from
            raw_login
        ) 
    where 
        latest_info = 1 
    ) sch_tab 
    
    left join clstr_ctgry_tab
    on sch_tab.first_login_hex8 = clstr_ctgry_tab.hex_id
    and sch_tab.login_temporal_id = clstr_ctgry_tab.temporal_id

    left join 
    (select * from experiments.cap_pc_v2_auto where city = 'Hyderabad') exp_pc_v2
    on sch_tab.captain_id = exp_pc_v2.captain_id
    
    left join (select distinct yyyymmdd, captain_id as cp_id, hhmm from raw_login) raw_lh_tab
    on sch_tab.yyyymmdd = raw_lh_tab.yyyymmdd
    and sch_tab.captain_id = raw_lh_tab.cp_id
    and date_format(sch_tab.fl_epoch_15mins,'%H%i') = raw_lh_tab.hhmm 
)

,logs_snap as 
(
select 
    yyyymmdd, service_obj_service_name as service, service_obj_city_display_name as city, 
    order_id, distance_final_distance as lm_main, order_status, spd_fraud_flag, sub_total
from 
    orders.order_logs_snapshot
where 
    yyyymmdd = '20230215'
    and service_obj_city_display_name = 'Hyderabad'
    and service_obj_service_name = 'Auto'
    -- and order_id = '63ec47695d8f0725c6c550a8'
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
            -- and order_id = '63ec47695d8f0725c6c550a8'
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

-- ,num_bt as 
-- (
-- select
--     data_orderid as d_oid, count(distinct data_propagationbatchid) as num_of_batches
-- from 
--     batch_tab_mcast
-- group by 
--     1 
-- )

,dispatch_batch as 
(
select
    disp_new.*, 
    cast(sub_total as double)/(first_mile + last_mile) as pp_km
from 
    (
    select
        coalesce(caps_disp_tab.yyyymmdd, batch_tab_mcast.data_yyyymmdd) as date_disp,
        coalesce(caps_disp_tab.order_id, batch_tab_mcast.data_orderid) as order_id_disp,
        coalesce(caps_disp_tab.rider_id, batch_tab_mcast.data_riderid) as rider_id_disp,
        from_unixtime(cast(substr(epoch_timestamp,1,13) as bigint) / 1000, 'Asia/Kolkata') as epoch_timestamp, 
        epkm_new, data_propagationbatchid, eventtype, 
        from_unixtime(updated_epoch / 1000, 'Asia/Kolkata') as updated_epoch, batch_number,
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
            when (last_mile >= 0 and last_mile < 4) then 'A_0-4_Km' 
            -- when (last_mile >= 3 and last_mile < 6) then 'B_3-6_Km' 
            -- when (last_mile >= 6 and last_mile < 9) then 'C_6-9_Km'
            when (last_mile >= 4) then 'D_4+_Km' 
        else 
            'E.Blank'
        end) as lm_category,
        (case 
            when epkm_new >= 0 and epkm_new < 19 then 'Bad'
            when epkm_new >= 19 then 'Good'
        else 
            'Blank'
        end) as epkm_category, first_mile, last_mile
    from    
        caps_disp_tab full outer join batch_tab_mcast
    on 
        caps_disp_tab.yyyymmdd = batch_tab_mcast.data_yyyymmdd
        and caps_disp_tab.order_id = batch_tab_mcast.data_orderid
        and caps_disp_tab.rider_id = batch_tab_mcast.data_riderid
        and caps_disp_tab.cap_row_num = batch_tab_mcast.row_nmbr_batch
    ) disp_new left join logs_snap
    on disp_new.order_id_disp = logs_snap.order_id
)

-- select
--     yyyymmdd, 
--     (case when date_format(epoch,'%H') >= '06' and date_format(epoch,'%H') < '12' then 'Mrng' else 
--         case when date_format(epoch,'%H') >= '12' and date_format(epoch,'%H') < '17' then 'Aftn' else
--             case when date_format(epoch,'%H') >= '17' and date_format(epoch,'%H') < '22' then 'Evng' else 
--                 'Night' 
--             end 
--         end 
--     end) as session,
    
--     hotness_category as cap_login_hotness, 
    
--     (case 
--         when pc_segment = 'LP' then 'LP'
--     else 
--         'MP_HP_UHP'
--      end) as pc_segment, 
--     lm_category, epkm_category,  
    
--     count(distinct captain_id) as first_login_caps,
    
--     count(distinct case when data_propagationbatchid is not null then captain_id end) as batched_caps,
    
--     count(distinct case when contains(eventtype,'rider_accepted') or contains(eventtype,'rider_busy') 
--     or contains(eventtype,'rider_rejected') then captain_id end) as ping_rcvd_caps,
    
--     count(distinct case when not contains(eventtype,'rider_accepted') and not contains(eventtype,'rider_busy') 
--     and not contains(eventtype,'rider_rejected') and contains(eventtype,'rider_accept_failed') then captain_id end) as acc_failed_caps,
    
--     count(case when contains(eventtype,'rider_accepted') then order_id_disp end) as accept_pings,
    
--     approx_percentile(coalesce(first_mile,0),0.5) as median_fm_kms,
    
--     count(case when contains(eventtype,'rider_accept_failed') then order_id_disp end) as accept_failed_pings,
--     count(case when contains(eventtype,'rider_busy') then order_id_disp end) as busy_pings,
--     count(case when contains(eventtype,'rider_rejected') then order_id_disp end) as rejected_pings,
    
--     count(distinct case when Login_Tag_in_15mins = 'Offline' then captain_id end) as offline_15mins
    
--     -- tod_affinity, lm_affinity, 
-- from 
--     (
,caps_dump_once as 
(
select
    yyyymmdd, captain_id, first_login_hex8, epoch, fl_epoch_15mins, hotness_category, 
    pc_segment, tod_affinity, lm_affinity, Login_Tag_in_15mins, 
    avg(last_mile) as last_mile
from 
    (
    select 
        login_tab.*, order_id_disp, epkm_new, data_propagationbatchid, eventtype, updated_epoch, 
        batch_number, fm_lm_category, epkm_category, lm_category, first_mile, pp_km, last_mile
        -- , num_of_batches
    from 
        login_tab left join dispatch_batch
    on
        login_tab.yyyymmdd = dispatch_batch.date_disp
        and login_tab.captain_id = dispatch_batch.rider_id_disp
        and login_tab.epoch <= dispatch_batch.updated_epoch
        and login_tab.fl_epoch_15mins >= dispatch_batch.updated_epoch
    )
where   
    last_mile is not null 
group by 
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10 
)

,caps_dump_next_week as 
(
    select  
        captainid, 
        -- sum(total_login_hr) as total_login_hr, 
       coalesce(sum(riderbusy_pings),0) as riderbusy_pings, coalesce(sum(riderrejected_pings),0) as riderrejected_pings, 
        coalesce(sum(accepted_pings),0) as accepted_pings, coalesce(sum(net_orders),0) as net_orders, 
        sum(ocara_customer_cancelled) as ocara_cc, sum(ocara_rider_cancelled) as ocara_rc, 
        (sum(riderbusy_pings) + sum(riderrejected_pings) + sum(accepted_pings)) as total_pings, 
        max(case when accepted_pings > 0 or riderbusy_pings > 0 or riderrejected_pings > 0 then yyyymmdd end) as last_ping_date, 
        count(distinct case when accepted_pings > 0 or riderbusy_pings > 0 or riderrejected_pings > 0 then yyyymmdd end) as ping_receive_days, 
        count(distinct case when net_orders > 0 then yyyymmdd end) as net_work_days, 
        avg(case when avg_rating > 0 then avg_rating end) as avg_rating
    from 
        datasets.captain_svo_daily_kpi
    where   
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and  yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and service_name = 'Auto'
        and city = 'Hyderabad'
    group by 
        1
    )
    
    
    select
        (case
            when last_mile <= 4 then '1.<4 km' 
            when last_mile > 4 then '2.>4 km' 
        else 
            'Check'
        end) as last_mile_tag, 
        count(distinct captain_id) as first_login_caps_15thFeb, 
        sum(accepted_pings) as accepted_pings, 
        count(distinct case when accepted_pings > 0 then captain_id end) as accept_caps,
        sum(accepted_pings + riderbusy_pings + riderrejected_pings) as total_pings, 
        count(distinct case when accepted_pings > 0 or riderbusy_pings > 0 or riderrejected_pings > 0 then captain_id end) as gross_caps,
        
        avg(ping_receive_days) as ping_receive_days,
        
        sum(net_orders) as net_orders,
        count(distinct case when net_orders > 0 then captain_id end) as net_caps,
        avg(net_work_days) as net_work_days
        
    from 
        caps_dump_once left join caps_dump_next_week
    on 
        caps_dump_once.captain_id = caps_dump_next_week.captainid
    
    group by 1 


    --     )
--     -- left join num_bt
--     -- on dispatch_batch.order_id_disp = num_bt.d_oid

-- group by 
--     1, 2, 3, 4, 5, 6 
-- order by 
--     1, 
--     (case when session = 'Mrng' then 1 
--         when session = 'Aftn' then 2 
--         when session = 'Evng' then 
--         else 4 
--     end), 
--     3, 
    
--         (case 
--         when pc_segment = 'LP' then 1 
--         when pc_segment = 'MP_HP_UHP' then 2
--     else 
--         4
--     end),
    
    
-- (case 
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
--     end)



limit 10  