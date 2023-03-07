with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230205'
    and lower(city_name) = 'bangalore'
)

,summ_tab as 
(
select
    logs_snapshot.yyyymmdd, logs_snapshot.city, coalesce(performance_segment,'UNKNWN') as segment, 
    count(distinct order_id) as net_orders, 
    count(distinct logs_snapshot.captain_id) as net_captains
from 
    logs_snapshot left join segment_tab
on 
    logs_snapshot.yyyymmdd = segment_tab.yyyymmdd
    and lower(logs_snapshot.city) = segment_tab.city_name
    and logs_snapshot.captain_id = segment_tab.captain_id
where 
    order_status = 'dropped'
    and (spd_fraud_flag = false or spd_fraud_flag is null)
group by 
    1, 2, 3 
)

,expired_cc_tab as
(
select 
    yyyymmdd as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, 
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch, drop_cap
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop, captain_id as drop_cap
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,possible_ords as 
(
select 
    ord_date_expcc, city_expcc, coalesce(performance_segment,'UNKNWN') as segment_1, 
    count(distinct ord_expcc) as possible_exp_and_cc,
    count(distinct drop_cap) as possible_exp_and_cc_caps,
    
    count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
    
    count(distinct case when order_status = 'expired' then drop_cap end) as possible_expired_caps,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then drop_cap end) as possible_expired_mapped_caps,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then drop_cap end) as possible_expired_unmapped_caps,
    
    count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
    
    count(distinct case when order_status = 'customerCancelled' then drop_cap end) as possible_cc_orders_caps,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then drop_cap end) as possible_cobrm_caps,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then drop_cap end) as possible_cobra_caps,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then drop_cap end) as possible_ocara_caps
from 
    (
    select  
        ord_date_expcc, city_expcc, service_expcc, ord_expcc, order_status, 
        map_riders_check, ord_drop, diff_distance, drop_cap, events_list,
        row_number() over(partition by ord_drop order by diff_distance) as drop_row_first,
        row_number() over(partition by ord_expcc order by diff_distance) as exp_row_sec
    from 
        (
         select 
            expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, drop_cap,
            ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), 
            to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
            (case 
                when city_expcc = 'Indore' then 1300
                when city_expcc IN ('Chennai','Delhi') then 1500
                when city_expcc = 'Mumbai' then 2500
                else 2000
            end) as auto_fm_limit
        from
            expired_cc_tab left join dropped_tab on
            expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
            and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
            and expired_cc_tab.city_expcc = dropped_tab.city_drop
            and expired_cc_tab.service_expcc = dropped_tab.service_drop
        )
    where  
        auto_fm_limit >= diff_distance
        and ord_drop is not null
    ) rw_extn left join segment_tab
    on 
        rw_extn.ord_date_expcc = segment_tab.yyyymmdd
        and lower(rw_extn.city_expcc) = segment_tab.city_name
        and rw_extn.drop_cap = segment_tab.captain_id
where 
    exp_row_sec = 1
    and drop_row_first = 1
group by 
    1, 2, 3 
)

select
    summ_tab.*, possible_exp_and_cc, possible_exp_and_cc_caps, possible_expired, possible_expired_mapped, possible_expired_unmapped,
    possible_expired_caps, possible_expired_mapped_caps, possible_expired_unmapped_caps, possible_cc_orders, possible_cobrm,
    possible_cobra, possible_ocara, possible_cc_orders_caps, possible_cobrm_caps, possible_cobra_caps, possible_ocara_caps
from 
    summ_tab left join possible_ords
on 
    summ_tab.yyyymmdd = possible_ords.ord_date_expcc
    and summ_tab.city = possible_ords.city_expcc
    and summ_tab.segment = possible_ords.segment_1
order by 
    1, 2, 
    (case 
        when segment = 'LP' then 1 
        when segment = 'MP' then 2 
        when segment = 'HP' then 3 
        when segment = 'UHP' then 4 
        when segment = 'UNKNWN' then 5 
    end)
    

-- newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,expired_cc_tab as
(
select 
    date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc,
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,raw_dump as 
(
select  
    * 
from 
    (
     select 
        expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, 
        ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
        (case 
            when city_expcc = 'Indore' then 1300
            when city_expcc IN ('Chennai','Delhi') then 1500
            when city_expcc = 'Mumbai' then 2500
            else 2000
        end) as auto_fm_limit
    from
        expired_cc_tab left join dropped_tab on
        expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
        and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
        and expired_cc_tab.city_expcc = dropped_tab.city_drop
        and expired_cc_tab.service_expcc = dropped_tab.service_drop
    )
where 
    auto_fm_limit >= diff_distance
    and ord_drop is not null
)

-- select
--     ex_ltab.order_date, ex_ltab.city, 
--     requested_orders, net_orders, 
--     expired_orders, possible_expired, expired_mapped, possible_expired_mapped,
--     expired_unmapped, possible_expired_unmapped
-- from 
--     (
    select
        ord_date_expcc as order_date, city_expcc as city, 
        count(distinct ord_expcc) as possible_exp_and_cc,
        
        count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
        
        count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara
        
    from 
        (
        select
            raw_dump.*, first_ord_drop,
            row_number() OVER(PARTITION BY first_ord_drop ORDER BY raw_dump.ord_expcc ASC) as row_firsttt
        from 
            raw_dump
            left join 
            (select ord_expcc, min(ord_drop) as first_ord_drop from raw_dump group by 1) m_ord_tab 
            on raw_dump.ord_expcc = m_ord_tab.ord_expcc
        ) 
    where
        row_firsttt = 1 
    group by 
        1, 2
--     ) ex_ltab 
--     left join 
--     (
--     select
--         date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as o_date, 
--         city as o_city, 
--         count(distinct order_id) as requested_orders,
--         count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then order_id end) as net_orders, 
--         count(distinct case when order_status = 'expired' then order_id end) as expired_orders, 
--         count(distinct case when order_status = 'expired' and length(map_riders) >= 28 then order_id end) as expired_mapped, 
--         count(distinct case when order_status = 'expired' and length(map_riders) < 28 then order_id end) as expired_unmapped 
--     from 
--         logs_snapshot
--     group by 
--         1, 2 
--     ) r_d_tab 
--     on ex_ltab.order_date = r_d_tab.o_date
--     and ex_ltab.city = r_d_tab.o_city
-- order by 
--     1, 2 
    
----- newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd >= '20230130'
    and yyyymmdd <= '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd >= '20230130'
    and yyyymmdd <= '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,expired_cc_tab as
(
select 
    date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc,
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,raw_dump as 
(
select  
    * 
from 
    (
     select 
        expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, 
        ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
        (case 
            when city_expcc = 'Indore' then 1300
            when city_expcc IN ('Chennai','Delhi') then 1500
            when city_expcc = 'Mumbai' then 2500
            else 2000
        end) as auto_fm_limit
    from
        expired_cc_tab left join dropped_tab on
        expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
        and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
        and expired_cc_tab.city_expcc = dropped_tab.city_drop
        and expired_cc_tab.service_expcc = dropped_tab.service_drop
    )
where 
    auto_fm_limit >= diff_distance
    and ord_drop is not null
)

select
    ex_ltab.order_date, ex_ltab.city, requested_orders, net_orders, 
    expired_orders, expired_mapped, expired_unmapped, cc_orders, cobrm, cobra, ocara, 
    possible_exp_and_cc, possible_expired, possible_expired_mapped, possible_expired_unmapped, 
    possible_cc_orders, possible_cobrm, possible_cobra, possible_ocara
from 
    (
    select
        ord_date_expcc as order_date, city_expcc as city, 
        count(distinct ord_expcc) as possible_exp_and_cc,
        
        count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
        
        count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara
        
    from 
        (
        select
            raw_dump.*, first_ord_drop,
            row_number() OVER(PARTITION BY first_ord_drop ORDER BY raw_dump.ord_expcc ASC) as row_firsttt
        from 
            raw_dump
            left join 
            (select ord_expcc, min(ord_drop) as first_ord_drop from raw_dump group by 1) m_ord_tab 
            on raw_dump.ord_expcc = m_ord_tab.ord_expcc
        ) 
    where
        row_firsttt = 1 
    group by 
        1, 2
    ) ex_ltab 
    left join 
    (
    select
        date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as o_date, city as o_city, 
        count(distinct order_id) as requested_orders,
        count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then order_id end) as net_orders, 
        count(distinct case when order_status = 'expired' then order_id end) as expired_orders, 
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then order_id end) as expired_mapped, 
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then order_id end) as expired_unmapped,
        
        count(distinct case when order_status = 'customerCancelled' then order_id end) as cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then order_id end) as cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then order_id end) as cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then order_id end) as ocara
    from 
        logs_snapshot left join (select order_id as o_id_im, map_riders_check, events_list from immutable) immutable 
        on logs_snapshot.order_id = immutable.o_id_im
    group by 
        1, 2 
    ) r_d_tab 
    on ex_ltab.order_date = r_d_tab.o_date
    and ex_ltab.city = r_d_tab.o_city
order by 
    1, 2 
    
----
newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,expired_cc_tab as
(
select 
    date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc,
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,raw_dump as 
(
select  
    * 
from 
    (
     select 
        expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, 
        ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
        (case 
            when city_expcc = 'Indore' then 1300
            when city_expcc IN ('Chennai','Delhi') then 1500
            when city_expcc = 'Mumbai' then 2500
            else 2000
        end) as auto_fm_limit
    from
        expired_cc_tab left join dropped_tab on
        expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
        and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
        and expired_cc_tab.city_expcc = dropped_tab.city_drop
        and expired_cc_tab.service_expcc = dropped_tab.service_drop
    )
where 
    auto_fm_limit >= diff_distance
    and ord_drop is not null
)

select
    ex_ltab.order_date, ex_ltab.city, requested_orders, net_orders, 
    expired_orders, expired_mapped, expired_unmapped, cc_orders, cobrm, cobra, ocara, 
    possible_exp_and_cc, possible_expired, possible_expired_mapped, possible_expired_unmapped, 
    possible_cc_orders, possible_cobrm, possible_cobra, possible_ocara
from 
    (
    select
        ord_date_expcc as order_date, city_expcc as city, 
        count(distinct ord_expcc) as possible_exp_and_cc,
        
        count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
        
        count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara
        
    from 
        (
        select
            raw_dump.*, first_ord_drop,
            row_number() OVER(PARTITION BY first_ord_drop ORDER BY raw_dump.ord_expcc ASC) as row_firsttt
        from 
            raw_dump
            left join 
            (select ord_expcc, min(ord_drop) as first_ord_drop from raw_dump group by 1) m_ord_tab 
            on raw_dump.ord_expcc = m_ord_tab.ord_expcc
        ) 
    where
        row_firsttt = 1 
    group by 
        1, 2
    ) ex_ltab 
    left join 
    (
    select
        date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as o_date, city as o_city, 
        count(distinct order_id) as requested_orders,
        count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then order_id end) as net_orders, 
        count(distinct case when order_status = 'expired' then order_id end) as expired_orders, 
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then order_id end) as expired_mapped, 
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then order_id end) as expired_unmapped,
        
        count(distinct case when order_status = 'customerCancelled' then order_id end) as cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then order_id end) as cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then order_id end) as cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then order_id end) as ocara
    from 
        logs_snapshot left join (select order_id as o_id_im, map_riders_check, events_list from immutable) immutable 
        on logs_snapshot.order_id = immutable.o_id_im
    group by 
        1, 2 
    ) r_d_tab 
    on ex_ltab.order_date = r_d_tab.o_date
    and ex_ltab.city = r_d_tab.o_city
order by 
    1, 2 
    
---
newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,expired_cc_tab as
(
select 
    date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, captain_id,
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch, drop_captain
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop, captain_id as drop_captain
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,raw_dump as 
(
select  
    * 
from 
    (
     select 
        expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, drop_captain,
        ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
        (case 
            when city_expcc = 'Indore' then 1300
            when city_expcc IN ('Chennai','Delhi') then 1500
            when city_expcc = 'Mumbai' then 2500
            else 2000
        end) as auto_fm_limit
    from
        expired_cc_tab left join dropped_tab on
        expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
        and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
        and expired_cc_tab.city_expcc = dropped_tab.city_drop
        and expired_cc_tab.service_expcc = dropped_tab.service_drop
    )
where 
    auto_fm_limit >= diff_distance
    and ord_drop is not null
)

select
    ex_ltab.order_date, ex_ltab.city, requested_orders, net_orders, 
    expired_orders, expired_mapped, expired_unmapped, cc_orders, cobrm, cobra, ocara, 
    possible_exp_and_cc, possible_expired, possible_expired_mapped, possible_expired_unmapped, 
    possible_cc_orders, possible_cobrm, possible_cobra, possible_ocara, 
    
    net_captains, possible_exp_and_cc_caps, possible_expired_caps, possible_expired_mapped_caps, possible_expired_unmapped_caps,
    possible_cc_orders_caps, possible_cobrm_caps, possible_cobra_caps, possible_ocara_caps
from 
    (
    select
        ord_date_expcc as order_date, city_expcc as city, 
        count(distinct ord_expcc) as possible_exp_and_cc,
        count(distinct drop_captain) as possible_exp_and_cc_caps,
        
        count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
        
        count(distinct case when order_status = 'expired' then drop_captain end) as possible_expired_caps,
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then drop_captain end) as possible_expired_mapped_caps,
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then drop_captain end) as possible_expired_unmapped_caps,
        
        count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
        
        count(distinct case when order_status = 'customerCancelled' then drop_captain end) as possible_cc_orders_caps,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then drop_captain end) as possible_cobrm_caps,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then drop_captain end) as possible_cobra_caps,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then drop_captain end) as possible_ocara_caps
        
    from 
        (
        select
            raw_dump.*, first_ord_drop,
            row_number() OVER(PARTITION BY first_ord_drop ORDER BY raw_dump.ord_expcc ASC) as row_firsttt
        from 
            raw_dump
            left join 
            (select ord_expcc, min(ord_drop) as first_ord_drop from raw_dump group by 1) m_ord_tab 
            on raw_dump.ord_expcc = m_ord_tab.ord_expcc
        ) 
    where
        row_firsttt = 1 
    group by 
        1, 2
    ) ex_ltab 
    left join 
    (
    select
        date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as o_date, city as o_city, 
        count(distinct order_id) as requested_orders,
        count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then order_id end) as net_orders, 
        
        count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then captain_id end) as net_captains, 
        
        count(distinct case when order_status = 'expired' then order_id end) as expired_orders, 
        count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then order_id end) as expired_mapped, 
        count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then order_id end) as expired_unmapped,
        
        count(distinct case when order_status = 'customerCancelled' then order_id end) as cc_orders,
        count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then order_id end) as cobrm,
        count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
        not contains(events_list,'accepted') then order_id end) as cobra,
        count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then order_id end) as ocara
    from 
        logs_snapshot left join (select order_id as o_id_im, map_riders_check, events_list from immutable) immutable 
        on logs_snapshot.order_id = immutable.o_id_im
    group by 
        1, 2 
    ) r_d_tab 
    on ex_ltab.order_date = r_d_tab.o_date
    and ex_ltab.city = r_d_tab.o_city
order by 
    1, 2 
    
---
newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,expired_cc_tab as
(
select 
    date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, captain_id,
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

-- ,raw_dump as 
-- (
select  
    *, 
    row_number() over(partition by ord_expcc order by diff_distance) as rw_min_dstnc
from 
    (
     select 
        expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, 
        ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
        (case 
            when city_expcc = 'Indore' then 1300
            when city_expcc IN ('Chennai','Delhi') then 1500
            when city_expcc = 'Mumbai' then 2500
            else 2000
        end) as auto_fm_limit
    from
        expired_cc_tab left join dropped_tab on
        expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
        and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
        and expired_cc_tab.city_expcc = dropped_tab.city_drop
        and expired_cc_tab.service_expcc = dropped_tab.service_drop
    )
where 
    auto_fm_limit >= diff_distance
    and ord_drop is not null
    
order by 
    ord_expcc, ord_drop
    
    limit 200 
    
    
-- )

-- select
--     ex_ltab.order_date, ex_ltab.city, requested_orders, net_orders, 
--     expired_orders, expired_mapped, expired_unmapped, cc_orders, cobrm, cobra, ocara, 
--     possible_exp_and_cc, possible_expired, possible_expired_mapped, possible_expired_unmapped, 
--     possible_cc_orders, possible_cobrm, possible_cobra, possible_ocara
-- from 
--     (
--     select
--         ord_date_expcc as order_date, city_expcc as city, 
--         count(distinct ord_expcc) as possible_exp_and_cc,
--         count(distinct captain_id) as possible_exp_and_cc_caps,
        
--         count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
--         count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
--         count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
        
--         count(distinct case when order_status = 'expired' then captain_id end) as possible_expired_caps,
--         count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then captain_id end) as possible_expired_mapped_caps,
--         count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then captain_id end) as possible_expired_unmapped_caps,
        
--         count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
--         count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
--         count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
--         not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
--         count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
        
--         count(distinct case when order_status = 'customerCancelled' then captain_id end) as possible_cc_orders_caps,
--         count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then captain_id end) as possible_cobrm_caps,
--         count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
--         not contains(events_list,'accepted') then captain_id end) as possible_cobra_caps,
--         count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then captain_id end) as possible_ocara_caps
        
--     from 
--         (
--         select
--             raw_dump.*, first_ord_drop,
--             row_number() OVER(PARTITION BY first_ord_drop ORDER BY raw_dump.ord_expcc ASC) as row_firsttt
--         from 
--             raw_dump
--             left join 
--             (select ord_expcc, min(ord_drop) as first_ord_drop, 
--             count(distinct ord_drop) as possible_drop_orders
--             from raw_dump group by 1) m_ord_tab 
--             on raw_dump.ord_expcc = m_ord_tab.ord_expcc
--         ) 
--     where
--         row_firsttt = 1 
--     group by 
--         1, 2
--     ) ex_ltab 
--     left join 
--     (
--     select
--         date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') as o_date, city as o_city, 
--         count(distinct order_id) as requested_orders,
--         count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then order_id end) as net_orders, 
        
--         count(distinct case when order_status = 'dropped' and (spd_fraud_flag = false or spd_fraud_flag is null) then captain_id end) as net_captains, 
        
--         count(distinct case when order_status = 'expired' then order_id end) as expired_orders, 
--         count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then order_id end) as expired_mapped, 
--         count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then order_id end) as expired_unmapped,
        
--         count(distinct case when order_status = 'customerCancelled' then order_id end) as cc_orders,
--         count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then order_id end) as cobrm,
--         count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
--         not contains(events_list,'accepted') then order_id end) as cobra,
--         count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then order_id end) as ocara
--     from 
--         logs_snapshot left join (select order_id as o_id_im, map_riders_check, events_list from immutable) immutable 
--         on logs_snapshot.order_id = immutable.o_id_im
--     group by 
--         1, 2 
--     ) r_d_tab 
--     on ex_ltab.order_date = r_d_tab.o_date
--     and ex_ltab.city = r_d_tab.o_city
-- order by 
--     1, 2 
    
----
newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur')
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur')
    and service_obj_service_name = 'Auto'
)

,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230205'
    and lower(city_name) IN ('hyderabad','chennai','mumbai','delhi','pune','jaipur')
)

,summ_tab as 
(
select
    logs_snapshot.yyyymmdd, logs_snapshot.city, coalesce(performance_segment,'UNKNWN') as segment, 
    count(distinct order_id) as net_orders, 
    count(distinct logs_snapshot.captain_id) as net_captains
from 
    logs_snapshot left join segment_tab
on 
    logs_snapshot.yyyymmdd = segment_tab.yyyymmdd
    and lower(logs_snapshot.city) = segment_tab.city_name
    and logs_snapshot.captain_id = segment_tab.captain_id
where 
    order_status = 'dropped'
    and (spd_fraud_flag = false or spd_fraud_flag is null)
group by 
    1, 2, 3 
)

,expired_cc_tab as
(
select 
    yyyymmdd as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, 
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch, drop_cap
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop, captain_id as drop_cap
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,possible_ords as 
(
select 
    ord_date_expcc, city_expcc, coalesce(performance_segment,'UNKNWN') as segment_1, 
    count(distinct ord_expcc) as possible_exp_and_cc,
    count(distinct drop_cap) as possible_exp_and_cc_caps,
    
    count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
    
    count(distinct case when order_status = 'expired' then drop_cap end) as possible_expired_caps,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then drop_cap end) as possible_expired_mapped_caps,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then drop_cap end) as possible_expired_unmapped_caps,
    
    count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
    
    count(distinct case when order_status = 'customerCancelled' then drop_cap end) as possible_cc_orders_caps,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then drop_cap end) as possible_cobrm_caps,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then drop_cap end) as possible_cobra_caps,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then drop_cap end) as possible_ocara_caps
from 
    (
    select  
        ord_date_expcc, city_expcc, service_expcc, ord_expcc, order_status, 
        map_riders_check, ord_drop, diff_distance, drop_cap, events_list,
        row_number() over(partition by ord_drop order by diff_distance) as drop_row_first,
        row_number() over(partition by ord_expcc order by diff_distance) as exp_row_sec
    from 
        (
         select 
            expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, drop_cap,
            ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), 
            to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
            (case 
                when city_expcc = 'Indore' then 1300
                when city_expcc IN ('Chennai','Delhi') then 1500
                when city_expcc = 'Mumbai' then 2500
                else 2000
            end) as auto_fm_limit
        from
            expired_cc_tab left join dropped_tab on
            expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
            and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
            and expired_cc_tab.city_expcc = dropped_tab.city_drop
            and expired_cc_tab.service_expcc = dropped_tab.service_drop
        )
    where  
        auto_fm_limit >= diff_distance
        and ord_drop is not null
    ) rw_extn left join segment_tab
    on 
        rw_extn.ord_date_expcc = segment_tab.yyyymmdd
        and lower(rw_extn.city_expcc) = segment_tab.city_name
        and rw_extn.drop_cap = segment_tab.captain_id
where 
    exp_row_sec = 1
    and drop_row_first = 1
group by 
    1, 2, 3 
)

select
    summ_tab.*, possible_exp_and_cc, possible_exp_and_cc_caps, possible_expired, possible_expired_mapped, possible_expired_unmapped,
    possible_expired_caps, possible_expired_mapped_caps, possible_expired_unmapped_caps, possible_cc_orders, possible_cobrm,
    possible_cobra, possible_ocara, possible_cc_orders_caps, possible_cobrm_caps, possible_cobra_caps, possible_ocara_caps
from 
    summ_tab left join possible_ords
on 
    summ_tab.yyyymmdd = possible_ords.ord_date_expcc
    and summ_tab.city = possible_ords.city_expcc
    and summ_tab.segment = possible_ords.segment_1
order by 
    1, 2, 
    (case 
        when segment = 'LP' then 1 
        when segment = 'MP' then 2 
        when segment = 'HP' then 3 
        when segment = 'UHP' then 4 
        when segment = 'UNKNWN' then 5 
    end)
    


----
newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name = 'Bangalore'
    and service_obj_service_name = 'Auto'
)

,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230205'
    and lower(city_name) = 'bangalore'
)

,summ_tab as 
(
select
    logs_snapshot.yyyymmdd, logs_snapshot.city, coalesce(performance_segment,'UNKNWN') as segment, 
    count(distinct order_id) as net_orders, 
    count(distinct logs_snapshot.captain_id) as net_captains
from 
    logs_snapshot left join segment_tab
on 
    logs_snapshot.yyyymmdd = segment_tab.yyyymmdd
    and lower(logs_snapshot.city) = segment_tab.city_name
    and logs_snapshot.captain_id = segment_tab.captain_id
where 
    order_status = 'dropped'
    and (spd_fraud_flag = false or spd_fraud_flag is null)
group by 
    1, 2, 3 
)

,expired_cc_tab as
(
select 
    yyyymmdd as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, 
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch, drop_cap
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop, captain_id as drop_cap
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

,possible_ords as 
(
select 
    ord_date_expcc, city_expcc, coalesce(performance_segment,'UNKNWN') as segment_1, 
    count(distinct ord_expcc) as possible_exp_and_cc,
    count(distinct drop_cap) as possible_exp_and_cc_caps,
    
    count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
    
    count(distinct case when order_status = 'expired' then drop_cap end) as possible_expired_caps,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then drop_cap end) as possible_expired_mapped_caps,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then drop_cap end) as possible_expired_unmapped_caps,
    
    count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
    
    count(distinct case when order_status = 'customerCancelled' then drop_cap end) as possible_cc_orders_caps,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then drop_cap end) as possible_cobrm_caps,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then drop_cap end) as possible_cobra_caps,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then drop_cap end) as possible_ocara_caps
from 
    (
    select  
        ord_date_expcc, city_expcc, service_expcc, ord_expcc, order_status, 
        map_riders_check, ord_drop, diff_distance, drop_cap, events_list,
        row_number() over(partition by ord_drop order by diff_distance) as drop_row_first,
        row_number() over(partition by ord_expcc order by diff_distance) as exp_row_sec
    from 
        (
         select 
            expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, drop_cap,
            ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), 
            to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
            (case 
                when city_expcc = 'Indore' then 1300
                when city_expcc IN ('Chennai','Delhi') then 1500
                when city_expcc = 'Mumbai' then 2500
                else 2000
            end) as auto_fm_limit
        from
            expired_cc_tab left join dropped_tab on
            expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
            and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
            and expired_cc_tab.city_expcc = dropped_tab.city_drop
            and expired_cc_tab.service_expcc = dropped_tab.service_drop
        )
    where  
        auto_fm_limit >= diff_distance
        and ord_drop is not null
    ) rw_extn left join segment_tab
    on 
        rw_extn.ord_date_expcc = segment_tab.yyyymmdd
        and lower(rw_extn.city_expcc) = segment_tab.city_name
        and rw_extn.drop_cap = segment_tab.captain_id
where 
    exp_row_sec = 1
    and drop_row_first = 1
group by 
    1, 2, 3 
-- )

-- select
--     summ_tab.*, possible_exp_and_cc, possible_exp_and_cc_caps, possible_expired, possible_expired_mapped, possible_expired_unmapped,
--     possible_expired_caps, possible_expired_mapped_caps, possible_expired_unmapped_caps, possible_cc_orders, possible_cobrm,
--     possible_cobra, possible_ocara, possible_cc_orders_caps, possible_cobrm_caps, possible_cobra_caps, possible_ocara_caps
-- from 
--     summ_tab left join possible_ords
-- on 
--     summ_tab.yyyymmdd = possible_ords.ord_date_expcc
--     and summ_tab.city = possible_ords.city_expcc
--     and summ_tab.segment = possible_ords.segment_1
-- order by 
--     1, 2, 
--     (case 
--         when segment = 'LP' then 1 
--         when segment = 'MP' then 2 
--         when segment = 'HP' then 3 
--         when segment = 'UHP' then 4 
--         when segment = 'UNKNWN' then 5 
--     end)
    


-----
newquery
with immutable as
( 
select 
    order_id, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur','Bangalore')
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur','Bangalore')
    and service_obj_service_name = 'Auto'
)

,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230205'
    and lower(city_name) IN ('hyderabad','chennai','mumbai','delhi','pune','jaipur','bangalore')
)

,summ_tab as 
(
select
    logs_snapshot.yyyymmdd, logs_snapshot.city, coalesce(performance_segment,'UNKNWN') as segment, 
    count(distinct order_id) as net_orders, 
    count(distinct logs_snapshot.captain_id) as net_captains
from 
    logs_snapshot left join segment_tab
on 
    logs_snapshot.yyyymmdd = segment_tab.yyyymmdd
    and lower(logs_snapshot.city) = segment_tab.city_name
    and logs_snapshot.captain_id = segment_tab.captain_id
where 
    order_status = 'dropped'
    and (spd_fraud_flag = false or spd_fraud_flag is null)
group by 
    1, 2, 3 
)

,expired_cc_tab as
(
select 
    yyyymmdd as ord_date_expcc, city as city_expcc, service as service_expcc,
    ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, 
    order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
from
    (
    select
        order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
        pickup_location_longitude, pickup_cluster, order_status, city, service
    from
        logs_snapshot
    where
        order_status IN ('expired','customerCancelled')
    ) ols_tab 
    join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
)

,dropped_tab as
(
select 
    ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch, drop_cap
from
    (
    select
        order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
        drop_location_longitude as drop_lng, order_status, city as city_drop, captain_id as drop_cap
    from
        logs_snapshot
    where
        order_status = 'dropped'
        and (spd_fraud_flag = false or spd_fraud_flag is null)
    ) ols 
    join immutable im on ols.ord_drop = im.order_id
)

-- ,possible_ords as 
-- (
select 
    ord_date_expcc, city_expcc, 
    coalesce(performance_segment,'UNKNWN') as segment_1, 
    count(distinct ord_expcc) as possible_exp_and_cc,
    count(distinct ord_drop) as possible_drp_new,
    count(distinct drop_cap) as possible_exp_and_cc_caps
    
    -- count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
    -- count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
    -- count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
    
    -- count(distinct case when order_status = 'expired' then drop_cap end) as possible_expired_caps,
    -- count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then drop_cap end) as possible_expired_mapped_caps,
    -- count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then drop_cap end) as possible_expired_unmapped_caps,
    
    -- count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
    -- count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
    -- count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    -- not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
    -- count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
    
    -- count(distinct case when order_status = 'customerCancelled' then drop_cap end) as possible_cc_orders_caps,
    -- count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then drop_cap end) as possible_cobrm_caps,
    -- count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    -- not contains(events_list,'accepted') then drop_cap end) as possible_cobra_caps,
    -- count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then drop_cap end) as possible_ocara_caps
from 
    (
    select  
        ord_date_expcc, city_expcc, service_expcc, ord_expcc, order_status, 
        map_riders_check, ord_drop, diff_distance, drop_cap, events_list,
        row_number() over(partition by ord_drop order by diff_distance) as drop_row_first,
        row_number() over(partition by ord_expcc order by diff_distance) as exp_row_sec
    from 
        (
         select 
            expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, drop_cap,
            ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), 
            to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
            (case 
                when city_expcc = 'Indore' then 1300
                when city_expcc IN ('Chennai','Delhi') then 1500
                when city_expcc = 'Mumbai' then 2500
                else 2000
            end) as auto_fm_limit
        from
            expired_cc_tab left join dropped_tab on
            expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
            and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
            and expired_cc_tab.city_expcc = dropped_tab.city_drop
            and expired_cc_tab.service_expcc = dropped_tab.service_drop
        )
    where  
        auto_fm_limit >= diff_distance
        and ord_drop is not null
    )
    rw_extn left join segment_tab
    on 
        rw_extn.ord_date_expcc = segment_tab.yyyymmdd
        and lower(rw_extn.city_expcc) = segment_tab.city_name
        and rw_extn.drop_cap = segment_tab.captain_id
-- where 
--     exp_row_sec = 1
--     and drop_row_first = 1
group by 
    1, 2, 3 
-- )

-- select
--     summ_tab.*, possible_exp_and_cc, possible_exp_and_cc_caps, possible_expired, possible_expired_mapped, possible_expired_unmapped,
--     possible_expired_caps, possible_expired_mapped_caps, possible_expired_unmapped_caps, possible_cc_orders, possible_cobrm,
--     possible_cobra, possible_ocara, possible_cc_orders_caps, possible_cobrm_caps, possible_cobra_caps, possible_ocara_caps
-- from 
--     summ_tab left join possible_ords
-- on 
--     summ_tab.yyyymmdd = possible_ords.ord_date_expcc
--     and summ_tab.city = possible_ords.city_expcc
--     and summ_tab.segment = possible_ords.segment_1
order by 
    1, 2,
    (case 
        when segment_1 = 'LP' then 1 
        when segment_1 = 'MP' then 2 
        when segment_1 = 'HP' then 3 
        when segment_1 = 'UHP' then 4 
        when segment_1 = 'UNKNWN' then 5 
    end)
    


---

newquery
with immutable as
( 
select 
    order_id as ord_im, 
    from_unixtime(min(epoch)/1000,'Asia/Kolkata') as requested_epoch,
    from_unixtime((min(epoch) + 180000)/1000, 'Asia/Kolkata') as requested_3mins,
    from_unixtime(max(case when event_type = 'dropped' then updated_epoch end)/1000,'Asia/Kolkata') as dropped_epoch, 
    from_unixtime(max(updated_epoch)/1000,'Asia/Kolkata') as order_end_epoch, 
    array_agg(distinct (case when length(map_riders) >= 28 then 'true' else 'false' end)) as map_riders_check, 
    array_agg(distinct event_type) as events_list
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur')
    and service_obj_service_name = 'Auto'
group by 
    1
)

,logs_snapshot as
(
select 
    yyyymmdd, order_id, service_obj_city_display_name as city, service_obj_service_name as service, 
    captain_id, pickup_cluster, order_status, drop_cluster, spd_fraud_flag, 
    pickup_location_latitude, pickup_location_longitude, drop_location_latitude, drop_location_longitude
from
    orders.order_logs_snapshot
where
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur')
    and service_obj_service_name = 'Auto'
)

select 
    yyyymmdd, city, 
    count(distinct order_id) as exp_and_cc,
    
    count(distinct case when order_status = 'expired' then order_id end) as expired,
    count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then order_id end) as expired_mapped,
    count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then order_id end) as expired_unmapped,
    
    count(distinct case when order_status = 'customerCancelled' then order_id end) as cc_orders,
    count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then order_id end) as cobrm,
    count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
    not contains(events_list,'accepted') then order_id end) as cobra,
    count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then order_id end) as ocara
from 
    logs_snapshot left join immutable
on 
    logs_snapshot.order_id = immutable.ord_im
group by 
    1, 2 
order by 
    1, 2 
    

----

newquery
with immutable as
( 
select 
    yyyymmdd, service_obj_city_display_name as city, order_id, captain_id, event_type
from
    orders.order_logs_immutable
where 
    yyyymmdd = '20230205'
    and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur','Bangalore')
    and service_obj_service_name = 'Auto'
    and event_type in ('rider_reject', 'rider_busy', 'accepted')
)

-- ,logs_snapshot as
-- (
-- select 
--     yyyymmdd as ymd, order_id as o_idd
-- from
--     orders.order_logs_snapshot
-- where
--     yyyymmdd = '20230205'
--     and service_obj_city_display_name IN ('Hyderabad','Chennai','Mumbai','Delhi','Pune','Jaipur','Bangalore')
--     and service_obj_service_name = 'Auto'
--     and order_status = 'dropped'
--     and (spd_fraud_flag = false or spd_fraud_flag is null)
-- )

,segment_tab as 
(
select
    distinct captain_id, yyyymmdd, performance_segment, pc_segment, lower(city_name) as city_name
from 
    datasets.captain_auto_pc_segments
where 
    yyyymmdd = '20230205'
    and lower(city_name) IN ('hyderabad','chennai','mumbai','delhi','pune','jaipur','bangalore')
)


select
    immutable.yyyymmdd, city, coalesce(performance_segment,'UNKNWN') as segment_1, 
    count(order_id) as total_pings,
    count(case when event_type = 'accepted' then order_id end) as accepted_pings
from 
    immutable
    
    -- left join logs_snapshot 
    -- on immutable.yyyymmdd = logs_snapshot.ymd
    -- immutable.order_id = logs_snapshot.o_idd
    
    left join segment_tab
    on immutable.yyyymmdd = segment_tab.yyyymmdd
    and lower(immutable.city) = segment_tab.city_name
    and immutable.captain_id = segment_tab.captain_id
group by 
    1, 2, 3 
-- )

-- ,expired_cc_tab as
-- (
-- select 
--     yyyymmdd as ord_date_expcc, city as city_expcc, service as service_expcc,
--     ord_expcc, pickup_location_latitude as pick_lat_expcc, pickup_location_longitude as pick_lng_expcc, 
--     order_status, requested_epoch, requested_3mins, order_end_epoch, map_riders_check, events_list
-- from
--     (
--     select
--         order_id as ord_expcc, yyyymmdd, captain_id, pickup_location_latitude, 
--         pickup_location_longitude, pickup_cluster, order_status, city, service
--     from
--         logs_snapshot
--     where
--         order_status IN ('expired','customerCancelled')
--     ) ols_tab 
--     join immutable im_tab on ols_tab.ord_expcc = im_tab.order_id
-- )

-- ,dropped_tab as
-- (
-- select 
--     ord_drop, drop_lat, drop_lng, city_drop, service_drop, dropped_epoch, drop_cap
-- from
--     (
--     select
--         order_id as ord_drop, drop_location_latitude as drop_lat, service as service_drop,
--         drop_location_longitude as drop_lng, order_status, city as city_drop, captain_id as drop_cap
--     from
--         logs_snapshot
--     where
--         order_status = 'dropped'
--         and (spd_fraud_flag = false or spd_fraud_flag is null)
--     ) ols 
--     join immutable im on ols.ord_drop = im.order_id
-- )

-- -- ,possible_ords as 
-- -- (
-- select 
--     ord_date_expcc, city_expcc, 
--     -- coalesce(performance_segment,'UNKNWN') as segment_1, 
--     count(distinct ord_expcc) as possible_exp_and_cc,
--     count(distinct ord_drop) as possible_drp_new,
--     count(distinct drop_cap) as possible_exp_and_cc_caps
    
--     -- count(distinct case when order_status = 'expired' then ord_expcc end) as possible_expired,
--     -- count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then ord_expcc end) as possible_expired_mapped,
--     -- count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then ord_expcc end) as possible_expired_unmapped,
    
--     -- count(distinct case when order_status = 'expired' then drop_cap end) as possible_expired_caps,
--     -- count(distinct case when order_status = 'expired' and contains(map_riders_check,'true') then drop_cap end) as possible_expired_mapped_caps,
--     -- count(distinct case when order_status = 'expired' and not contains(map_riders_check,'true') then drop_cap end) as possible_expired_unmapped_caps,
    
--     -- count(distinct case when order_status = 'customerCancelled' then ord_expcc end) as possible_cc_orders,
--     -- count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then ord_expcc end) as possible_cobrm,
--     -- count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
--     -- not contains(events_list,'accepted') then ord_expcc end) as possible_cobra,
--     -- count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then ord_expcc end) as possible_ocara,
    
--     -- count(distinct case when order_status = 'customerCancelled' then drop_cap end) as possible_cc_orders_caps,
--     -- count(distinct case when order_status = 'customerCancelled' and not contains(map_riders_check,'true') then drop_cap end) as possible_cobrm_caps,
--     -- count(distinct case when order_status = 'customerCancelled' and contains(map_riders_check,'true') and
--     -- not contains(events_list,'accepted') then drop_cap end) as possible_cobra_caps,
--     -- count(distinct case when order_status = 'customerCancelled' and contains(events_list,'accepted') then drop_cap end) as possible_ocara_caps
-- from 
--     (
--     select  
--         ord_date_expcc, city_expcc, service_expcc, ord_expcc, order_status, 
--         map_riders_check, ord_drop, diff_distance, drop_cap, events_list,
--         row_number() over(partition by ord_drop order by diff_distance) as drop_row_first,
--         row_number() over(partition by ord_expcc order by diff_distance) as exp_row_sec
--     from 
--         (
--          select 
--             expired_cc_tab.*, ord_drop, dropped_epoch, drop_lat, drop_lng, drop_cap,
--             ST_Distance(to_spherical_geography(ST_Point(drop_lng,drop_lat)), 
--             to_spherical_geography(ST_Point(pick_lng_expcc,pick_lat_expcc))) as diff_distance, 
--             (case 
--                 when city_expcc = 'Indore' then 1300
--                 when city_expcc IN ('Chennai','Delhi') then 1500
--                 when city_expcc = 'Mumbai' then 2500
--                 else 2000
--             end) as auto_fm_limit
--         from
--             expired_cc_tab left join dropped_tab on
--             expired_cc_tab.requested_epoch <= dropped_tab.dropped_epoch
--             and expired_cc_tab.requested_3mins >= dropped_tab.dropped_epoch
--             and expired_cc_tab.city_expcc = dropped_tab.city_drop
--             and expired_cc_tab.service_expcc = dropped_tab.service_drop
--         )
--     where  
--         auto_fm_limit >= diff_distance
--         and ord_drop is not null
--     )
--     -- rw_extn left join segment_tab
--     -- on 
--     --     rw_extn.ord_date_expcc = segment_tab.yyyymmdd
--     --     and lower(rw_extn.city_expcc) = segment_tab.city_name
--     --     and rw_extn.drop_cap = segment_tab.captain_id
-- -- where 
-- --     exp_row_sec = 1
-- --     and drop_row_first = 1
-- group by 
--     1, 2
-- -- )

-- -- select
-- --     summ_tab.*, possible_exp_and_cc, possible_exp_and_cc_caps, possible_expired, possible_expired_mapped, possible_expired_unmapped,
-- --     possible_expired_caps, possible_expired_mapped_caps, possible_expired_unmapped_caps, possible_cc_orders, possible_cobrm,
-- --     possible_cobra, possible_ocara, possible_cc_orders_caps, possible_cobrm_caps, possible_cobra_caps, possible_ocara_caps
-- -- from 
-- --     summ_tab left join possible_ords
-- -- on 
-- --     summ_tab.yyyymmdd = possible_ords.ord_date_expcc
-- --     and summ_tab.city = possible_ords.city_expcc
-- --     and summ_tab.segment = possible_ords.segment_1
order by 
    1, 2,
    (case 
        when segment_1 = 'LP' then 1 
        when segment_1 = 'MP' then 2 
        when segment_1 = 'HP' then 3 
        when segment_1 = 'UHP' then 4 
        when segment_1 = 'UNKNWN' then 5 
    end)
    


---


