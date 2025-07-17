"""
-- 1. dim_complex
insert overwrite table tms.dim_complex_full
    partition (dt = '20250718')
select complex_info.id as id,
       complex_name,
       courier_emp_id,
       province_id,
       dic_for_prov.name province_name,
       city_id,
       dic_for_city.name city_name,
       district_id,
       district_name
from (select id,
             complex_name,
             province_id,
             city_id,
             district_id,
             district_name
      from ods_base_complex
      where ds = '20250711'
        and is_deleted = '0') complex_info
         join
     (select id,
             name
      from ods_base_region_info
      where ds = '20200623'
        and is_deleted = '0') dic_for_prov
     on complex_info.province_id = dic_for_prov.id
         join
     (select id,
             name
      from ods_base_region_info
      where ds = '20200623'
        and is_deleted = '0') dic_for_city
     on complex_info.city_id = dic_for_city.id
         left join
     (select courier_emp_id,
             complex_id
      from ods_express_courier_complex
      where ds = '20250710'
        and is_deleted = '0') complex_courier
     on complex_info.id = complex_courier.complex_id;

-- 2. dim_organ
insert overwrite table tms.dim_organ_full
    partition (dt = '20250718')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ
      where ds = '20250711'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info
    where ds = '2023-01-10'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ
    where ds = '20250711'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;

-- 3. dim_region
insert overwrite table dim_region_full
    partition (dt = '20250718')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info
where ds = '20200623'
  and is_deleted = '0';

-- 4. dim_express_courier
insert overwrite table tms.dim_express_courier_full
    partition (dt = '20250718')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             md5(working_phone) working_phone,
             express_type
      from ods_express_courier
      where ds = '20200811'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from ods_base_organ
    where ds = '20250711'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_base_dic
    where ds = '20220708'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;

-- 5. dim_shift
insert overwrite table tms.dim_shift_full
    partition (dt = '20250718')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from ods_line_base_shift
      where ds = '20250711'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from ods_line_base_info
      where ds = '20250710'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from ods_base_dic
    where ds = '20220708'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;

-- 6. dim_truck_driver
insert overwrite table tms.dim_truck_driver_full
    partition (dt = '20250718')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver
      where ds = '20250710'
        and is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from ods_base_organ
    where ds = '20250711'
      and is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_truck_team
    where ds = '20250710'
      and is_deleted = '0'
) team_info
              on driver_info.team_id = team_info.id;

-- 7. dim_truck
insert overwrite table tms.dim_truck_full
    partition (dt = '20250718')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info
      where ds = '20250710'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,

             manager_emp_id
      from ods_truck_team
      where ds = '20250710'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,

             model_no,
             brand,

             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model
      where ds = '20220618'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ
      where ds = '20250711'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;

-- 8. dim_user_zip
-- 8.1 首日装载
insert overwrite table dim_user_zip
    partition (dt = '9999-12-31')
select id,
       login_name,
       nick_name,
       md5(passwd)  as passwd,
       md5(real_name)                                                                                 realname,
       md5(if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone_num, null))   as phone_num,
       md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) as email,
       user_level,
       date_add('1970-01-01', cast(birthday as int))  as birthday,
       gender,
       date_format(from_utc_timestamp(
                           cast(create_time as bigint), 'UTC'),
                   'yyyy-MM-dd')                                                                            start_date,
       '9999-12-31'                                                                                         end_date
from ods_user_info
where ds = '20250710'
  and is_deleted = '0';

-- 8.2 每日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip
    partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-10', -1))as end_date,
       if(rk = 1, end_date, date_add('2025-07-10', -1)) dt
from (select id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   login_name,
                   nick_name,
                   passwd,
                   real_name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   start_date,
                   end_date
            from dim_user_zip
            where dt = '9999-12-31'
            union
            select id,
                   login_name,
                   nick_name,
                   md5(passwd)                                                                              passwd,
                   md5(real_name)                                                                           realname,
                   md5(if(phone_num regexp
                          '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                          phone_num, null))                                                                 phone_num,
                   md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
                   user_level,
                   cast(date_add('1970-01-01', cast(birthday as int)) as string)                            birthday,
                   gender,
                   '2025-07-10'                                                                             start_date,
                   '9999-12-31'                                                                             end_date
            from (select id,
                         login_name,
                         nick_name,
                         passwd,
                         real_name,
                         phone_num,
                         email,
                         user_level,
                         birthday,
                         gender,
                         row_number() over (partition by id order by ds desc) rn
                  from ods_user_info
                  where ds = '2025-07-10'
                    and is_deleted = '0'
                 ) inc
            where rn = 1) full_info) final_info;

-- 9. dim_user_address_zip
-- 9.1 首日装载
insert overwrite table dim_user_address_zip
    partition (dt = '9999-12-31')
select id,
       user_id,
       md5(if(phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone, null)) as phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       concat(substr(create_time, 1, 10), ' ',
              substr(create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from ods_user_address
where ds = '20250711'
  and is_deleted = '0';

-- 9.2 每日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_address_zip
    partition (dt)
select id,
       user_id,
       phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-11', -1)) as end_date,
       if(rk = 1, end_date, date_add('2025-07-11', -1)) dt
from (select id,
             user_id,
             phone,
             province_id,
             city_id,
             district_id,
             complex_id,
             address,
             is_default,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   start_date,
                   end_date
            from dim_user_address_zip
            where dt = '9999-12-31'
            union
            select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   '2025-07-11' start_date,
                   '9999-12-31' end_date
            from (select id,
                         user_id,
                         phone,
                         province_id,
                         city_id,
                         district_id,
                         complex_id,
                         address,
                         cast(is_default as tinyint)                          is_default,
                         row_number() over (partition by id order by ds desc) rn
                  from ods_user_address
                  where ds = '20250711'
                    and is_deleted = '0') inc
            where rn = 1
           ) union_info
     ) with_rk;




-- 1.1.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount,
       dt
from (select org_id,
             org_name,
             sender_city_id  city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount,
             dt
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id,
                   sum(amount) amount,
                   dt
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount,
                         dt
                  from dwd_trade_order_detail_inc) detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id,
                     dt) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250718') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               cargo_type,
               cargo_type_name,
               sender_city_id,
               dt) agg
         left join (
    select id,
           name
    from dim_region_full
    where dt = '20250718'
) region on city_id = region.id;

-- 1.1.2 每日装载
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from (select org_id,
             org_name,
             city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id city_id,
                   sum(amount)    amount
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount
                  from dwd_trade_order_detail_inc
                  where dt = '2023-01-11') detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250718') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               city_id,
               cargo_type,
               cargo_type_name) agg
         left join (
    select id,
           name
    from dim_region_full
    where dt = '20250718'
) region on city_id = region.id;

-- 2.1 dws_trade_org_cargo_type_order_nd
insert overwrite table dws_trade_org_cargo_type_order_nd
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       city_name,
       cargo_type,
       cargo_type_name,
       recent_days,
       sum(order_count) as order_count,
       sum(order_amount) as order_amount
from dws_trade_org_cargo_type_order_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-10', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         cargo_type,
         cargo_type_name,
         recent_days;

-- 1.2 dws_trans_org_receive_1d
-- 1.2.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_receive_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount,
             dt
      from (select order_id,
                   amount,
                   sender_district_id,
                   dt
            from dwd_trans_receive_detail_inc) detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250718') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where dt = '20250718') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where dt = '20250718') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where dt = '20250718') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name,
               dt) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         dt;

-- 1.2.2 每日装载
insert overwrite table dws_trans_org_receive_1d
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount
      from (select order_id,
                   amount,
                   sender_district_id
            from dwd_trans_receive_detail_inc
            where dt = '2023-01-11') detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250718') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where dt = '20250718') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where dt = '20250718') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where dt = '20250718') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name;

-- 2.2 dws_trans_org_receive_nd
insert overwrite table dws_trans_org_receive_nd
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_org_receive_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-10', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;

-- 1.3 dws_trans_dispatch_1d
-- 1.3.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_dispatch_1d
    partition (dt)
select count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             dt,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      group by order_id,
               dt) distinct_info
group by dt;

-- 1.3.2 每日装载
insert overwrite table dws_trans_dispatch_1d
    partition (dt = '20250718')
select count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      where dt = '2023-01-11'
      group by order_id) distinct_info;

-- 2.3 dws_trans_dispatch_nd
insert overwrite table dws_trans_dispatch_nd
    partition (dt = '20250718')
select recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-10', -recent_days + 1)
group by recent_days;

-- 3.1 dws_trans_dispatch_td
-- 3.1.1 首日装载
insert overwrite table dws_trans_dispatch_td
    partition (dt = '20250718')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d;

-- 3.1.2 每日装载
insert overwrite table dws_trans_dispatch_td
    partition (dt = '20250718')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = date_add('2025-07-11', -1)
      union
      select order_count,
             order_amount
      from dws_trans_dispatch_1d
      where dt = '20250711') all_data;

-- 3.2 dws_trans_bound_finish_td
-- 3.2.1 首日装载
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '20250718')
select count(order_id)   order_count,
       sum(order_amount) order_amount
from (select order_id,
             max(amount) order_amount
      from dwd_trans_bound_finish_detail_inc
      group by order_id) distinct_info;

-- 3.2.2 每日装载
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '20250718')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_bound_finish_td
      where dt = date_add('2025-07-11', -1)
      union
      select count(order_id)   order_count,
             sum(order_amount) order_amount
      from (select order_id,
                   max(amount) order_amount
            from dwd_trans_bound_finish_detail_inc
            where dt = '2023-01-11'
            group by order_id) distinct_tb) all_data;

-- 1.4 dws_trans_org_truck_type_trans_finish_1d
-- 1.4.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt)
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec,
       dt
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec,
             dt
      from dwd_trade_trans_finish_inc) trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '20250718') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name,
         dt;

-- 1.4.2 每日装载
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt = '20250718')
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec
      from dwd_trade_trans_finish_inc
      where dt = '2023-01-11') trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '20250718') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name;

-- 2.4 dws_trans_shift_trans_finish_nd
insert overwrite table dws_trans_shift_trans_finish_nd
    partition (dt = '20250718')
select shift_id,
       if(org_level = 1, first.region_id, city.id)     city_id,
       if(org_level = 1, first.region_name, city.name) city_name,
       org_id,
       org_name,
       line_id,
       line_name,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_model_type,
       truck_model_type_name,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count,
       trans_finish_delay_count
from (select recent_days,
             shift_id,
             line_id,
             truck_id,
             start_org_id                                       org_id,
             start_org_name                                     org_name,
             driver1_emp_id,
             driver1_name,
             driver2_emp_id,
             driver2_name,
             count(id)                                          trans_finish_count,
             sum(actual_distance)                               trans_finish_distance,
             sum(finish_dur_sec)                                trans_finish_dur_sec,
             sum(order_num)                                     trans_finish_order_count,
             sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
      from dwd_trade_trans_finish_inc lateral view
          explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-01-10', -recent_days + 1)
      group by recent_days,
               shift_id,
               line_id,
               start_org_id,
               start_org_name,
               driver1_emp_id,
               driver1_name,
               driver2_emp_id,
               driver2_name,
               truck_id) aggregated
         left join
     (select id,
             org_level,
             region_id,
             region_name
      from dim_organ_full
      where dt = '20250718'
     ) first
     on aggregated.org_id = first.id
         left join
     (select id,
             parent_id
      from dim_region_full
      where dt = '20250718'
     ) parent
     on first.region_id = parent.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '20250718'
     ) city
     on parent.parent_id = city.id
         left join
     (select id,
             line_name
      from dim_shift_full
      where dt = '20250718') for_line_name
     on shift_id = for_line_name.id
         left join (
    select id,
           truck_model_type,
           truck_model_type_name
    from dim_truck_full
    where dt = '20250718'
) truck_info on truck_id = truck_info.id;

-- 1.5 dws_trans_org_deliver_suc_1d
-- 1.5.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count,
       dt
from (select order_id,
             sender_district_id,
             dt
      from dwd_trans_deliver_suc_detail_inc
      group by order_id, sender_district_id, dt) detail
         left join
     (select id org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '20250718') organ
     on detail.sender_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '20250718') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '20250718') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '20250718') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name,
         dt;

-- 1.5.2 每日装载
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count
from (select order_id,
             sender_district_id
      from dwd_trans_deliver_suc_detail_inc
      where dt = '2023-01-11'
      group by order_id, sender_district_id) detail
         left join
     (select id org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '20250718') organ
     on detail.sender_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '20250718') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '20250718') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '20250718') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name;

-- 2.5 dws_trans_org_deliver_suc_nd
insert overwrite table dws_trans_org_deliver_suc_nd
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count) order_count
from dws_trans_org_deliver_suc_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-01-10', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;

-- 1.6 dws_trans_org_sort_1d
-- 1.6.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_1d
    partition (dt)
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count,
       dt
from (select org_id,
             count(*) sort_count,
             dt
      from dwd_bound_sort_inc
      group by org_id, dt) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '20250718') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20250718') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20250718') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20250718') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;

-- 1.6.2 每日装载
insert overwrite table dws_trans_org_sort_1d
    partition (dt = '20250718')
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count
from (select org_id,
             count(*) sort_count
      from dwd_bound_sort_inc
      where dt = '2023-01-11'
      group by org_id) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '20250718') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20250718') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20250718') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20250718') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;

-- 2.6 dws_trans_org_sort_nd
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_nd
    partition (dt = '20250718')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(sort_count) sort_count
from dws_trans_org_sort_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-01-10', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;


-- 1. 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trade_order_detail_inc partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name  cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name   status_name,
       collect_type,
       dic_for_collect_type.name  collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(order_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);


-- 2 每日装载
insert overwrite table tms.dwd_trade_order_detail_inc partition (dt = '20250718')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name   cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name       status_name,
       collect_type,
       dic_for_collect_type.name collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             date_format(
                     from_utc_timestamp(
                                 to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                          substr(create_time, 12, 8))) * 1000,
                                 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
             ds
      from ods_order_cargo
      where ds = '20250710') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance
      from ods_order_info
      where ds = '20250710') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);



-- 2. dwd_trade_pay_suc_detail
-- 2.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trade_pay_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       payment_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(payment_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) payment_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 2.2 每日装载
with pay_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 payment_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') payment_time,
                       ds
                from ods_order_info
                where ds = '20250710'
                  and status = '60010'
                  and status = '60020'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and status = '60010'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710') cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710') info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trade_pay_suc_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       pay_info.payment_time,
       order_info.order_no,
       pay_info.status,
       pay_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       pay_info.payment_type,
       pay_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       pay_info.ds
from pay_info
         join order_info
              on pay_info.id = order_info.order_id;

-- 3. dwd_trade_order_cancel_detail
-- 3.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trade_order_cancel_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(cancel_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) cancel_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status = '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

-- 3.2 每日装载
with cancel_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 cancel_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') cancel_time,
                       ds
                from ods_order_info
                where ds = '20250710'
                  and status = '60999'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trade_order_cancel_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       cancel_info.cancel_time,
       order_info.order_no,
       cancel_info.status,
       cancel_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       cancel_info.ds
from cancel_info
         join order_info
              on cancel_info.id = order_info.order_id;

-- 4. dwd_trans_receive_detail
-- 4.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trans_receive_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(receive_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) receive_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 4.2 每日装载
with receive_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 receive_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') receive_time,
                       ds
                from ods_order_info
                where ds = '20250710'

                  and status = '60020'
                  and status = '60030'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_receive_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       receive_info.receive_time,
       order_info.order_no,
       receive_info.status,
       receive_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       receive_info.payment_type,
       receive_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       receive_info.ds
from receive_info
         join order_info
              on receive_info.id = order_info.order_id;

-- 5. dwd_trans_dispatch_detail
-- 5.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trans_dispatch_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(dispatch_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) dispatch_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 5.2 每日装载
with dispatch_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 dispatch_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') dispatch_time,
                       ds
                from ods_order_info
                where ds = '20250710'

                  and status = '60040'
                  and status = '60050'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_dispatch_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       dispatch_info.dispatch_time,
       order_info.order_no,
       dispatch_info.status,
       dispatch_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       dispatch_info.payment_type,
       dispatch_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       dispatch_info.ds
from dispatch_info
         join order_info
              on dispatch_info.id = order_info.order_id;

-- 6. dwd_trans_bound_finish_detail
-- 6.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trans_bound_finish_detail_inc partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       bound_finish_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(bound_finish_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) bound_finish_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60050'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 6.2 每日装载
with bound_finish_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 bound_finish_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') bound_finish_time,
                       ds
                from ods_order_info
                where ds = '20250710'

                  and status = '60050'
                  and status = '60060'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040' or
                status = '60050')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_bound_finish_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       bound_finish_info.bound_finish_time,
       order_info.order_no,
       bound_finish_info.status,
       bound_finish_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       bound_finish_info.payment_type,
       bound_finish_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       bound_finish_info.ds
from bound_finish_info
         join order_info
              on bound_finish_info.id = order_info.order_id;

-- 7. dwd_trans_deliver_suc_detail
-- 7.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trans_deliver_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(deliver_suc_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) deliver_suc_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60050'
        and status <> '60060'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 7.2 每日装载
with deliver_suc_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 deliver_suc_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') deliver_suc_time,
                       ds
                from ods_order_info
                where ds = '20250710'

                  and status = '60060'
                  and status = '60070'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040' or
                status = '60050' or
                status = '60060')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_deliver_suc_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       deliver_suc_info.deliver_suc_time,
       order_info.order_no,
       deliver_suc_info.status,
       deliver_suc_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       deliver_suc_info.payment_type,
       deliver_suc_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       deliver_suc_info.ds
from deliver_suc_info
         join order_info
              on deliver_suc_info.id = order_info.order_id;

-- 8. dwd_trans_sign_detail
-- 8.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trans_sign_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       sign_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(sign_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) sign_time
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60050'
        and status <> '60060'
        and status <> '60070'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 8.2 每日装载
with sign_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 sign_time,
                 ds
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') sign_time,
                       ds
                from ods_order_info
                where ds = '20250710'

                  and status = '60070'
                  and status = '60080'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo
               where ds = '20250710'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20250710'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_sign_detail_inc
partition(dt = '20250718')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       sign_info.sign_time,
       order_info.order_no,
       sign_info.status,
       sign_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       sign_info.payment_type,
       sign_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       sign_info.ds
from sign_info
         join order_info
              on sign_info.id = order_info.order_id;

-- 9. dwd_trade_order_process_inc
-- 9.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trade_order_process_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       end_date                              dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo
      where ds = '20250710'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance,
             if(status = '60080' or
                status = '60999',
                concat(substr(update_time, 1, 10)),
                '9999-12-31')                               end_date
      from ods_order_info
      where ds = '20250710'
        and is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 9.2 每日装载
with tmp
         as
         (select id,
                 order_id,
                 cargo_type,
                 cargo_type_name,
                 volumn_length,
                 volumn_width,
                 volumn_height,
                 weight,
                 order_time,
                 order_no,
                 status,
                 status_name,
                 collect_type,
                 collect_type_name,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 dt,
                 start_date,
                 end_date
          from dwd_trade_order_process_inc
          where dt = '9999-12-31'
          union
          select cargo.id,
                 order_id,
                 cargo_type,
                 dic_for_cargo_type.name               cargo_type_name,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight,
                 order_time,
                 order_no,
                 status,
                 dic_for_status.name                   status_name,
                 collect_type,
                 dic_for_collect_type.name             collect_type_name,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_for_payment_type.name             payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 ds,
                 date_format(order_time, 'yyyy-MM-dd') start_date,
                 '9999-12-31'                          end_date
          from (select id,
                       order_id,
                       cargo_type,
                       volume_length,
                       volume_width,
                       volume_height,
                       weight,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                    substr(create_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                       ds
                from ods_order_cargo
                where ds = '20250710'
                  ) cargo
                   join
               (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*') receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')   sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                       distance
                from ods_order_info
                where ds = '20250710'
                  ) info
               on cargo.order_id = info.id
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_cargo_type
               on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on info.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_collect_type
               on info.collect_type = cast(dic_for_cargo_type.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_payment_type
               on info.payment_type = cast(dic_for_payment_type.id as string)),
     inc
         as
         (select without_type_name.id,
                 status,
                 payment_type,
                 dic_for_payment_type.name payment_type_name
          from (select id,
                       status,
                       payment_type
                from (select id,
                             status,
                             payment_type,
                             row_number() over (partition by id order by ds desc) rn
                      from ods_order_info
                      where ds = '20250710'

                        and is_deleted = '0'
                     ) inc_origin
                where rn = 1) without_type_name
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_payment_type
               on without_type_name.payment_type = cast(dic_for_payment_type.id as string)
         )
insert overwrite table dwd_trade_order_process_inc
partition(dt)
select tmp.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       order_time,
       order_no,
       inc.status,
       status_name,
       collect_type,
       collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       inc.payment_type,
       inc.payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       dt,
       start_date,
       if(inc.status = '60080' or
          inc.status = '60999',
          '20250710', tmp.end_date)as  end_date,
       if(inc.status = '60080' or
          inc.status = '60999',
          '20250710', tmp.end_date)as dt
from tmp
         left join inc
                   on tmp.order_id = inc.id;

-- 10. dwd_trans_trans_finish
-- 10.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trans_finish_inc   partition (dt)
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time,
       actual_distance,
       finish_dur_sec,
       ds,
       dt
from (select id,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
             truck_id,
             md5(truck_no)                                                                      truck_no,
             date_format(from_utc_timestamp(
                                 cast(actual_start_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_end_time,

             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
             ds,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd')                                                                dt
      from ods_transport_task
      where ds = '20250710'
        and is_deleted = '0'
        and actual_end_time is not null) info
         left join
     (select id,
             estimated_time
      from dim_shift_full
      where dt = '20250710') dim_tb
     on info.shift_id = dim_tb.id;

-- 10.2 每日装载
insert overwrite table dwd_trade_trans_finish_inc
    partition (dt = '20250710')
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time,
       actual_distance,
       finish_dur_sec,
       ds
from (select id,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
             truck_id,
             md5(truck_no)                                                                      truck_no,
             date_format(from_utc_timestamp(
                                 cast(actual_start_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_end_time,
             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
             ds                                                                                       ds
      from ods_transport_task
      where ds = '20250710'

        and actual_end_time is null
        and actual_end_time is not null
        and is_deleted = '0') info
         left join
     (select id,
             estimated_time
      from dim_shift_full
      where dt = '20250710') dim_tb
     on info.shift_id = dim_tb.id;

-- 11. dwd_bound_inbound
-- 11.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_inbound_inc
    partition (dt)
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')as  inbound_time,
       inbound_emp_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound
where ds = '20250710';

-- 11.2 每日装载
insert overwrite table dwd_bound_inbound_inc
    partition (dt = '20250710')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')as  inbound_time,
       inbound_emp_id
from ods_order_org_bound
where ds = '20250710';

-- 12. dwd_bound_sort
-- 12.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_sort_inc
    partition (dt)
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')as  sort_time,
       sorter_emp_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound
where ds = '20250710'
  and sort_time is not null;

-- 12.2 每日装载
insert overwrite table dwd_bound_sort_inc
    partition (dt = '20250710')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')as sort_time,
       sorter_emp_id
from ods_order_org_bound
where ds = '20250710'
  and sort_time is null
  and sort_time is not null
  and is_deleted = '0';

-- 13. dwd_bound_outbound
-- 13.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_outbound_inc
    partition (dt)
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')as outbound_time,
       outbound_emp_id,
       date_format(from_utc_timestamp(
                           cast(outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound
where ds = '20250710'
  and outbound_time is not null;

-- 13.2 每日装载
insert overwrite table dwd_bound_outbound_inc
    partition (dt = '20250710')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')as outbound_time,
       outbound_emp_id
from ods_order_org_bound
where ds = '20250710'
  and outbound_time is null
  and outbound_time is not null
  and is_deleted = '0';


"""