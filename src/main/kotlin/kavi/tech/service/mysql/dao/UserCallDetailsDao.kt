package kavi.tech.service.mysql.dao

import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable

/**
 * 用户通话详单
 */
@Repository
class UserCallDetailsDao @Autowired constructor
    (private val client: AsyncSQLClient){


    /**
     * 通话详单
     * @param mobile 手机号
     * @param taskId mongo id
     */
    fun getUserDetails(mobile: String, taskId: String):Observable<ResultSet>{
        val sql = "select \n" +
                "cv.location as city,\n" +
                "'未知' as p_relation,\n" +
                "cv.peer_number as peer_num,\n" +
                "'未知' as group_name,\n" +
                "'未知' as company_name,\n" +
                "count(if((date_sub(curdate(), interval 7 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt_1w,\n" +
                "count(if((date_sub(curdate(), interval 30 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt_1m,\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt_3m,\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt_6m,\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as SIGNED) as call_time_3m,\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as SIGNED) as call_time_6m,\n" +
                "count(if((date_sub(curdate(), interval 30 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and cv.dial_type='DIAL',true,null)) as dial_cnt_3m,\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time)) and cv.dial_type='DIAL'),true,null)) as dial_cnt_6m,\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and cv.dial_type='DIAL',cv.duration_in_second,0)),0) as SIGNED) as dial_time_3m,\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and cv.dial_type='DIAL',cv.duration_in_second,0)),0) as SIGNED) as dial_time_6m,\n" +
                "count(if((date_sub(curdate(), interval 30 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and cv.dial_type='DIALED',true,null)) as dialed_cnt_3m,\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time)) and cv.dial_type='DIALED'),true,null)) as dialed_cnt_6m,\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and cv.dial_type='DIALED',cv.duration_in_second,0)),0) as SIGNED) as dialed_time_3m,\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and cv.dial_type='DIALED',cv.duration_in_second,0)),0) as SIGNED) as dialed_time_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 6 and 12 then 1 else null end ) as call_cnt_morning_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 6 and 12 then 1 else null end ) as call_cnt_morning_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 13 then 1 else null end ) as call_cnt_noon_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 13 then 1 else null end ) as call_cnt_noon_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 18 then 1 else null end ) as call_cnt_afternoon_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 18 then 1 else null end ) as call_cnt_afternoon_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 18 and 24 then 1 else null end ) as call_cnt_evening_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 18 and 24 then 1 else null end ) as call_cnt_evening_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 0 and 6 then 1 else null end ) as call_cnt_night_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 0 and 6 then 1 else null end ) as call_cnt_night_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and  date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=1) then 1 else null end) as call_cnt_weekday_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=1) then 1 else null end) as call_cnt_weekday_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and  date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=2) then 1 else null end) as call_cnt_weekend_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=2) then 1 else null end) as call_cnt_weekend_6m,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and  date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=0) then 1 else null end) as call_cnt_holiday_3m,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=0) then 1 else null end) as call_cnt_holiday_6m,\n" +
                "0 as call_if_whole_day_3m,\n" +
                "0 as call_if_whole_day_6m,\n" +
                "min(concat(substr(cv.bill_month,1,4),'-',cv.time)) as trans_start,\n" +
                "max(concat(substr(cv.bill_month,1,4),'-',cv.time)) as trans_end\n" +
                "from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id='$taskId'  group by cv.peer_number order by dial_time_3m desc;"
        return this.client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate(conn::close)
        }.toObservable()
    }
}
