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
                "cv.location,\n" +
                "'未知' as relation,\n" +
                "cv.peer_number,\n" +
                "'未知' as number_type,\n" +
                "'未知' as number_tag,\n" +
                "count(if((date_sub(curdate(), interval 7 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 7_call_count,\n" +
                "count(if((date_sub(curdate(), interval 30 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 30_call_count,\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 90_call_count,\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 180_call_count,\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as 90_shi_chang,\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as 180_shi_chang,\n" +
                "count(if((date_sub(curdate(), interval 30 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as 30_dial_count,\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time)) && cv.dial_type='DIAL'),true,null)) as 180_dial_count,\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)),0) as 90_dial_shi_chang,\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)),0) as 180_dial_shi_chang,\n" +
                "count(if((date_sub(curdate(), interval 30 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as 30_dialed_count,\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time)) && cv.dial_type='DIALED'),true,null)) as 180_dialed_count,\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)),0) as 90_dialed_shi_chang,\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)),0) as 180_dialed_shi_chang,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 6 and 12 then 1 else null end ) as 90_zao_chen_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 6 and 12 then 1 else null end ) as 180_zao_chen_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 13 then 1 else null end ) as 90_zhong_wu_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 13 then 1 else null end ) as 180_zhong_wu_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 18 then 1 else null end ) as 90_xia_wu_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 12 and 18 then 1 else null end ) as 180_xia_wu_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 18 and 24 then 1 else null end ) as 90_wan_shang_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 18 and 24 then 1 else null end ) as 180_wan_shang_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 0 and 6 then 1 else null end ) as 90_ling_chen_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and hour(str_to_date(concat(substr(cv.bill_month,1,4),'-',cv.time),'%Y-%m-%d %H:%i:%s')) between 0 and 6 then 1 else null end ) as 180_ling_chen_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and  date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=1) then 1 else null end) as 90_work_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=1) then 1 else null end) as 180_work_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and  date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=2) then 1 else null end) as 90_week_end_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=2) then 1 else null end) as 180_week_end_count,\n" +
                "count(case when(date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and  date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=0) then 1 else null end) as 90_jie_jia_ri_count,\n" +
                "count(case when(date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) and date(concat(substr(cv.bill_month,1,4),'-',cv.time)) in (select dt.day_time from days_type dt where dt.day_type=0) then 1 else null end) as 180_jie_jia_ri_count,\n" +
                "0 as 90_is_contact,\n" +
                "0 as 180_is_contact,\n" +
                "min(concat(substr(cv.bill_month,1,4),'-',cv.time)) as first_call_time,\n" +
                "max(concat(substr(cv.bill_month,1,4),'-',cv.time)) as last_call_time\n" +
                "from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id='$taskId'  group by cv.peer_number order by 90_dial_shi_chang desc;"
        return this.client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate(conn::close)
        }.toObservable()
    }
}
