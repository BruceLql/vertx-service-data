package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.utils.DateUtils
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.entity.ResultData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

/**
 * 用户行为
 */
@Repository
class UserBehaviorDao @Autowired constructor
    (private val client: AsyncSQLClient) : AbstractDao<ResultData>(client) {
    override val log: Logger = logger(this::class)

    /**
     * 返回用户行为分析
     * @param mobile 手机号
     * @param taskId mongo ID
     */
    fun getCellBehavior(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        return this.client.rxGetConnection().flatMap { conn ->
            val list =
                (0..5).map { d -> sqlExecuteQuery(conn, mobile, taskId, dateList[d]).map { it.rows[0] }.toObservable() }
            Observable.concat(list).toList().toSingle().doAfterTerminate(conn::close)
        }
    }


    /**
     * sql查询
     */
    fun sqlExecuteQuery(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "select distinct\n" +
                "    (select count(*) from carrier_sms cs where cs.mobile='$mobile' and cs.task_id='$taskId' and cs.bill_month='$moth' and cs.deleted_at=0) as sms_cnt,\n" +
                "    '$mobile' as cell_phone_num,\n" +
                "    if(LOCATE('.',b.net_flow)>1,SUBSTR(b.net_flow,1,LOCATE('.',b.net_flow) -1),b.net_flow) as net_flow,\n" +
                "    if(LOCATE('.',b.total_amount)>1,SUBSTR(b.total_amount,1,LOCATE('.',b.total_amount) -1),b.total_amount) as total_amount,\n" +
                "    '$moth' as cell_mth,\n" +
                "    ifnull(c.cell_loc,'') as cell_loc,\n" +
                "    ifnull(c.cell_operator,'') as cell_operator,\n" +
                "    ifnull(c.cell_operator_zh,'') as cell_operator_zh,\n" +
                "    (select count(*) as call_cnt  from carrier_voicecall cv where cv.mobile='$mobile' and cv.task_id='$taskId' and cv.bill_month='$moth' and cv.deleted_at=0) as call_cnt,\n" +
                "    (select floor(ifnull(sum(cv.duration_in_second),0)) from carrier_voicecall cv where cv.mobile='$mobile' and cv.task_id='$taskId' and cv.bill_month='$moth' and cv.deleted_at=0) as call_time,\n" +
                "    (select count(*) as dial_cnt from carrier_voicecall cv where cv.dial_type = 'DIAL' and cv.mobile='$mobile' and cv.task_id='$taskId' and cv.bill_month='$moth' and cv.deleted_at=0) as dial_cnt,\n" +
                "    (select floor(ifnull(sum(cv.duration_in_second),0)) as dial_time from carrier_voicecall cv where cv.dial_type = 'DIAL' and cv.mobile='$mobile' and cv.task_id='$taskId' and cv.bill_month='$moth' and cv.deleted_at=0) as dial_time,\n" +
                "    (select count(*) as dialed_cnt from carrier_voicecall cv where cv.dial_type = 'DIALED' and cv.mobile='$mobile' and cv.task_id='$taskId' and cv.bill_month='$moth' and cv.deleted_at=0) as dialed_cnt,\n" +
                "    (select floor(ifnull(sum(cv.duration_in_second),0)) as dialed_time from carrier_voicecall cv where cv.dial_type = 'DIALED' and cv.mobile='$mobile' and cv.task_id='$taskId' and cv.bill_month='$moth' and cv.deleted_at=0) as dialed_time,\n" +
                "    (select count(*) as rechange_cnt from carrier_recharge cr where cr.mobile='$mobile' and cr.task_id='$taskId' and DATE_FORMAT(date(cr.recharge_time),'%Y-%m')='$moth' and cr.deleted_at=0) as rechange_cnt,\n" +
                "    (select floor(ifnull(sum(cr.amount_money),0)) as rechange_amount  from carrier_recharge cr where cr.mobile='$mobile' and cr.task_id='$taskId' and DATE_FORMAT(date(cr.recharge_time),'%Y-%m')='$moth' and cr.deleted_at=0) as rechange_amount\n" +
                "from (select ifnull(sum(cnd.sum_flow),0) as net_flow,ifnull(sum(cnd.comm_fee),0) as total_amount , cnd.mobile , cnd.task_id from carrier_net_detial cnd where cnd.mobile='$mobile' and cnd.task_id='$taskId' and cnd.bill_month='$moth' and cnd.deleted_at=0) as b\n" +
                "left join (select cb.city as cell_loc, cb.carrier as cell_operator,\n" +
                "                  CASE cb.carrier\n" +
                "                      WHEN 'CMCC' THEN '中国移动'\n" +
                "                      WHEN 'CUCC' THEN '中国联通'\n" +
                "                      WHEN 'CTCC' THEN '中国电信'\n" +
                "                      ELSE\n" +
                "                          '其他'\n" +
                "                      END as cell_operator_zh,\n" +
                "                  cb.task_id,cb.mobile\n" +
                "           from carrier_baseinfo cb where cb.mobile = '$mobile' and cb.task_id='$taskId') as c on b.task_id=c.task_id and b.mobile=c.mobile"
        return this.query(conn, sql)
    }
}
