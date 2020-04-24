package kavi.tech.service.mysql.dao

import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable

/**
 * 手机号基本信息
 */
@Repository
class CellPhoneDao @Autowired constructor(
    private val client: AsyncSQLClient
) {

    /**
     * 获取手机号基本信息
     * @param mobile 手机号
     */
    fun getCellPhoneInfo(mobile: String,taskId:String): Observable<ResultSet> {
        val sql = "select\n" +
                "cb.mobile as mobile,\n" +
                "cb.`name` as carrier_name,\n" +
                "cb.`idcard` as carrier_idcard,\n" +
                "cb.`in_net_date` as reg_time,\n" +
                "TIMESTAMPDIFF(MONTH,cb.`in_net_date`, DATE_FORMAT(now(), '%Y-%m-%d')) as in_time,\n" +
                "cb.user_email as email,\n" +
                "concat(cb.`province`,cb.`city`) as address,\n" +
                "case cb.`reliability`\n" +
                "when 1 then '实名认证'\n" +
                "when -1 then '未知'\n" +
                "else '未实名' end as reliability,\n" +
                "'未知' as phone_attribution,\n" +
                "cb.`user_address` as address,\n" +
                "'未知' as available_balance,\n" +
                "cb.`package_name` as package_name,\n" +
                "FROM_UNIXTIME((cb.`created_at` / 1000),'%Y-%m-%d %H:%i:%S') as bill_certification_day\n" +
                "from `carrier_baseinfo` cb where cb.`mobile` = '$mobile' and cb.task_id = '$taskId'"
        return this.client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate(conn::close)
        }.toObservable()
    }
}
