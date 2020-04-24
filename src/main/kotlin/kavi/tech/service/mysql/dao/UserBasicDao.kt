package kavi.tech.service.mysql.dao

import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable


/**
 * 用户基本信息
 */
@Repository
class UserBasicDao @Autowired constructor(
    private val client: AsyncSQLClient
) {


    /**
     * 获取用户基本信息
     * @param idCard 身份证号
     */
    fun getUserBasicInfo(mobile: String,taskId:String): Observable<ResultSet> {

        val sql = "select\n" +
                "cb.`name` as name,\n" +
                "cb.`idcard` as id_card,\n" +
                "if((substr(substr(cb.`user_idcard`,length(cb.`user_idcard`) -1),1,1) % 2 > 0) ,'男','女') as gender,\n" +
                "cast((DATE_FORMAT(now(), '%Y') - substr(cb.`user_idcard`,7,4)) AS SIGNED) as age,\n" +
                "case\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0321' and '0420') > 0 then '白羊座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0421' and '0521') > 0 then '金牛座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0522' and '0621') > 0 then '双子座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0622' and '0722') > 0 then '巨蟹座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0723' and '0823') > 0 then '狮子座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0824' and '0923') > 0 then '处女座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0924' and '1023') > 0 then '天秤座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '1024' and '1122') > 0 then '天蝎座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '1123' and '1221') > 0 then '射手座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0121' and '0219') > 0 then '水瓶座'\n" +
                "when (substr(cb.`user_idcard`,11,4) between '0220' and '0320') > 0 then '双鱼座'\n" +
                "else '摩羯座'\n" +
                "end as constellation,\n" +
                "cb.`province` as province,\n" +
                "cb.`city` as city,\n" +
                "'未知' as region,\n" +
                "'未知' as native_place\n" +
                "from `carrier_baseinfo` cb where cb.`mobile` = '$mobile' and cb.task_id = '$taskId'"
        return this.client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate(conn::close)
        }.toObservable()
    }
}
