package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.splitYmd
import kavi.tech.service.common.strategry.HashStrategy
import kavi.tech.service.common.utils.DateUtils
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.PaymentRecord
import kavi.tech.service.mysql.entity.UserInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

@Repository
class UserInfoDao @Autowired constructor(
    private val client: AsyncSQLClient,
    private val strategy: HashStrategy
) : AbstractDao<UserInfo>(client) {
    override val log: Logger = logger(this::class)

    /**
     * 新增记录
     * */
    public fun insert(userInfo: UserInfo): Single<UpdateResult> {
        val sql = SQL.init {
            INSERT_INTO(userInfo.tableName())
            userInfo.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        println("insert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxUpdate(sql).doAfterTerminate(conn::close)
        }
    }


    /**
     * 获取 用户基本信息原始数据
     */
    fun queryUserInfoRaw6Month(mobile: String, taskId: String): Single<JsonObject> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)

        return this.client.rxGetConnection().flatMap { conn ->

            var jsonList = ArrayList<JsonObject>()
            var json = JsonObject()
            val reChargesObservable = sqlExecuteQuery(conn, mobile, taskId).map {
                log.info("户基本信息:" + it.toJson())
                json.put("data", if (it.numRows == 0) JsonObject() else it.rows)

            }.toObservable()

            Observable.concat(listOf(reChargesObservable)).toSingle().doAfterTerminate(conn::close)

        }

    }


    /**
     * sql查询 用户基本信原始数据
     */
    fun sqlExecuteQuery(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        // 联通的
        val sql = "SELECT\n" +
                "\t FROM_UNIXTIME(IFNULL( updated_at, created_at )/1000 ,'%Y-%m-%d %H:%i:%s') last_modify_time ,\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN reliability = \"\" THEN\n" +
                "\t\t\"-1\" ELSE reliability \n" +
                "\tEND AS reliability,\n" +
                "\tin_net_date AS open_time,\n" +
                "\t\"\" AS imsi,\n" +
                "\t 0 AS available_balance,\n" +
                "\tprovince,\n" +
                "\tcity,\n" +
                "\tstate,\n" +
                "\t 0 AS real_balance,\n" +
                "\tuser_email AS email,\n" +
                "\tuser_address AS address,\n" +
                "\tlevel,\n" +
                "\tmobile,\n" +
                " CASE\n" +
                "\t\tWHEN carrier = \"CMCC\" THEN\n" +
                "\t\t\"CHINA_MOBILE\" \n" +
                "\t\tWHEN carrier = \"CUCC\" THEN\n" +
                "\t\t\"CHINA_UNICOM\" ELSE \"CHINA_TELECOM\" \n" +
                "\tEND AS carrier,\n" +
                "\tidcard,\n" +
                "\tname,\n" +
                "\tpackage_name  "+
                "FROM ${UserInfo.tableName}" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n"
        log.info("----------用户基本信原始数据--------:$sql")
        return this.query(conn, sql)
    }
}
