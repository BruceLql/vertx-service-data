package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.splitYmd
import kavi.tech.service.common.strategry.HashStrategy
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.UserInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class UserInfoDao @Autowired constructor(
    private val client: AsyncSQLClient,
    private val strategy: HashStrategy
) : AbstractDao<UserInfo>(client) {
    override val log: Logger = logger(this::class)


    fun userInfoDataInsert(data: List<JsonObject>): Single<UpdateResult> {

        println(" 基本信息存入mysql: .....")
        val userInfo = UserInfo()
        data.forEach {
            userInfo.mobile = it.getString("mobile")
            userInfo.task_id = it.getString("mid")
            userInfo.carrier = it.getString("operator")  // CMCC 移动
            val dataOut = it.getJsonObject("data")
            if (!dataOut.isEmpty) {
                userInfo.name = dataOut.getString("name")
                userInfo.state = dataOut.getString("status")
                userInfo.reliability = dataOut.getString("realNameInfo")
                userInfo.brand = dataOut.getString("brand")
                userInfo.package_name = ""
                userInfo.in_net_date = dataOut.getString("inNetDate")
                userInfo.net_age = dataOut.getString("netAge")
                if (userInfo.carrier.equals("CMCC")) {
                    userInfo.net_age = splitYmd(dataOut.getString("netAge")).toString()
                }

                userInfo.level = dataOut.getString("starLevel")
                userInfo.user_lever = dataOut.getString("level")
                userInfo.star_score = dataOut.getString("star_score")
                userInfo.user_email = dataOut.getString("email")
                userInfo.zip_code = dataOut.getString("zipCode")
                userInfo.user_address = dataOut.getString("address")
                userInfo.idcard = dataOut.getString("certnum") // 仅联通有
                userInfo.city = dataOut.getString("city_name")  //城市
                userInfo.province = dataOut.getString("area")  // 省份

                println("userInfo====" + userInfo.toString())
            }

        }
        if (!userInfo.name.isNullOrEmpty()) {
            return insert(userInfo)
        } else {
            return Single.just(UpdateResult())
        }

    }

    /**
     * 新增记录
     * */
    private fun insert(userInfo: UserInfo): Single<UpdateResult> {
        val sql = SQL.init {
            INSERT_INTO(userInfo.tableName())
            userInfo.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        println("insert sql：" + sql)
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxUpdate(sql).doAfterTerminate(conn::close)
        }
    }

    /**
     * 查询是否已经入库
     * */
    private fun selectBeforeInsert(userInfo: UserInfo): Single<ResultSet> {
        val sql = SQL.init {
            SELECT("id");
            FROM(UserInfo.tableName);
            WHERE(Pair("task_id", userInfo.task_id))
        }
        println("selectBeforeInsert sql：" + sql)
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

}
