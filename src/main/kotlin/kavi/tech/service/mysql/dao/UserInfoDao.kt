package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.strategry.HashStrategy
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.User
import kavi.tech.service.mysql.entity.UserInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single
import rx.Subscription

@Repository
class UserInfoDao @Autowired constructor(
    private val client: AsyncSQLClient,
    private val strategy: HashStrategy
) : AbstractDao<UserInfo>(client) {
    override val log: Logger = logger(this::class)



    fun userInfoDataInsert(data: List<JsonObject>) {

        println(" 基本信息存入mysql: .....")
        val userInfo = UserInfo()
        data.forEach {
            userInfo.mobile = it.getString("mobile")
            userInfo.task_id = it.getString("mid")
            userInfo.province = it.getString("operator")  // CMCC 移动
            val dataOut = it.getJsonObject("data")
            if (dataOut.isEmpty) { return }
            userInfo.name = dataOut.getString("name")
            userInfo.state = dataOut.getString("status")
            userInfo.reliability = dataOut.getString("realNameInfo")
            userInfo.brand = dataOut.getString("brand")
//            userInfo.package_name = dataOut.getString("package_name")
            userInfo.in_net_date = dataOut.getString("inNetDate")
            userInfo.net_age = dataOut.getString("netAge")
            userInfo.net_age = dataOut.getString("netAge")
            userInfo.level = dataOut.getString("starLevel")
            userInfo.star_score = dataOut.getString("star_score")
            userInfo.user_email = dataOut.getString("email")
            userInfo.zip_code = dataOut.getString("zipCode")
            userInfo.user_address = dataOut.getString("address")
            userInfo.idcard = dataOut.getString("certnum") // 仅联通有
            userInfo.city = dataOut.getString("city_name")  //城市
            userInfo.province = dataOut.getString("area")  // 省份


            println(it.toString())
            println("userInfo===="+userInfo.toString())

        }
        insert(userInfo)
    }

    /**
     * 新增记录
     * */
    private fun insert( userInfo : UserInfo): Single<Void> {
        val sql = SQL.init {
            INSERT_INTO(userInfo.tableName())
            userInfo.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        println("sql："+sql)
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxExecute(sql).doAfterTerminate(conn::close)

        }
    }


}
