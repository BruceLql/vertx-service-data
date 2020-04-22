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


    fun userInfoDataInsert(data: List<JsonObject>): Single<UpdateResult> {

        println(" 基本信息存入mysql: .....")
        val userInfo = UserInfo()
        data.forEach {
            println("基本信息:" + it)
            userInfo.mobile = it.getString("mobile")
            userInfo.task_id = it.getString("mid")
            userInfo.carrier = it.getString("operator")  // CMCC 移动
            val dataOut = it.getJsonObject("data")


            if (!dataOut.isEmpty) {
                when (userInfo.carrier) {
                    "CMCC" -> {
                        cmcc(userInfo,dataOut)

                    }
                    "CUCC" -> {
                        cucc(userInfo,dataOut)
                    }
                    "CTCC" -> {

                    }
                }

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
     * 移动-短信数据提取
     */
    private fun cmcc(userInfo: UserInfo, dataOut: JsonObject) {
        userInfo.name = dataOut.getString("name")
        userInfo.state = dataOut.getString("status")
        userInfo.real_name_info = dataOut.getString("realNameInfo")
        userInfo.reliability = dataOut.getString("realNameInfo")
        userInfo.brand = dataOut.getString("brand")
        userInfo.package_name = ""
        userInfo.in_net_date = dataOut.getString("inNetDate")
        userInfo.net_age = dataOut.getString("netAge")
        userInfo.net_age = splitYmd(dataOut.getString("netAge")).toString()


        userInfo.level = dataOut.getString("starLevel")
        userInfo.user_lever = dataOut.getString("level")
        userInfo.star_score = dataOut.getString("star_score")
        userInfo.user_email = dataOut.getString("email")
        userInfo.zip_code = dataOut.getString("zipCode")
        userInfo.user_address = dataOut.getString("address")
        userInfo.idcard = dataOut.getString("certnum") // 仅联通有
        userInfo.city = dataOut.getString("city_name")  //城市
        userInfo.province = dataOut.getString("area")  // 省份


        // 预留字段
        userInfo.carrier_001 = ""
        userInfo.carrier_002 = ""
    }

    /**
     * 联通-短信数据提取
     */
    private fun cucc(userInfo: UserInfo, dataOut: JsonObject) {

        userInfo.name = dataOut.getJsonObject("userinfo").getString("custName")
        userInfo.state = dataOut.getJsonObject("userinfo").getString("status")
        userInfo.real_name_info = dataOut.getJsonObject("userinfo").getString("verifyState")
        userInfo.reliability = dataOut.getJsonObject("userinfo").getString("verifyState")
        userInfo.brand = dataOut.getJsonObject("userinfo").getString("brand_name")
        userInfo.package_name = dataOut.getJsonObject("userinfo").getString("packageName")
        userInfo.in_net_date = dataOut.getJsonObject("userinfo").getString("opendate")
        userInfo.net_age = dataOut.getString("netAge")  // 根据opendate 计算

        userInfo.level = dataOut.getJsonObject("userinfo").getString("custlvl")
        userInfo.user_lever = dataOut.getJsonObject("userinfo").getString("custlvl")
        userInfo.star_score = ""
        userInfo.user_email = dataOut.getJsonObject("result").getJsonObject("MyDetail").getString("sendemail")
        userInfo.zip_code = ""
        userInfo.user_address = dataOut.getJsonObject("result").getJsonObject("MyDetail").getString("certaddr")
        userInfo.idcard = dataOut.getJsonObject("userinfo").getString("certnum") // 仅联通有
        userInfo.city = dataOut.getJsonObject("userinfo").getString("citycode")  //城市
        userInfo.province = dataOut.getJsonObject("userinfo").getString("provincecode")  // 省份


        // 预留字段
        userInfo.carrier_001 = ""
        userInfo.carrier_002 = ""

    }

    /**
     * 电信-短信数据提取
     */
    private fun ctcc(userInfo: UserInfo, obj: JsonObject) {

        // 预留字段
        userInfo.carrier_001 = ""
        userInfo.carrier_002 = ""
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
                println("户基本信息:" + it.toJson())
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
        println("----------用户基本信原始数据--------:$sql")
        return this.query(conn, sql)
    }
}
