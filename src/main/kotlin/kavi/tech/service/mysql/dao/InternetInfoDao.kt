package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.CallLog
import kavi.tech.service.mysql.entity.InternetInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class InternetInfoDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 新增记录
     * */
    private fun insert(internetInfo: InternetInfo, conn: SQLConnection): Single<InternetInfo> {
        val sql_str = SQL.init {
            INSERT_INTO(internetInfo.tableName())
            internetInfo.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        return this.update(conn, sql_str).map {
            internetInfo.apply { this.id = it.keys.getLong(0) }
        }
    }

    fun internetInfoDataInsert(data: List<JsonObject>): Single<UpdateResult> {

        println(" 上网记录存入mysql: .....")
        val internetInfoList = ArrayList<InternetInfo>()
        data.forEach {
            println("internetData：$it")
            // 运营商类型  移动：CMCC 联通：CUCC 电信：CTCC
            val operator = it.getString("operator")
            val mobile = it.getString("mobile")
            val bill_month = it.getString("bill_month")
            val task_id = it.getString("mid")
            val dataOut = it.getJsonObject("data")
            if (!dataOut.isEmpty) {

                when (operator) {
                    // 移动数据提取
                    "CMCC" -> {

                        dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                            val InternetInfo_s = InternetInfo()
                            InternetInfo_s.mobile = mobile
                            InternetInfo_s.bill_month = bill_month
                            InternetInfo_s.task_id = task_id
                            val obj = JsonObject(mutableEntry.toString())

                            cmcc(InternetInfo_s, obj)
                            internetInfoList.add(InternetInfo_s)
                        }
                    }
                    // 联通数据提取
                    "CUCC" -> {
                        dataOut.getJsonArray("pagelist").forEachIndexed { index, mutableEntry ->
                            val InternetInfo_s = InternetInfo()
                            InternetInfo_s.mobile = mobile
                            InternetInfo_s.bill_month = bill_month
                            InternetInfo_s.task_id = task_id
                            val obj = JsonObject(mutableEntry.toString())

                            cucc(InternetInfo_s, obj)
                            internetInfoList.add(InternetInfo_s)
                        }


                    }
                    // 电信数据提取
                    "CTCC" -> {
                    }
                }

            }
        }
        println("internetInfoList:${internetInfoList.size}" + internetInfoList.toString())
        if (internetInfoList.size > 0) {
            return insertBybatch(InternetInfo(), internetInfoList)
        } else {
            return Single.just(UpdateResult())
        }

    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(internetInfo: InternetInfo, valueList: List<InternetInfo>): Single<UpdateResult> {

        val sql = SQL.init {
            BATCH_INSERT_INTO(internetInfo.tableName())
            BATCH_INTO_COLUMNS(
                "task_id",
                "bill_month",
                "mobile",
                "start_time",
                "comm_plac",
                "net_play_type",
                "net_type",
                "comm_time",
                "sum_flow",
                "meal",
                "comm_fee",
                "carrier_001",
                "carrier_002",
                "created_at",
                "deleted_at"
            )
            println("valueList:$valueList")
            valueList.map {
                val ss = it.preInsert()

                BATCH_INTO_VALUES(

                    '"' + ss["task_id"].toString() + '"',
                    '"' + ss["bill_month"].toString() + '"',
                    '"' + ss["mobile"].toString() + '"',
                    '"' + ss["start_time"].toString() + '"',
                    '"' + ss["comm_plac"].toString() + '"',
                    '"' + ss["net_play_type"].toString() + '"',
                    '"' + ss["net_type"].toString() + '"',
                    '"' + ss["comm_time"].toString() + '"',
                    '"' + ss["sum_flow"].toString() + '"',
                    '"' + ss["meal"].toString() + '"',
                    ss["comm_fee"],
                    '"' + ss["carrier_001"].toString() + '"',
                    '"' + ss["carrier_002"].toString() + '"',
                    '"' + ss["created_at"].toString() + '"',
                    '"' + ss["deleted_at"].toString() + '"'
                )
            }

        }

        println("sql=$sql")

        return this.client.rxGetConnection().flatMap { conn ->
            val startTime = System.currentTimeMillis()
            conn.rxUpdate(sql).doAfterTerminate {
                conn.close()
                println("上网流量批量插入 执行时间：${System.currentTimeMillis() - startTime}ms")
            }
        }

    }

    /**
     * 通过task_id查询数据是否已经入库
     * */
    private fun selectBeforeInsert(internetInfo: InternetInfo): Single<ResultSet> {
        val sql = SQL.init {
            SELECT("id")
            FROM(InternetInfo.tableName)
            WHERE(Pair("task_id", internetInfo.task_id))
        }
        println("selectBeforeInsert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

    /**
     * 移动-上网记录数据提取
     */
    private fun cmcc(internetInfo: InternetInfo, obj: JsonObject) {
        internetInfo.start_time = obj.getString("startTime")
        internetInfo.comm_plac = obj.getString("commPlac")
        // 上网方式
        internetInfo.net_play_type = obj.getString("netPlayType")
        // 网络类型
        internetInfo.net_type = obj.getString("netType")
        // 总时长
        internetInfo.comm_time = obj.getString("commTime")
        // 总流量
        internetInfo.sum_flow = obj.getString("sumFlow").replace("KB","")

        // 套餐优惠
        internetInfo.meal = obj.getString("meal")

        val commFee = when (obj.getString("commFee")) {
            null -> "0.00"
            "" -> "0.00"
            else -> obj.getString("commFee")
        }

        // 费用 原始数据单位是元  转换成分后存储
        internetInfo.comm_fee = (commFee.toDouble() * 100).toInt()
        // 预留字段
        internetInfo.carrier_001 = ""
        internetInfo.carrier_002 = ""

    }

    /**
     * 联通-上网记录数据提取
     */
    private fun cucc(internetInfo: InternetInfo, obj: JsonObject) {
        internetInfo.start_time = obj.getString("begindateformat") + " " + obj.getString("begintimeformat")
        // 通信地点 无数据
        internetInfo.comm_plac = obj.getString("homearea")
        // 上网方式
        internetInfo.net_play_type = obj.getString("roamstat") //(国际漫游/国内)
        // 网络类型
        internetInfo.net_type = obj.getString("nettypeformat") //4g网络
        // 总时长 单位s
        internetInfo.comm_time = obj.getString("longhour")
        // 总流量
        val pertotalsm = when (obj.getString("pertotalsm")) {
            null -> "0.00"
            "" -> "0.00"
            else -> obj.getString("pertotalsm")
        }
        internetInfo.sum_flow = (pertotalsm.toDouble() * 1000).toInt().toString()
        // 套餐优惠
        internetInfo.meal = obj.getString("deratefee")
        // 费用 原始数据单位是元  转换成分后存储
        val totalfee = when (obj.getString("totalfee")) {
            null -> "0.00"
            "" -> "0.00"
            else -> obj.getString("totalfee")
        }
        internetInfo.comm_fee = (totalfee.toDouble() * 100).toInt()
        // 预留字段
        internetInfo.carrier_001 = ""
        internetInfo.carrier_002 = ""


    }

    /**
     * 电信-上网记录数据提取
     */
    private fun ctcc(internetInfo: InternetInfo, obj: JsonObject) {

        // 预留字段
        internetInfo.carrier_001 = ""
        internetInfo.carrier_002 = ""
    }


}
