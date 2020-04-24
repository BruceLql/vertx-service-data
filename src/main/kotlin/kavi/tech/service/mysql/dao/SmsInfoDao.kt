package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.utils.DateUtils
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.CallLog
import kavi.tech.service.mysql.entity.SmsInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

@Repository
class SmsInfoDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 新增记录
     * */
    private fun insert(conn: SQLConnection, smsInfo: SmsInfo): Single<SmsInfo> {
        val sql = SQL.init {
            INSERT_INTO(smsInfo.tableName())

            smsInfo.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        return this.update(conn, sql).map {
            smsInfo.apply { this.id = it.keys.getLong(0) }
        }
    }

    fun smsInfoDataInsert(data: List<JsonObject>): Single<UpdateResult> {

        println(" 短信记录存入mysql: .....")
        val smsInfoList = ArrayList<SmsInfo>()
        data.forEach {
            println("smsData：$it")
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
                        if (dataOut.getJsonArray("data").size() >= 1) {
                            dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                                val smsInfo_s = SmsInfo()
                                smsInfo_s.mobile = mobile
                                smsInfo_s.bill_month = bill_month
                                smsInfo_s.task_id = task_id
                                val obj = JsonObject(mutableEntry.toString())

                                cmcc(smsInfo_s, obj)
                                smsInfoList.add(smsInfo_s)
                            }
                        }
                    }

                    // 联通数据提取
                    "CUCC" -> {
                        val pageMap = dataOut.getJsonObject("pageMap")
                        println("===== pageMap:$pageMap")
                        println("===== pageMap.result:${pageMap.getJsonArray("result")}")
                        dataOut.getJsonObject("pageMap").getJsonArray("result").forEachIndexed { index, mutableEntry ->
                            val smsInfo_s = SmsInfo()
                            smsInfo_s.mobile = mobile
                            smsInfo_s.bill_month = bill_month
                            smsInfo_s.task_id = task_id
                            val obj = JsonObject(mutableEntry.toString())

                            cucc(smsInfo_s, obj)
                            smsInfoList.add(smsInfo_s)
                        }

                    }

                    // 电信数据提取
                    "CTCC" -> {
                    }

                }
            }
        }
        println("smsInfoList:${smsInfoList.size}" + smsInfoList.toString())
        return insertBybatch(smsInfoList)

    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(valueList: List<SmsInfo>): Single<UpdateResult> {

        if(valueList.isNullOrEmpty()){
            return Single.just(UpdateResult())
        }

        val sql = SQL.init {
            BATCH_INSERT_INTO(SmsInfo.tableName)
            BATCH_INTO_COLUMNS(
                "task_id",
                "mobile",
                "bill_month",
                "time",
                "peer_number",
                "location",
                "send_type",
                "msg_type",
                "service_name",
                "fee",
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
                    '"' + ss["mobile"].toString() + '"',
                    '"' + ss["bill_month"].toString() + '"',
                    '"' + ss["time"].toString() + '"',
                    '"' + ss["peer_number"].toString() + '"',
                    '"' + ss["location"].toString() + '"',
                    '"' + ss["send_type"].toString() + '"',
                    '"' + ss["msg_type"].toString() + '"',
                    '"' + ss["service_name"].toString() + '"',
                    ss["fee"],
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
                println("执行时间：${System.currentTimeMillis() - startTime}ms")
            }
        }

    }

    /**
     * 通过task_id查询数据是否已经入库
     * */
    private fun selectBeforeInsert(smsInfo: SmsInfo): Single<ResultSet> {
        val sql = SQL.init {
            SELECT("id")
            FROM(SmsInfo.tableName)
            WHERE(Pair("task_id", smsInfo.task_id))
        }
        println("selectBeforeInsert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

    /**
     * 移动-短信数据提取
     */
    private fun cmcc(smsInfo: SmsInfo, obj: JsonObject) {
        smsInfo.time = obj.getString("startTime")
        smsInfo.location = obj.getString("commPlac")
        // 通信方式 （SMS-短信; MSS-彩信）
        smsInfo.msg_type = obj.getString("infoType")
        // 接收类型 SEND-发送; RECEIVE-收取
        smsInfo.send_type = when (obj.getString("commMode")) {
            "接收" -> "RECEIVE"
            "发送" -> "SEND"
            else -> obj.getString("commMode")
        }
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = obj.getString("meal")
        // 对方号码
        smsInfo.peer_number = obj.getString("anotherNm")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = (obj.getString("commFee").toDouble() * (100)).toInt()

        // 预留字段
        smsInfo.carrier_001 = ""
        smsInfo.carrier_002 = ""
    }

    /**
     * 联通-短信数据提取
     */
    private fun cucc(smsInfo: SmsInfo, obj: JsonObject) {
        smsInfo.time = obj.getString("smsdate") + " " + obj.getString("smstime")
        // 通信地点 无数据
        smsInfo.location = ""
        // 通信方式 （SMS-短信; MSS-彩信） //(01-国内短信/02-国际短信/03-国内彩信)
        smsInfo.msg_type = when (obj.getString("businesstype")) {
            "01" -> "SMS"
            "02" -> "SMS"
            "03" -> "MSS"
            else -> obj.getString("businesstype")
        }
        // 接收类型 (SEND-发送; RECEIVE-收取)
        smsInfo.send_type = when (obj.getString("smstype")) {
            // 1接收
            "1" -> "RECEIVE"
            "2" -> "SEND"
            else -> obj.getString("smstype")
        }
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = ""
        // 对方号码
        smsInfo.peer_number = obj.getString("othernum")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = (obj.getString("fee").toDouble() * (100)).toInt()

        // 预留字段
        smsInfo.carrier_001 = ""
        smsInfo.carrier_002 = ""

    }

    /**
     * 电信-短信数据提取
     */
    private fun ctcc(smsInfo: SmsInfo, obj: JsonObject) {


    }


    /**
     * 获取近六个月 短信记录原始数据
     */
    fun querySmsRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("=========:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    var json = JsonObject()
                    sqlExecuteQuery2(conn, mobile, taskId, dateList[d]).map {

                        json.put("data", if (it.numRows == 0) JsonObject() else it.rows)
                    }.toObservable()

                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * 获取近六个月 短信记录原始数据统计信息
     */
    fun querySmsCountRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("=========:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    var json = JsonObject()
                    sqlExecuteQuery1(conn, mobile, taskId, dateList[d]).map {

                        println("result1" + it.toJson())
                        json.put("data", if (it.numRows == 0) JsonObject().put("bill_month",dateList[d].let { it -> it.substring(0,4)+"-"+it.substring(4,6) }).put("total_size",0).put("items",ArrayList<JsonObject>()) else it.rows[0])

                    }.toObservable()
                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * sql查询 按月汇总短信数据
     */
    fun sqlExecuteQuery1(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT CONCAT_WS('-',LEFT(bill_month,4),RIGHT(bill_month,2)) as bill_month , count(*) AS total_size FROM ${SmsInfo.tableName}\n" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n" +
                "GROUP BY bill_month\n" +
                "ORDER BY bill_month DESC"
        println("++++++++++++++++：" + sql)
        return this.query(conn, sql)
    }


    /**
     * sql查询 具体短信数据
     */
    fun sqlExecuteQuery2(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT CONCAT_WS(\"-\",LEFT(bill_month,4),time) as time,\n" +
                "cast(id as char ) as details_id,\n" +
                "location as location,\n" +
                "msg_type ,\n" +
                "send_type ,\n" +
                "peer_number,\n" +
                "service_name,\n" +
                "cast(fee as char ) as fee  " +
                "FROM ${SmsInfo.tableName}" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n" +
                "ORDER BY time DESC"
        println("------------------:" + sql)
        return this.query(conn, sql)
    }


}
