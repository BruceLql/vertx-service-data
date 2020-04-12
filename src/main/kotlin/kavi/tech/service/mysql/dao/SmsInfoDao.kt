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
import kavi.tech.service.mysql.entity.SmsInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
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

    fun smsInfoDataInsert(data: List<JsonObject>) {

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
            if (dataOut.isEmpty) {
                return
            }
            if (dataOut.getInteger("totalNum") < 1) {
                return
            }
            dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                val smsInfo_s = SmsInfo()
                smsInfo_s.mobile = mobile
                smsInfo_s.bill_month = bill_month
                smsInfo_s.task_id = task_id
                val obj = JsonObject(mutableEntry.toString())
                when (operator) {
                    // 移动数据提取
                    "CMCC" -> cmcc(smsInfo_s, obj)
                    // 联通数据提取
                    "CUCC" -> cucc(smsInfo_s, obj)
                    // 电信数据提取
                    "CTCC" -> ctcc(smsInfo_s, obj)
                }
                smsInfoList.add(smsInfo_s)
            }

        }
        println("smsInfoList:${smsInfoList.size}" + smsInfoList.toString())
        selectBeforeInsert(smsInfoList.get(0)).subscribe({
            // 如果查询结果的行数大于0 说明已经入库过了  暂时先不处理
            if (it.numRows == 0) {
                // 执行批量方法
                insertBybatch(SmsInfo(), smsInfoList).subscribe({
                    println(it)
                }, {
                    it.printStackTrace()
                })
            } else {
                println("已经存在${it.numRows}条数据,该数据已经入库过！新数据有${smsInfoList.size}条")
            }
        }, {
            it.printStackTrace()
        })
    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(smsInfo: SmsInfo, valueList: List<SmsInfo>): Single<UpdateResult> {

        val sql = SQL.init {
            BATCH_INSERT_INTO(smsInfo.tableName())
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
        smsInfo.send_type = obj.getString("commMode")
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = obj.getString("meal")
        // 对方号码
        smsInfo.peer_number = obj.getString("anotherNm")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = (obj.getString("commFee").toDouble() * (100)).toInt()

    }

    /**
     * 联通-短信数据提取
     */
    private fun cucc(smsInfo: SmsInfo, obj: JsonObject) {
        smsInfo.time = obj.getString("smsdate") + " " + obj.getString("smstime")
        // 通信地点 无数据
        smsInfo.location = ""
        // 通信方式 （SMS-短信; MSS-彩信）
        smsInfo.msg_type = obj.getString("businesstype") //(01-国内短信/02-国际短信/03-国内彩信)
        // 接收类型 (SEND-发送; RECEIVE-收取)
        when (obj.getString("smstype")) {
            // 1接收
            "1" -> {
                smsInfo.send_type = "RECEIVE"
            }
            else -> {
                smsInfo.send_type = "SEND"
            }
        }
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = ""
        // 对方号码
        smsInfo.peer_number = obj.getString("othernum")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = (obj.getString("Fee").toDouble() * (100)).toInt()

    }

    /**
     * 电信-短信数据提取
     */
    private fun ctcc(smsInfo: SmsInfo, obj: JsonObject) {


    }


}