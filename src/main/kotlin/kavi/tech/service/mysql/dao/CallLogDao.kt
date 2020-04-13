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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class CallLogDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 新增记录
     * */
    private fun insert(conn: SQLConnection, callLog: CallLog): Single<CallLog> {
        val sql = SQL.init {
            INSERT_INTO(callLog.tableName())

            callLog.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        return this.update(conn, sql).map {
            callLog.apply { this.id = it.keys.getLong(0) }
        }
    }

    fun callLogDataInsert(data: List<JsonObject>) {

        println(" 通话记录存入mysql: .....")
        val callLogList = ArrayList<CallLog>()
        data.forEach {
            println("通话记录 ${it.toString()}")
            // 运营商类型
            val operator = it.getString("operator")
            val mobile = it.getString("mobile")
            val bill_month = it.getString("bill_month")
            val task_id = it.getString("mid")
            val dataOut = it.getJsonObject("data")
            if (dataOut.isEmpty) {
                return
            }


            when (operator) {
                // 移动数据提取
                "CMCC" -> {
                    if (dataOut.getInteger("totalNum") < 1) {
                        return
                    }
                    dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                        val callLog_s = CallLog()
                        callLog_s.mobile = mobile
                        callLog_s.bill_month = bill_month
                        callLog_s.task_id = task_id
                        val obj = JsonObject(mutableEntry.toString())
                        cmcc(callLog_s, obj)

                        callLogList.add(callLog_s)

                    }
                }

                // 联通数据提取
                "CUCC" -> {
                    dataOut.getJsonObject("pageMap").getJsonArray("result").forEachIndexed { index, mutableEntry ->
                        val callLog_s = CallLog()
                        callLog_s.mobile = mobile
                        callLog_s.bill_month = bill_month
                        callLog_s.task_id = task_id
                        val obj = JsonObject(mutableEntry.toString())
                        cucc(callLog_s, obj)

                        callLogList.add(callLog_s)

                    }
                }
                // 电信数据提取
                "CTCC" -> {
                }
            }


        }
        println("callLogList:${callLogList.size}" + callLogList.toString())
        selectBeforeInsert(callLogList[0]).subscribe({
            // 如果查询结果的行数大于0 说明已经入库过了  暂时先不处理
            if (it.numRows == 0) {
                // 执行批量方法
                insertBybatch(CallLog(), callLogList).subscribe({ it ->
                    println(it)
                    // todo 处理通话数据分析规则
                    println("处理通话数据分析规则：=====")

                }, {
                    it.printStackTrace()
                })
            } else {
                println("已经存在${it.numRows}条数据,该数据已经入库过！新数据有${callLogList.size}条")
            }
        }, {
            it.printStackTrace()
        })
    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(callLog: CallLog, valueList: List<CallLog>): Single<UpdateResult> {

        val sql = SQL.init {
            BATCH_INSERT_INTO(callLog.tableName())
//            BATCH_INTO_COLUMNS( "task_id", "mobile", "bill_month", "time", "peer_number", "location", "location_type", "duration_in_second", "dial_type", "fee", "homearea", "carrier_001", "carrier_002", "created_at", "updated_at", "deleted_at")
            BATCH_INTO_COLUMNS(
                "id",
                "task_id",
                "mobile",
                "bill_month",
                "time",
                "peer_number",
                "location",
                "location_type",
                "duration_in_second",
                "dial_type",
                "fee",
                "homearea",
                "created_at", "deleted_at"
            )
            println("valueList:$valueList")
            valueList.map {
                val ss = it.preInsert()

                BATCH_INTO_VALUES(
                    ss["id"],
                    '"' + ss["task_id"].toString() + '"',
                    '"' + ss["mobile"].toString() + '"',
                    '"' + ss["bill_month"].toString() + '"',
                    '"' + ss["time"].toString() + '"',
                    '"' + ss["peer_number"].toString() + '"',
                    '"' + ss["location"].toString() + '"',
                    '"' + ss["location_type"].toString() + '"',
                    '"' + ss["duration_in_second"].toString() + '"',
                    '"' + ss["dial_type"].toString() + '"',
                    '"' + ss["fee"].toString() + '"',
                    '"' + ss["homearea"].toString() + '"',
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
            /* conn.rxBatch(sql).doAfterTerminate{
                 conn.close()
                 println("执行时间：${System.currentTimeMillis() - startTime}ms")
             }*/
        }

    }

    /**
     * 通过task_id查询数据是否已经入库
     * */
    private fun selectBeforeInsert(callLog: CallLog): Single<ResultSet> {
        val sql = SQL.init {
            SELECT("id")
            FROM(CallLog.tableName)
            WHERE(Pair("task_id", callLog.task_id))
        }
        println("selectBeforeInsert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

    /**
     * 移动-通话数据提取
     */
    private fun cmcc(callLog_s: CallLog, obj: JsonObject) {

        callLog_s.time = obj.getString("startTime")
        callLog_s.location = obj.getString("commPlac")
        // （DIAL-主叫; DIALED-被叫）
        callLog_s.dial_type = when (obj.getString("commMode")) {
            "主叫" -> "DIAL"
            "VOLTE主叫" -> "DIAL"
            "被叫" -> "DIALED"
            "VOLTE被叫" -> "DIALED"
            else -> obj.getString("commMode")
        }
        // 对方号码
        callLog_s.peer_number = obj.getString("anotherNm")
        // 通信时长
        callLog_s.duration_in_second = obj.getString("commTime")
        //通信类型 （通话地类型 e.g.省内漫游、 国内被叫）
        callLog_s.location_type = obj.getString("commType")
        // 费用 原始数据单位是元  转换成分后存储
        callLog_s.fee = (obj.getString("commFee").toDouble() * (100)).toInt()
        // 预留字段
        callLog_s.carrier_001 = ""
        callLog_s.carrier_002 = ""

    }

    /**
     * 联通-通话数据提取
     */
    private fun cucc(callLog_s: CallLog, obj: JsonObject) {

        callLog_s.time = obj.getString("calldate") + " " + obj.getString("calltime")
        callLog_s.location = obj.getString("calledhome")
        // （DIAL-主叫; DIALED-被叫）
        callLog_s.dial_type = when (obj.getString("calltypeName")) {
            "主叫" -> "DIAL"
            "被叫" -> "DIALED"
            else -> obj.getString("calltypeName")
        }
        // 对方号码
        callLog_s.peer_number = obj.getString("othernum")
        // 通信时长
        callLog_s.duration_in_second = obj.getString("calllonghour")
        //通信类型 （通话地类型 e.g.省内漫游、 国内被叫）
        callLog_s.location_type = obj.getString("landtype")
        // 费用 原始数据单位是元  转换成分后存储
        callLog_s.fee = (obj.getString("totalfee").toDouble() * (100)).toInt()

        // 预留字段
        callLog_s.carrier_001 = ""
        callLog_s.carrier_002 = ""


    }

    /**
     * 电信-短信数据提取
     */
    private fun ctcc(callLog_s: CallLog, obj: JsonObject) {

        // 预留字段
        callLog_s.carrier_001 = ""
        callLog_s.carrier_002 = ""
    }

}
