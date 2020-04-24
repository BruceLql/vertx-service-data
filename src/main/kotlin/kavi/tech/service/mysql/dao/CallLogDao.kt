package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.splitHms
import kavi.tech.service.common.extension.value
import kavi.tech.service.common.utils.DateUtils
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.CallLog
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
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

    fun callLogDataInsert(data: List<JsonObject>): Single<UpdateResult> {

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
            if (!dataOut.isEmpty) {
                when (operator) {
                    // 移动数据提取
                    "CMCC" -> {
                        if (dataOut.getJsonArray("data").size() >= 1) {

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


        }
        println("callLogList:${callLogList.size}" + callLogList.toString())
        if (callLogList.size > 0) {
            return insertBybatch(callLogList)
        } else {
            return Single.just(UpdateResult())
        }

    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(valueList: List<CallLog>): Single<UpdateResult> {
        if(valueList.isNullOrEmpty()){
           return Single.just(UpdateResult())
        }

        val sql = SQL.init {
            BATCH_INSERT_INTO(CallLog.tableName)
//            BATCH_INTO_COLUMNS( "task_id", "mobile", "bill_month", "time", "peer_number", "location", "location_type", "duration_in_second", "dial_type", "fee", "homearea", "carrier_001", "carrier_002", "created_at", "updated_at", "deleted_at")
            BATCH_INTO_COLUMNS(
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
        return this.client.rxGetConnection().flatMap { conn ->
            val startTime = System.currentTimeMillis()
            conn.rxUpdate(sql).doAfterTerminate {
                conn.close()
                logger(sql, startTime)
            }
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
        val commMode = obj.value<String>("commMode")

        // （DIAL-主叫; DIALED-被叫）
        callLog_s.dial_type = when (commMode) {
            "主叫" -> "DIAL"
            "VOLTE主叫" -> "DIAL"
            "被叫" -> "DIALED"
            "VOLTE被叫" -> "DIALED"
            "呼转" -> "DIALED"
            else -> commMode
        }
        // 对方号码
        callLog_s.peer_number = obj.getString("anotherNm")
        // 对方归属地
        callLog_s.homearea = ""
        val commTime = obj.getString("commTime")

        // 通信时长  // 示例数据：“2小时26分钟58秒”
        callLog_s.duration_in_second = splitHms(commTime).toString()
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
            "呼叫转移" -> "DIALED"
            "呼转" -> "DIALED"
            else -> obj.getString("calltypeName")
        }
        // 对方号码
        callLog_s.peer_number = obj.getString("othernum")
        // 对方归属地
        callLog_s.homearea = obj.getString("homearea")
        // 通信时长
        val calllonghour = obj.getString("calllonghour")
        callLog_s.duration_in_second = splitHms(calllonghour).toString()
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

    /**
     * 获取近六个月 通话记录原始数据
     */
    fun queryCallLogRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("=========:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    val json = JsonObject()
                    sqlExecuteQuery2(conn, mobile, taskId, dateList[d]).map {

                        json.put("data", if (it.numRows == 0) JsonObject() else it.rows)

                    }.toObservable()

                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * 获取近六个月 通话记录原始数据统计信息
     */
    fun queryCallLogCountRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
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
     * sql查询 按月汇总数据
     */
    fun sqlExecuteQuery1(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT CONCAT_WS('-',LEFT(bill_month,4),RIGHT(bill_month,2)) as bill_month , count(*) AS total_size FROM ${CallLog.tableName}\n" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n" +
                "GROUP BY bill_month\n" +
                "ORDER BY bill_month DESC"
        println("++++++++++++++++：" + sql)
        return this.query(conn, sql)
    }


    /**
     * sql查询 具体通话数据
     */
    fun sqlExecuteQuery2(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT CONCAT_WS(\"-\",LEFT(bill_month,4),time) as time,\n" +
                "cast(id as char ) as details_id,\n" +
                "location as location,\n" +
                "location_type as location_type,\n" +
                "dial_type as dial_type,\n" +
                "peer_number as peer_number,\n" +
                "duration_in_second as  duration,\n" +
                "fee " +
                "FROM ${CallLog.tableName}" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n" +
                "ORDER BY time DESC"
        println("------------------:" + sql)
        return this.query(conn, sql)
    }

}
