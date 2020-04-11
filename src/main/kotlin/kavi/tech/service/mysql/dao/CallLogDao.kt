package kavi.tech.service.mysql.dao

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.strategry.HashStrategy
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.CallLog
import kavi.tech.service.mysql.entity.UserInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single
import rx.Subscription

@Repository
class CallLogDao @Autowired constructor(
    private val client: AsyncSQLClient,
    private val strategy: HashStrategy
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

            val mobile = it.getString("mobile")
            val bill_month = it.getString("bill_month")
            val task_id = it.getString("mid")
            val dataOut = it.getJsonObject("data")
            if (dataOut.isEmpty) { return }
            if (dataOut.getInteger("totalNum") < 1) { return }
            dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                val callLog_s = CallLog()
                callLog_s.mobile = mobile;
                callLog_s.bill_month = bill_month;
                callLog_s.task_id = task_id;
                val obj = JsonObject(mutableEntry.toString())
                callLog_s.time = obj.getString("startTime")
                callLog_s.location = obj.getString("commPlac")
                // （DIAL-主叫; DIALED-被叫）
                callLog_s.dial_type = obj.getString("commMode")
                // 对方号码
                callLog_s.peer_number = obj.getString("anotherNm")
                // 通信时长
                callLog_s.duration_in_second = obj.getString("commTime")
                //通信类型 （通话地类型 e.g.省内漫游、 国内被叫）
                callLog_s.location_type = obj.getString("commType")
                // 费用 原始数据单位是元  转换成分后存储
                callLog_s.fee = (obj.getString("commFee").toDouble() * (100)).toInt()

                callLogList.add(callLog_s)
            }

        }
        println("callLogList:${callLogList.size}" + callLogList.toString())
        selectBeforeInsert(callLogList.get(0)).subscribe({
            // 如果查询结果的行数大于0 说明已经入库过了  暂时先不处理
            if( it.numRows == 0){
                // 执行批量方法
                insertBybatch(CallLog(), callLogList).subscribe({
                    println(it)
                }, {
                    it.printStackTrace()
                })
            }else{
                println("已经存在${it.numRows}条数据,该数据已经入库过！新数据有${callLogList.size}条")
            }
        },{
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
            println("valueList:" + valueList)
            valueList.map {
                val ss = it.preInsert()

                BATCH_INTO_VALUES(
                    ss.get("id"),
                    '"' + ss.get("task_id").toString() + '"',
                    '"' + ss.get("mobile").toString() + '"',
                    '"' + ss.get("bill_month").toString() + '"',
                    '"' + ss.get("time").toString() + '"',
                    '"' + ss.get("peer_number").toString() + '"',
                    '"' + ss.get("location").toString() + '"',
                    '"' + ss.get("location_type").toString() + '"',
                    '"' + ss.get("duration_in_second").toString() + '"',
                    '"' + ss.get("dial_type").toString() + '"',
                    '"' + ss.get("fee").toString() + '"',
                    '"' + ss.get("homearea").toString() + '"',
                    '"' + ss.get("created_at").toString() + '"',
                    '"' + ss.get("deleted_at").toString() + '"'
                )
            }

        }

        println("sql=" + sql)

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
            SELECT("id");
            FROM(CallLog.tableName);
            WHERE(Pair("task_id", callLog.task_id))
        }
        println("selectBeforeInsert sql：" + sql)
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

}
