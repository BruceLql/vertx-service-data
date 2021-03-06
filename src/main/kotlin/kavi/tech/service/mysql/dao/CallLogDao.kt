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


    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(valueList: List<CallLog>): Single<UpdateResult> {
        println("============>>>>>>>>>>>: ${valueList.toString()}")
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
     * 获取近六个月 通话记录原始数据
     */
    fun queryCallLogRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)

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
        return this.query(conn, sql)
    }

}
