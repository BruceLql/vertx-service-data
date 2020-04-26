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
import kavi.tech.service.mysql.entity.InternetInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
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


    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(valueList: List<InternetInfo>): Single<UpdateResult> {

        if (valueList.isNullOrEmpty()) {
            return Single.just(UpdateResult())
        }
        val sql = SQL.init {
            BATCH_INSERT_INTO(InternetInfo.tableName)
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


        return this.client.rxGetConnection().flatMap { conn ->
            val startTime = System.currentTimeMillis()
            conn.rxUpdate(sql).doAfterTerminate {
                conn.close()
                log.info("上网流量批量插入 执行时间：${System.currentTimeMillis() - startTime}ms")
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
     * 获取近六个月 通话记录原始数据
     */
    fun queryNetsRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        log.info("=========:$dateList")
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    val json = JsonObject()
                    sqlExecuteQuery2(conn, mobile, taskId, dateList[d]).map {

                        json.put(dateList[d], if (it.numRows == 0) JsonObject() else it.rows)

                    }.toObservable()

                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * 获取近六个月 通话记录原始数据统计信息
     */
    fun queryNetsCountRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("=========:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    val json = JsonObject()
                    sqlExecuteQuery1(conn, mobile, taskId, dateList[d]).map {

                        println("result1" + it.toJson())
//                        json.put("data", if (it.numRows == 0) JsonObject().put("bill_month",dateList[d]).put("total_size",0).put("items",ArrayList<JsonObject>()) else it.rows[0])
                        json.put("data", JsonObject().put("bill_month", dateList[d].let { it -> it.substring(0, 4) + "-" + it.substring(4, 6) }).put("total_size", 0).put("items", ArrayList<JsonObject>()) )

                    }.toObservable()
                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * sql查询 上网流量按月汇总数据
     */
    fun sqlExecuteQuery1(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql =
            "SELECT CONCAT_WS('-',LEFT(bill_month,4),RIGHT(bill_month,2)) as bill_month , count(*) AS total_size FROM ${InternetInfo.tableName}\n" +
                    " WHERE mobile = \"$mobile\" \n" +
                    "AND task_id = \"$taskId\" \n" +
                    "AND bill_month = \"$moth\"\n" +
                    "GROUP BY bill_month\n" +
                    "ORDER BY bill_month DESC"
        return this.query(conn, sql)
    }


    /**
     * sql查询 上网记录数据
     */
    fun sqlExecuteQuery2(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT start_time as time,\n" +
                "comm_time as  duration,\n" +
                "sum_flow as subflow,\n" +
                "comm_plac as location,\n" +
                "net_type as net_type,\n" +
                "\"\" as service_name,\n" +
                "cast(id as char ) as details_id,\n" +
                "comm_fee as fee " +
                "FROM ${InternetInfo.tableName}" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n" +
                "ORDER BY time DESC"
        return this.query(conn, sql)
    }
}
