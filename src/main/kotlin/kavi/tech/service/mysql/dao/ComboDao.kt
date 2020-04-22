package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonArray
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
import kavi.tech.service.mysql.entity.Combo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

@Repository
class ComboDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 批量新增套餐信息
     * */
    fun insertBybatch(valueList: List<Combo>): Single<UpdateResult> {

        if (valueList.isNullOrEmpty()) {
            println("======valueList.isNullOrEmpty===================")
            return  Single.just(UpdateResult())
        }

        val sql = SQL.init {
            BATCH_INSERT_INTO(Combo.tableName)
            BATCH_INTO_COLUMNS(
                "task_id",
                "mobile",
                "bill_month",
                "bill_start_date",
                "bill_end_date",
                "name",
                "unit",
                "used",
                "total",
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
                    '"' + ss["bill_start_date"].toString() + '"',
                    '"' + ss["bill_end_date"].toString() + '"',
                    '"' + ss["name"].toString() + '"',
                    '"' + ss["unit"].toString() + '"',
                    '"' + ss["used"].toString() + '"',
                    '"' + ss["total"].toString() + '"',
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
     * 获取近六个月 套餐信息原始数据
     */
    fun queryComboRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("====queryComboRaw6Month=====:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    var json = JsonObject()
                    sqlExecuteQuery2(conn, mobile, taskId, dateList[d]).map {
                        println("=========it.rows==========="+it.rows)
                        json.put("items", if (it.numRows == 0) JsonArray() else it.rows)
                    }.toObservable()

                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * 获取近六个月 短信记录原始数据统计信息
     */
    fun queryComboCountRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("=====queryComboCountRaw6Month====:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    var json = JsonObject()
                    sqlExecuteQuery(conn, mobile, taskId, dateList[d]).map {

                        println("result1" + it.toJson())
                        json.put("data", if (it.numRows == 0) JsonObject().put("bill_start_date",dateList[d].let { it-> it.substring(0,4)+"-"+it.substring(4,6)+"01" }).put("bill_end_date",dateList[d].let { it-> it.substring(0,4)+"-"+it.substring(4,6)+"30" }).put("items",ArrayList<JsonObject>()) else it.rows[0].put("bill_start_date",dateList[d]+"-01").put("bill_end_date",dateList[d]+"-30"))

                    }.toObservable()
                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }


    }

    /**
     * sql查询 按月汇总套餐数据
     */
    fun sqlExecuteQuery(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT " +
                "bill_month, count(*) AS total_size FROM ${Combo.tableName}\n" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n" +
                "GROUP BY bill_month\n" +
                "ORDER BY bill_month DESC"
        println("+++++++++combo+++++++：$sql")
        return this.query(conn, sql)
    }


    /**
     * sql查询 具体套餐数据
     */
    fun sqlExecuteQuery2(conn: SQLConnection, mobile: String, taskId: String, moth: String): Single<ResultSet> {
        val sql = "SELECT\n" +
                "\t CASE\t\n" +
                "\tWHEN\n" +
                "\t\t( bill_start_date = NULL || bill_start_date = \"\" ) THEN\n" +
                "\t\"$moth-01\" ELSE bill_start_date \n" +
                "\t\t\tEND AS bill_start_date,\n" +
                "\t bill_end_date,\n" +
                "\t name,\n" +
                "\t unit,\n" +
                "\tused,\n" +
                "\t total  "+

                " FROM ${Combo.tableName}  com " +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$moth\"\n"
        println("----------combo--------:$sql")
        return this.query(conn, sql)
    }


}
