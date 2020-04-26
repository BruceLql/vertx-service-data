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
import kavi.tech.service.mysql.entity.PaymentRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

@Repository
class PaymentRecordDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(valueList: List<PaymentRecord>): Single<UpdateResult> {
        if (valueList.isNullOrEmpty()) {
            return Single.just(UpdateResult())
        }
        val sql = SQL.init {
            BATCH_INSERT_INTO(PaymentRecord.tableName)
            BATCH_INTO_COLUMNS(
                "task_id",
                "mobile",
                "recharge_time",
                "amount_money",
                "type",
                "pay_chanel",
                "pay_addr",
                "pay_flag",
                "carrier_001",
                "carrier_002",
                "created_at",
                "deleted_at"
            )
            println("valueList:$valueList")
            valueList.map {
                val ss = it.preInsert()

                return@map BATCH_INTO_VALUES(

                    '"' + ss["task_id"].toString() + '"',
                    '"' + ss["mobile"].toString() + '"',
                    '"' + ss["recharge_time"].toString() + '"',
                    ss["amount_money"],
                    '"' + ss["type"].toString() + '"',
                    '"' + ss["pay_chanel"].toString() + '"',
                    '"' + ss["pay_addr"].toString() + '"',
                    '"' + ss["pay_flag"].toString() + '"',
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
                println("执行时间：${System.currentTimeMillis() - startTime}ms")
            }
        }

    }

    /**
     * 通过task_id查询数据是否已经入库
     * */
    private fun selectBeforeInsert(paymentRecord: PaymentRecord): Single<ResultSet> {
        val sql = SQL.init {
            SELECT("id")
            FROM(paymentRecord.tableName())
            WHERE(Pair("task_id", paymentRecord.task_id))
        }
        println("selectBeforeInsert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }



    /**
     * 获取近六个月 充值记录原始数据
     */
    fun queryRechargesRaw6Month(mobile: String, taskId: String): Single<JsonObject> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)

        return this.client.rxGetConnection().flatMap { conn ->

            var jsonList = ArrayList<JsonObject>()
            var json = JsonObject()
            val reChargesObservable = sqlExecuteQuery(conn, mobile, taskId).map {
                println("充值记录:" + it.toJson())
                json.put("data", if (it.numRows == 0) JsonObject() else it.rows)

            }.toObservable()

            Observable.concat(listOf(reChargesObservable)).toSingle().doAfterTerminate(conn::close)

        }

    }


    /**
     * sql查询 充值记录数据
     */
    fun sqlExecuteQuery(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        // 移动的
        /*val sql = "SELECT amount_money as amount,\n" +
                "CONCAT_WS(\" \",\n" +
                "CONCAT_WS(\"-\",SUBSTRING(recharge_time FROM 1 FOR 4),SUBSTRING(recharge_time FROM 5 FOR 2),SUBSTRING(recharge_time FROM 7 FOR 2)),\n" +
                "CONCAT_WS(\":\",SUBSTRING(recharge_time FROM 9 FOR 2),SUBSTRING(recharge_time FROM 11 FOR 2),SUBSTRING(recharge_time FROM 13 FOR 2))\n" +
                ") as recharge_time,\n" +
                "type  "+
                "FROM ${PaymentRecord.tableName}" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "ORDER BY recharge_time DESC"*/
        // 联通的
        val sql = "SELECT amount_money as amount,\n" +
                "recharge_time,\n" +
                "type  " +
                "FROM ${PaymentRecord.tableName}" +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "ORDER BY recharge_time DESC"
        log.info("----------充值记录数据--------:$sql")
        return this.query(conn, sql)
    }

}
