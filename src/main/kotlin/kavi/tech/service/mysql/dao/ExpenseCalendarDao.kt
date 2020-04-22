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
import kavi.tech.service.mysql.entity.ExpenseCalendar
import kavi.tech.service.mysql.entity.PaymentRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

@Repository
class ExpenseCalendarDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 批量新增通话记录
     * */
    fun insertBybatch( valueList: List<ExpenseCalendar>): Single<UpdateResult> {

        if(valueList.isNullOrEmpty()){
            return Single.just(UpdateResult())
        }
        val sql = SQL.init {
            BATCH_INSERT_INTO(ExpenseCalendar.tableName)
            BATCH_INTO_COLUMNS(
                "task_id",
                "mobile",
                "bill_month",
                "bill_start_date",
                "bill_end_date",
                "bill_fee",
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
                    '"' + ss["bill_month"].toString() + '"',
                    '"' + ss["bill_start_date"].toString() + '"',
                    '"' + ss["bill_end_date"].toString() + '"',
                    ss["bill_fee"],
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
    private fun selectBeforeInsert(expenseCalendar: ExpenseCalendar): Single<ResultSet> {
        val sql = SQL.init {
            SELECT("id")
            FROM(expenseCalendar.tableName())
            WHERE(Pair("task_id", expenseCalendar.task_id))
        }
        println("selectBeforeInsert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

    /**
     * 移动-消费（月账单）数据提取
     */
    private fun cmcc(expenseCalendar: ExpenseCalendar, obj: JsonObject) {

        // 账单月
        expenseCalendar.bill_month = obj.getString("billMonth")
        // 交费方式
        expenseCalendar.bill_start_date = obj.getString("billStartDate")
        // 交费渠道
        expenseCalendar.bill_end_date = obj.getString("billEndDate")

        // 金额费用 原始数据单位是元  转换成分后存储
        expenseCalendar.bill_fee = (obj.getString("billFee").toDouble() * 100).toInt()
        // 预留字段
        expenseCalendar.carrier_001 = ""
        expenseCalendar.carrier_002 = ""

    }

    /**
     * 联通-消费（月账单）数据提取
     */
    private fun cucc(expenseCalendar: ExpenseCalendar, obj: JsonObject) {
        // 账单月
        expenseCalendar.bill_month = obj.getString("billMonth")
        // 交费方式
        expenseCalendar.bill_start_date = obj.getString("billStartDate")
        // 交费渠道
        expenseCalendar.bill_end_date = obj.getString("billEndDate")
        val billFee = obj.getInteger("billFee")
        // 金额费用 原始数据单位是元  转换成分后存储
        expenseCalendar.bill_fee = (obj.getInteger("billFee") * 100).toInt()
        // 预留字段
        expenseCalendar.carrier_001 = ""
        expenseCalendar.carrier_002 = ""


    }

    /**
     * 电信-消费（月账单）数据提取
     */
    private fun ctcc(expenseCalendar: ExpenseCalendar, obj: JsonObject) {

        // 预留字段
        expenseCalendar.carrier_001 = ""
        expenseCalendar.carrier_002 = ""
    }

    /**
     * 获取近六个月 月账单记录原始数据
     */
    fun queryBillsRaw6Month(mobile: String, taskId: String): Single<List<JsonObject>> {
        //获取最近6个月时间
        val dateList = DateUtils.getPreMothInCurrentMoth(6, DateUtils.DatePattern.YYYY_MM.value)
        println("=========:" + dateList.toString())
        return this.client.rxGetConnection().flatMap { conn ->

            val listCount =
                (0..5).map { d ->
                    var json = JsonObject()
                    sqlExecuteQuery(conn, mobile, taskId, dateList[d]).map {

                        json.put("data", if (it.numRows == 0) JsonArray().add(JsonObject().put("bill_month",dateList[d].let { it->it.substring(0,4)+"-"+it.substring(4,6) }))  else it.rows)

                    }.toObservable()

                }
            Observable.concat(listCount).toList().toSingle().doAfterTerminate(conn::close)

        }
    }


    /**
     * sql查询 月账单记录原始数据
     */
    fun sqlExecuteQuery(conn: SQLConnection, mobile: String, taskId: String, month: String): Single<ResultSet> {
        // 联通的
        val sql = "SELECT CONCAT_WS('-',LEFT(bill_month,4),RIGHT(bill_month,2)) as bill_month ,\n" +
                "bill_start_date,\n" +
                "bill_end_date,\n" +
                "bass_fee,\n" +
                "extra_fee,\n" +
                "voice_fee,\n" +
                "sms_fee,\n" +
                "extra_fee,\n" +
                "bill_fee as total_fee,\n" +
                "discount,\n" +
                "extra_discount_fee,\n" +
                "actual_fee,\n" +
                "paid_fee,\n" +
                "unpaid_fee,\n" +
                "point,\n" +
                "last_point,\n" +
                "related_mobiles,\n" +
                "notes  \n" +
                "FROM  ${ExpenseCalendar.tableName} " +
                " WHERE mobile = \"$mobile\" \n" +
                "AND task_id = \"$taskId\" \n" +
                "AND bill_month = \"$month\" \n" +
                "ORDER BY bill_start_date DESC"
        println("----------月账单记录原始数据--------:$sql")
        return this.query(conn, sql)
    }

}
