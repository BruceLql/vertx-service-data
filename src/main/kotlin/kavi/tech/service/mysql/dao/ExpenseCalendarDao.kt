package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.CallLog
import kavi.tech.service.mysql.entity.ExpenseCalendar
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class ExpenseCalendarDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    fun expenseCalendarDataInsert(data: List<JsonObject>) {

        println(" 消费记录（月账单信息）存入mysql: .....")
        val expenseCalendarList = ArrayList<ExpenseCalendar>()
        data.forEach {
            println("expenseCalendarData：" + it.toString())
            // 运营商类型  移动：CMCC 联通：CUCC 电信：CTCC
            val operator = it.getString("operator")
            val mobile = it.getString("mobile")
            val task_id = it.getString("mid")
            val dataOut = it.getJsonArray("data")
            if (dataOut.isEmpty) {
                return
            }
            dataOut.forEachIndexed { index, mutableEntry ->
                val expenseCalendar_s = ExpenseCalendar()
                expenseCalendar_s.mobile = mobile
                expenseCalendar_s.task_id = task_id
                val obj = JsonObject(mutableEntry.toString())
                println("obj:$obj")
                when (operator) {
                    // 移动数据提取
                    "CMCC" -> cmcc(expenseCalendar_s, obj)
                    // 联通数据提取
                    "CUCC" -> cucc(expenseCalendar_s, obj)
                    // 电信数据提取
                    "CTCC" -> ctcc(expenseCalendar_s, obj)
                }
                expenseCalendarList.add(expenseCalendar_s)
            }

        }
        println("smsInfoList:${expenseCalendarList.size}" + expenseCalendarList.toString())
        selectBeforeInsert(expenseCalendarList.get(0)).subscribe({
            // 如果查询结果的行数大于0 说明已经入库过了  暂时先不处理
            if (it.numRows == 0) {
                // 执行批量方法
                insertBybatch(ExpenseCalendar(), expenseCalendarList).subscribe({
                    println(it)
                }, {
                    it.printStackTrace()
                })
            } else {
                println("已经存在${it.numRows}条数据,该数据已经入库过！新数据有${expenseCalendarList.size}条")
            }
        }, {
            it.printStackTrace()
        })
    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(expenseCalendar: ExpenseCalendar, valueList: List<ExpenseCalendar>): Single<UpdateResult> {

        val sql = SQL.init {
            BATCH_INSERT_INTO(expenseCalendar.tableName())
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


}
