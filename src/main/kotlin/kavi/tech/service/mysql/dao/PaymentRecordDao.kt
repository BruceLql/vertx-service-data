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
import kavi.tech.service.mysql.entity.PaymentRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class PaymentRecordDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CallLog>(client) {
    override val log: Logger = logger(this::class)


    fun paymentRecordDataInsert(data: List<JsonObject>) {

        println(" 充值记录存入mysql: .....")
        val paymentRecordList = ArrayList<PaymentRecord>()
        data.forEach {
            println("paymentRecordData：" + it.toString())
            // 运营商类型  移动：CMCC 联通：CUCC 电信：CTCC
            val operator = it.getString("operator")
            val mobile = it.getString("mobile")
            val task_id = it.getString("mid")
            val dataOut = it.getJsonObject("data")
            if (dataOut.isEmpty) {
                return
            }

            when (operator) {
                // 移动数据提取
                "CMCC" -> {
                    dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                        val paymentRecord_s = PaymentRecord()
                        paymentRecord_s.mobile = mobile
                        paymentRecord_s.task_id = task_id
                        val obj = JsonObject(mutableEntry.toString())
                        cmcc(paymentRecord_s, obj)
                        paymentRecordList.add(paymentRecord_s)
                    }

                }
                // 联通数据提取
                "CUCC" -> {
                    dataOut.getJsonArray("totalResult").forEachIndexed { index, mutableEntry ->
                        val paymentRecord_s = PaymentRecord()
                        paymentRecord_s.mobile = mobile
                        paymentRecord_s.task_id = task_id
                        val obj = JsonObject(mutableEntry.toString())
                        cucc(paymentRecord_s, obj)
                        paymentRecordList.add(paymentRecord_s)
                    }
                }
                // 电信数据提取
                "CTCC" ->{

                }
            }

        }
        println("smsInfoList:${paymentRecordList.size}" + paymentRecordList.toString())
        selectBeforeInsert(paymentRecordList.get(0)).subscribe({
            // 如果查询结果的行数大于0 说明已经入库过了  暂时先不处理
            if (it.numRows == 0) {
                // 执行批量方法
                insertBybatch(PaymentRecord(), paymentRecordList).subscribe({
                    println(it)
                }, {
                    it.printStackTrace()
                })
            } else {
                println("已经存在${it.numRows}条数据,该数据已经入库过！新数据有${paymentRecordList.size}条")
            }
        }, {
            it.printStackTrace()
        })
    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(paymentRecord: PaymentRecord, valueList: List<PaymentRecord>): Single<UpdateResult> {

        val sql = SQL.init {
            BATCH_INSERT_INTO(paymentRecord.tableName())
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
     * 移动-短信数据提取
     */
    private fun cmcc(paymentRecord: PaymentRecord, obj: JsonObject) {

        // 交费日期
        paymentRecord.recharge_time = obj.getString("payDate")
        // 交费方式
        paymentRecord.type = obj.getString("payTypeName")
        // 交费渠道
        paymentRecord.pay_chanel = obj.getString("payChannel")
        // 支付状态
        paymentRecord.pay_flag = obj.getString("payFlag")
        // 支付地址
        paymentRecord.pay_addr = obj.getString("payAddr")

        // 金额费用 原始数据单位是元  转换成分后存储
        paymentRecord.amount_money = (obj.getString("payFee").toDouble() * 100).toInt()
        // 预留字段
        paymentRecord.carrier_001 = ""
        paymentRecord.carrier_002 = ""

    }

    /**
     * 联通-短信数据提取
     */
    private fun cucc(paymentRecord: PaymentRecord, obj: JsonObject) {
        // 交费日期
        paymentRecord.recharge_time = obj.getString("paydate")
        // 交费方式
        paymentRecord.type = obj.getString("payment")
        // 交费渠道
        paymentRecord.pay_chanel = obj.getString("paychannel")
        // 支付状态 暂无
        paymentRecord.pay_flag = ""
        // 支付地址 暂无
        paymentRecord.pay_addr = ""

        // 金额费用 原始数据单位是元  转换成分后存储
        paymentRecord.amount_money = (obj.getString("payfee").toDouble() * 100).toInt()
        // 预留字段
        paymentRecord.carrier_001 = ""
        paymentRecord.carrier_002 = ""


    }

    /**
     * 电信-短信数据提取
     */
    private fun ctcc(paymentRecord: PaymentRecord, obj: JsonObject) {

        // 预留字段
        paymentRecord.carrier_001 = ""
        paymentRecord.carrier_002 = ""
    }


}
