package kavi.tech.service.common.utils

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kavi.tech.service.common.extension.regexDate
import kavi.tech.service.common.extension.value
import kavi.tech.service.mysql.entity.*
import java.util.*

object CTCC {
    fun buildCallLog(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : CallLog {
        println("==============ctcc json :$json")
        val callLog = CallLog()
        callLog.task_id = taskId
        callLog.mobile = mobile
        callLog.bill_month = billMonth
        callLog.time = json.value<String>("time").let {
            val timeStr = it?.replace("/","-").toString()
            if(regexDate(timeStr)){ timeStr.substring(5,19) }else timeStr
        }

        callLog.location = json.value<String>("location")
        val dial_type = json.value<String>("dial_type")
        // （DIAL-主叫; DIALED-被叫）
        callLog.dial_type = when (dial_type) {
            "主叫" -> "DIAL"
            "VOLTE主叫" -> "DIAL"
            "被叫" -> "DIALED"
            "VOLTE被叫" -> "DIALED"
            "呼转" -> "DIALED"
            "高清语音主叫" -> "DIAL"
            "高清语音被叫" -> "DIALED"
            else -> dial_type
        }
        callLog.peer_number = json.value<String>("peer_number")
        callLog.duration_in_second = json.value<String>("duration_in_second")
        callLog.location_type = json.value<String>("location_type")

        val _fee = json.value<String>("fee")
        val commFee = when (_fee) {
            null -> "0.00"
            "" -> "0.00"
            else -> _fee
        }
        // 费用 原始数据单位是元  转换成分后存储
        callLog.fee = (commFee.toDouble() * 100).toInt()
        callLog.homearea = ""
        // 预留字段
        callLog.carrier_001 = ""
        callLog.carrier_002 = ""
        return callLog
    }

    fun buildExpenseCalendar(json: JsonObject, mobile:String, taskId: String) : ExpenseCalendar {
        println("=====================消费记录 json：$json")
        val data = json.value<JsonObject>("data")
        val bill_month = json.value<String>("bill_month")
        val expenseCalendar = ExpenseCalendar()
        expenseCalendar.task_id = taskId
        expenseCalendar.mobile = mobile

        // 账单月
        expenseCalendar.bill_month = bill_month?:""

        // 交费方式
        expenseCalendar.bill_start_date = data.value<String>("bill_start_date")+"01"?:bill_month+"01"
        // 交费渠道
        expenseCalendar.bill_end_date = data.value<String>("billEndDate")?:bill_month+"30"  //todo 需要优化

        // 总费用 金额费用 原始数据单位是元  转换成分后存储
        expenseCalendar.bill_fee = ((data.value<Double>("bill_fee")?:0.00) * 100).toInt()

        // 预留字段
        expenseCalendar.carrier_001 = ""
        expenseCalendar.carrier_002 = ""

        return expenseCalendar
    }

    fun buildInternetInfo(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : InternetInfo {
        val internetInfo = InternetInfo()
        internetInfo.task_id = taskId
        internetInfo.mobile = mobile
        internetInfo.bill_month = billMonth

        return internetInfo
    }

    fun buildPaymentRecord(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : PaymentRecord {
        val paymentRecord = PaymentRecord()
        paymentRecord.task_id = taskId

        paymentRecord.mobile = mobile
//        paymentRecord.bill_month = billMonth

        return paymentRecord
    }


    fun buileSmsInfo(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : SmsInfo {
        val smsInfo = SmsInfo()
        smsInfo.task_id = taskId

        smsInfo.mobile = mobile
        smsInfo.bill_month = billMonth

        return smsInfo
    }

    fun buileUserInfo(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : UserInfo {
        val userInfo = UserInfo()
        userInfo.task_id = taskId

        userInfo.mobile = mobile
//        userInfo.bill_month = billMonth

        return userInfo
    }


}
