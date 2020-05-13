package kavi.tech.service.common.utils

import io.vertx.core.json.JsonObject
import kavi.tech.service.common.extension.value
import kavi.tech.service.mysql.entity.*

object CTCC {
    fun buildCallLog(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : CallLog {
        val callLog = CallLog()
        callLog.task_id = taskId
        callLog.mobile = mobile
        callLog.bill_month = billMonth
        val startTime = json.value<String>("startTime")

        return callLog
    }

    fun buildExpenseCalendar(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : ExpenseCalendar {
        val expenseCalendar = ExpenseCalendar()
        expenseCalendar.task_id = taskId
        expenseCalendar.mobile = mobile

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
