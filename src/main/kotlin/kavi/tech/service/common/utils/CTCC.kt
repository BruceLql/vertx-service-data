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
        callLog.duration_in_second = json.value<Int>("duration_in_second").toString()  // 单位秒
        callLog.location_type = json.value<String>("location_type")

        // 费用 原始数据单位是元  转换成分后存储
        callLog.fee = json.value<Int>("fee") //单位 分
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
        expenseCalendar.bill_start_date = data.value<String>("bill_start_date")?:bill_month+"01"
        // 交费渠道
        expenseCalendar.bill_end_date = data.value<String>("billEndDate")?:bill_month+"30"  //todo 需要优化

        // 总费用 金额费用 原始数据单位是元  转换成分后存储
        expenseCalendar.bill_fee = data.value<Int>("bill_fee")

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

        internetInfo.start_time = json.value<String>("start_time")
        internetInfo.comm_plac = json.value<String>("comm_plac")?.replace("-","")
        internetInfo.net_type = json.value<String>("net_type")
        internetInfo.comm_time = json.value<Int>("comm_time").toString()
        internetInfo.sum_flow = json.value<Int>("sum_flow").toString()
        internetInfo.comm_fee = json.value<Int>("comm_fee")
        internetInfo.net_play_type = ""
        internetInfo.meal = ""
        internetInfo.carrier_001 = ""
        internetInfo.carrier_002 = ""

        return internetInfo
    }

    fun buildPaymentRecord(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : PaymentRecord {
        val paymentRecord = PaymentRecord()
        paymentRecord.task_id = taskId
        paymentRecord.mobile = mobile
        println("========= ctcc buildPaymentRecord: $json")

        // 交费日期
        paymentRecord.recharge_time = json.value<String>("recharge_time")
        // 交费方式
        paymentRecord.type = json.value<String>("type")
        // 交费渠道
        paymentRecord.pay_chanel = json.value<String>("pay_chanel")
        // 支付状态 暂无
        paymentRecord.pay_flag = ""
        // 支付地址 暂无
        paymentRecord.pay_addr = ""

        // 金额费用 原始数据单位是元  转换成分后存储
        paymentRecord.amount_money = json.value<Int>("amount_money")
        // 预留字段
        paymentRecord.carrier_001 = ""
        paymentRecord.carrier_002 = ""

        return paymentRecord
    }


    fun buildSmsInfo(json: JsonObject, mobile:String, taskId: String, billMonth:String?) : SmsInfo {
        val smsInfo = SmsInfo()
        smsInfo.task_id = taskId

        smsInfo.mobile = mobile
        smsInfo.bill_month = billMonth
        smsInfo.time = json.value<String>("time")
        // 通信地点 无数据
        smsInfo.location = ""
        // 通信方式 （SMS-短信; MSS-彩信） //(01-国内短信/02-国际短信/03-国内彩信)
        val _businesstype = json.value<String>("msg_type")?:"SMS"
        smsInfo.msg_type = when {
            _businesstype.contains("短信") -> "SMS"
            _businesstype.contains("彩") -> "MSS"
            else -> _businesstype
        }
        // 接收类型 (SEND-发送; RECEIVE-收取)
        val _smstype = json.value<String>("send_type")?:"SEND"
        smsInfo.send_type = when{
            // 1接收
            _smstype.contains("收")  -> "RECEIVE"
            _smstype.contains("发") -> "SEND"
            else -> _smstype
        }
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = ""
        // 对方号码
        smsInfo.peer_number = json.value<String>("peer_number")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = json.value<Int>("fee")

        // 预留字段
        smsInfo.carrier_001 = ""
        smsInfo.carrier_002 = ""
        println("=======smsInfo ==========: $smsInfo")
        return smsInfo
    }

    fun buildUserInfo(json: JsonObject, mobile: String, taskId: String, billMonth: String?, carrier: String, userName: String, userIdcard: String) : UserInfo {
        println("=============CTCC buileUserInfo :"+json)
        val userInfo = UserInfo()
        userInfo.task_id = taskId
        userInfo.mobile = mobile
        // 外部传进来的姓名 身份证号
        userInfo.user_name = userName
        userInfo.user_idcard = userIdcard
        userInfo.carrier = carrier
        userInfo.name = json.value<String>("name")
        userInfo.state = "" // todo  没有state
        userInfo.real_name_info = "" // todo real_name_info
        userInfo.reliability = "" // todo reliability
        userInfo.brand = json.value<String>("package_name")
        userInfo.package_name = json.value<String>("package_name")
        userInfo.in_net_date = json.value<String>("in_net_date")

        userInfo.level = ""
        userInfo.user_lever = ""
        userInfo.star_score = ""
        userInfo.zip_code = ""
        userInfo.idcard = ""
        userInfo.city = json.value<String>("city")  //城市
        userInfo.province = json.value<String>("province")  // 省份

        userInfo.user_address = json.value("user_address")
        userInfo.net_age = json.value<Int>("net_age").toString()  // 根据opendate 计算
        userInfo.user_email = ""


        // 预留字段
        userInfo.carrier_001 = ""
        userInfo.carrier_002 = ""

        return userInfo
    }

    fun buildCombo(json: JsonObject, mobile: String, taskId: String, billMonth: String?): Combo {

        val combo = Combo()
        combo.task_id = taskId

        combo.mobile = mobile
        combo.bill_month = billMonth
        // 套餐起始时间
        combo.bill_start_date = json.value<String>("bill_start_date")
        // 套餐结束时间
        combo.bill_end_date = json.value<String>("bill_end_date")
        // 套餐名
        combo.name = json.value<String>("name")
        // 单位
        combo.unit = json.value<String>("unit")

        // 已使用量
        combo.used = json.value<Int>("used").toString()
        // 总量
        combo.total = json.value<Int>("total").toString()
        // 预留字段
        combo.carrier_001 = ""
        combo.carrier_002 = ""

        return combo
    }

}
