package kavi.tech.service.common.utils

import io.vertx.core.json.JsonObject
import kavi.tech.service.common.extension.regexDate
import kavi.tech.service.common.extension.splitHms
import kavi.tech.service.common.extension.splitYmd
import kavi.tech.service.common.extension.value
import kavi.tech.service.mysql.entity.*

object CMCC {
    fun buildCallLog(json: JsonObject, mobile: String, taskId: String, billMonth: String?): CallLog {

        println("json："+json)
        val callLog = CallLog()
        callLog.task_id = taskId
        callLog.mobile = mobile
        callLog.bill_month = billMonth
        callLog.time = json.value<String>("startTime").let {
            if(regexDate(it.toString())){ it.toString().substring(5,19) }else it
        }

        callLog.location = json.value<String>("commPlac")
        val commMode = json.value<String>("commMode")

        // （DIAL-主叫; DIALED-被叫）
        callLog.dial_type = when (commMode) {
            "主叫" -> "DIAL"
            "VOLTE主叫" -> "DIAL"
            "被叫" -> "DIALED"
            "VOLTE被叫" -> "DIALED"
            "呼转" -> "DIALED"
            "高清语音主叫" ->"DIAL"
            "高清语音被叫" ->"DIALED"
            else -> commMode
        }
        // 对方号码
        callLog.peer_number = json.value<String>("anotherNm")
        // 对方归属地
        callLog.homearea = ""
        val commTime = json.value<String>("commTime")?:""

        // 通信时长  // 示例数据：“2小时26分钟58秒”
        callLog.duration_in_second = splitHms(commTime).toString()
        //通信类型 （通话地类型 e.g.省内漫游、 国内被叫）
        callLog.location_type = json.value<String>("commType")
        // 费用 原始数据单位是元  转换成分后存储
        callLog.fee = ((json.value<String>("commFee")?:"0.0").toDouble() * 100).toInt()
        // 预留字段
        callLog.carrier_001 = ""
        callLog.carrier_002 = ""

        return callLog
    }

    fun buildExpenseCalendar(json: JsonObject, mobile: String, taskId: String ): ExpenseCalendar {
        val expenseCalendar = ExpenseCalendar()
        expenseCalendar.task_id = taskId
        expenseCalendar.mobile = mobile
        // 账单月
        expenseCalendar.bill_month = json.value("billMonth")

        // 交费方式
        expenseCalendar.bill_start_date = json.value<String>("billStartDate")
        // 交费渠道
        expenseCalendar.bill_end_date = json.value<String>("billEndDate")

        // 金额费用 原始数据单位是元  转换成分后存储
        expenseCalendar.bill_fee =((json.value<String>("billFee")?:"0.0").toDouble()*100).toInt()
        // 预留字段
        expenseCalendar.carrier_001 = ""
        expenseCalendar.carrier_002 = ""



        return expenseCalendar
    }

    fun buildInternetInfo(json: JsonObject, mobile: String, taskId: String, billMonth: String?): InternetInfo {

        val internetInfo = InternetInfo()
        internetInfo.task_id = taskId
        internetInfo.mobile = mobile
        internetInfo.bill_month = billMonth

        internetInfo.start_time = json.value<String>("startTime")
        internetInfo.comm_plac = json.value<String>("commPlac")
        // 上网方式
        internetInfo.net_play_type = json.value<String>("netPlayType")
        // 网络类型
        internetInfo.net_type = json.value<String>("netType")
        // 总时长
        internetInfo.comm_time = json.value<String>("commTime")
        // 总流量
        internetInfo.sum_flow = json.value<String>("sumFlow")?.replace("KB","")

        // 套餐优惠
        internetInfo.meal = json.value<String>("meal")

        val _commFee = json.value<String>("commFee")
        val commFee = when (_commFee) {
            null -> "0.00"
            "" -> "0.00"
            else -> _commFee
        }

        // 费用 原始数据单位是元  转换成分后存储
        internetInfo.comm_fee = (commFee.toDouble() * 100).toInt()
        // 预留字段
        internetInfo.carrier_001 = ""
        internetInfo.carrier_002 = ""


        return internetInfo
    }

    fun buildPaymentRecord(json: JsonObject, mobile: String, taskId: String, billMonth: String?): PaymentRecord {
        val paymentRecord = PaymentRecord()
        paymentRecord.task_id = taskId

        paymentRecord.mobile = mobile
//        paymentRecord.bill_month = billMonth


        // 交费日期
        paymentRecord.recharge_time = json.value<String>("payDate") ?: ""
        // 交费方式
        paymentRecord.type = json.value<String>("payTypeName") ?: ""
        // 交费渠道
        paymentRecord.pay_chanel = json.value<String>("payChannel")
        // 支付状态
        paymentRecord.pay_flag = json.value<String>("payFlag") ?: ""
        // 支付地址
        paymentRecord.pay_addr = json.value<String>("payAddr") ?: ""

        // 金额费用 原始数据单位是元  转换成分后存储
        val commFee = json.value<String>("payFee")?:"0.0"

        paymentRecord.amount_money = (commFee.toDouble() * 100).toInt()
        // 预留字段
        paymentRecord.carrier_001 = ""
        paymentRecord.carrier_002 = ""


        return paymentRecord
    }


    fun buileSmsInfo(json: JsonObject, mobile: String, taskId: String, billMonth: String?): SmsInfo {
        val smsInfo = SmsInfo()
        smsInfo.task_id = taskId

        smsInfo.mobile = mobile
        smsInfo.bill_month = billMonth

        smsInfo.time = json.value<String>("startTime").let {
            if(regexDate(it.toString())){ it.toString().substring(5,19) }else it
        }
        smsInfo.location = json.value<String>("commPlac")
        // 通信方式 （SMS-短信; MSS-彩信）
        smsInfo.msg_type = json.value<String>("infoType")
        // 接收类型 SEND-发送; RECEIVE-收取
        val _commMode = json.value<String>("commMode")
        smsInfo.send_type = when (_commMode) {
            "接收" -> "RECEIVE"
            "发送" -> "SEND"
            else -> _commMode
        }
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = json.value<String>("meal")
        // 对方号码
        smsInfo.peer_number = json.value<String>("anotherNm")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = ((json.value<String>("commFee")?:"0.0").toDouble() * 100).toInt()

        // 预留字段
        smsInfo.carrier_001 = ""
        smsInfo.carrier_002 = ""

        return smsInfo
    }

    fun buileUserInfo(json: JsonObject, mobile: String, taskId: String, billMonth: String?,carrier: String,userName: String,userIdcard: String): UserInfo {
        val userInfo = UserInfo()
        userInfo.task_id = taskId
        // 外部传进来的姓名 身份证号
        userInfo.user_name = userName
        userInfo.user_idcard = userIdcard

        userInfo.mobile = mobile
        userInfo.carrier = carrier
//        userInfo.bill_month = billMonth
        userInfo.name = json.value<String>("name")
        userInfo.state = json.value<String>("status")
        userInfo.real_name_info = json.value<String>("realNameInfo")
        userInfo.reliability = json.value<String>("realNameInfo")
        userInfo.brand = json.value<String>("brand")
        userInfo.package_name = ""
        userInfo.in_net_date = json.value<String>("inNetDate")
        userInfo.net_age = splitYmd(json.value<String>("netAge")?:"").toString()


        userInfo.level = json.value<String>("starLevel")
        userInfo.user_lever = json.value<String>("level")
        userInfo.star_score = json.value<String>("star_score")
        userInfo.user_email = json.value<String>("email")
        userInfo.zip_code = json.value<String>("zipCode")
        userInfo.user_address = json.value<String>("address")
        userInfo.idcard = json.value<String>("certnum") // 仅联通有
        userInfo.city = json.value<String>("city_name")  //城市
        userInfo.province = json.value<String>("area")  // 省份


        // 预留字段
        userInfo.carrier_001 = ""
        userInfo.carrier_002 = ""

        return userInfo
    }

    fun buileCombo(json: JsonObject, mobile: String, taskId: String, billMonth: String?): Combo {
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
        combo.used = json.value<String>("used")
        // 总量
        combo.total = json.value<String>("total")
        // 预留字段
        combo.carrier_001 = ""
        combo.carrier_002 = ""

        return combo
    }
}
