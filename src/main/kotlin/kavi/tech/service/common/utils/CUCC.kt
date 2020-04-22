package kavi.tech.service.common.utils

import com.alibaba.druid.sql.visitor.functions.Substring
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kavi.tech.service.common.extension.splitHms
import kavi.tech.service.common.extension.value
import kavi.tech.service.mysql.entity.*

object CUCC {
    @JvmStatic
    fun buildCallLog(json: JsonObject, mobile: String, taskId: String, billMonth: String?): CallLog {
        val callLog = CallLog()
        callLog.task_id = taskId
        callLog.mobile = mobile
        callLog.bill_month = billMonth
        // 时间截取成 和移动一样
        callLog.time = json.value<String>("calldate")?.substring(5) + " " + json.value<String>("calltime")
        callLog.location = json.value<String>("calledhome")
        // （DIAL-主叫; DIALED-被叫）
        callLog.dial_type = when (json.value<String>("calltypeName")) {
            "主叫" -> "DIAL"
            "被叫" -> "DIALED"
            "呼叫转移" -> "DIALED"
            "呼转" -> "DIALED"
            else -> json.value<String>("calltypeName")
        }
        // 对方号码
        callLog.peer_number = json.value<String>("othernum")
        // 对方归属地
        callLog.homearea = json.value<String>("homearea")
        // 通信时长
        val calllonghour = json.value<String>("calllonghour")
        callLog.duration_in_second = splitHms(calllonghour ?: "").toString()
        //通信类型 （通话地类型 e.g.省内漫游、 国内被叫）
        callLog.location_type = json.value<String>("landtype")
        // 费用 原始数据单位是元  转换成分后存储
        callLog.fee = ((json.value<String>("totalfee") ?: "0.0").toDouble() * (100)).toInt()

        // 预留字段
        callLog.carrier_001 = ""
        callLog.carrier_002 = ""

        return callLog
    }

    fun buildExpenseCalendar(json: JsonObject, mobile: String, taskId: String): ExpenseCalendar {
        val expenseCalendar = ExpenseCalendar()
        expenseCalendar.task_id = taskId
        expenseCalendar.mobile = mobile
        // 账单月
        expenseCalendar.bill_month = json.value<String>("billMonth")

        // 交费方式
        expenseCalendar.bill_start_date = json.value<String>("billStartDate")
        // 交费渠道
        expenseCalendar.bill_end_date = json.value<String>("billEndDate")

        // 总费用 金额费用 原始数据单位是元  转换成分后存储
        expenseCalendar.bill_fee = ((json.value<Double>("billFee")?:0.00) * 100).toInt()

        val billCUCCList = json.value<JsonArray>("billCUCCList")
        if (billCUCCList != null) {
            val list = billCUCCList.mapNotNull {
                it as JsonObject
            }
            list.mapNotNull { it ->
                val name = it.value<String>("name")!!
                val amount = ((it.value<String>("amount") ?: "0.0").toDouble() * 100).toInt()
                when(name){

                    "月固定费" ->{
                        // 套餐及固定费  单位分
                        expenseCalendar.bass_fee = amount
                    }
                    "上网费" ->{
                        // 网络流量费
                        expenseCalendar.web_fee = amount
                    }
                    "增值业务费" ->{
                        // 增值业务费 单位
                        expenseCalendar.extra_service_fee = amount
                    }
                    "语音费" ->{
                        // 语音费
                        expenseCalendar.voice_fee = amount
                    }
                    "短彩信费" ->{
                        // 短彩信费 单位
                        expenseCalendar.sms_fee = amount
                    }
                    "其他费用" ->{
                        // 其他费用
                        expenseCalendar.extra_fee = amount
                    }

                }
                // 优惠费
//                expenseCalendar.discnt = it.value<String>("discnt")


            }
        }


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

        internetInfo.start_time = json.value<String>("begindateformat") + " " + json.value<String>("begintimeformat")
        // 通信地点 无数据
        internetInfo.comm_plac = json.value<String>("homearea")
        // 上网方式
        internetInfo.net_play_type = json.value<String>("roamstat") //(国际漫游/国内)
        // 网络类型
        internetInfo.net_type = json.value<String>("nettypeformat") //4g网络
        // 总时长 单位s
        internetInfo.comm_time = json.value<String>("longhour")
        // 总流量
        val pertotalsm = json.value<String>("pertotalsm") ?: "0.0"

        internetInfo.sum_flow = (pertotalsm.toDouble() * 1024).toInt().toString() // 单位为 M  转换成 KB 乘以1024
        // 套餐优惠
        internetInfo.meal = json.value<String>("deratefee")
        // 费用 原始数据单位是元  转换成分后存储
        val totalfee = json.value<String>("totalfee") ?: "0.0"
        internetInfo.comm_fee = (totalfee.toDouble() * 100).toInt()
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
        paymentRecord.recharge_time = json.value<String>("paydate") ?: ""
        // 交费方式
        paymentRecord.type = json.value<String>("payment") ?: ""
        // 交费渠道
        paymentRecord.pay_chanel = json.value<String>("paychannel") ?: ""
        // 支付状态 暂无
        paymentRecord.pay_flag = ""
        // 支付地址 暂无
        paymentRecord.pay_addr = ""

        // 金额费用 原始数据单位是元  转换成分后存储
        val commFee = json.value<String>("payfee") ?: "0.0"

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

        smsInfo.time = json.value<String>("smsdate") + " " + json.value<String>("smstime")
        // 通信地点 无数据
        smsInfo.location = ""
        // 通信方式 （SMS-短信; MSS-彩信） //(01-国内短信/02-国际短信/03-国内彩信)
        val _businesstype = json.value<String>("businesstype")
        smsInfo.msg_type = when (_businesstype) {
            "01" -> "SMS"
            "02" -> "SMS"
            "03" -> "MSS"
            else -> _businesstype
        }
        // 接收类型 (SEND-发送; RECEIVE-收取)
        val _smstype = json.value<String>("smstype")
        smsInfo.send_type = when (_smstype) {
            // 1接收
            "1" -> "RECEIVE"
            "2" -> "SEND"
            else -> _smstype
        }
        // 业务名称 （e.g. 点对点(网内)）
        smsInfo.service_name = ""
        // 对方号码
        smsInfo.peer_number = json.value<String>("othernum")
        // 费用 原始数据单位是元  转换成分后存储
        smsInfo.fee = ((json.value<String>("fee") ?: "0.0").toDouble() * 100).toInt()

        // 预留字段
        smsInfo.carrier_001 = ""
        smsInfo.carrier_002 = ""

        return smsInfo
    }

    fun buileUserInfo(json: JsonObject, mobile: String, taskId: String, billMonth: String?): UserInfo {
        val userInfo = UserInfo()
        userInfo.task_id = taskId
        userInfo.mobile = mobile
//        userInfo.bill_month = billMonth
        val userinfo: JsonObject = json.value<JsonObject>("userinfo")!!
        val myDetial: JsonObject = json.value<JsonObject>("result").value<JsonObject>("MyDetail")!!
        userInfo.name = userinfo.value<String>("custName")
        userInfo.state = userinfo.value<String>("status")
        userInfo.real_name_info = userinfo.value<String>("verifyState")
        userInfo.reliability = userinfo.value<String>("verifyState")
        userInfo.brand = userinfo.value<String>("brand_name")
        userInfo.package_name = userinfo.value<String>("packageName")
        userInfo.in_net_date = userinfo.value<String>("opendate")

        userInfo.level = userinfo.value<String>("custlvl")
        userInfo.user_lever = userinfo.value<String>("custlvl")
        userInfo.star_score = ""
        userInfo.zip_code = ""
        userInfo.idcard = userinfo.value<String>("certnum") // 仅联通有
        userInfo.city = userinfo.value<String>("citycode")  //城市
        userInfo.province = userinfo.value<String>("provincecode")  // 省份

        userInfo.user_address = myDetial.value("certaddr")
        userInfo.net_age = myDetial.value("netAge")  // 根据opendate 计算
        userInfo.user_email = myDetial.value("sendemail")


        // 预留字段
        userInfo.carrier_001 = ""
        userInfo.carrier_002 = ""

        return userInfo
    }

    fun buileCombo(json: JsonObject, mobile: String, taskId: String, billMonth: String?): Combo {

        val combo = Combo()
        combo.task_id = taskId

        combo.mobile = mobile
        combo.bill_month = json.value<String>("cycleId")?:""
        // 套餐起始时间
        var startdate = json.value<String>("startdate")?:""
        if(startdate.isNotEmpty()){
            startdate = startdate.substring(0,4)+"-"+startdate.substring(4,6)+"-"+startdate.substring(6,8)
        }
        combo.bill_start_date = startdate
        // 套餐结束时间
        var enddate = json.value<String>("enddate") ?:""
        if(enddate.isNotEmpty()){
            enddate = enddate.substring(0,4)+"-"+enddate.substring(4,6)+"-"+enddate.substring(6,8)
        }
        combo.bill_end_date = enddate
        // 套餐名
        combo.name = json.value<String>("feePolicyName")
        // 单位
        combo.unit = json.value<String>("totalUnitVal")?:""

        // 已使用量
        combo.used = json.value<String>("usedValue")?:""
        // 总量
        combo.total = json.value<String>("totalValue")?:""
        // 预留字段
        combo.carrier_001 = ""
        combo.carrier_002 = ""

        return combo
    }

}
