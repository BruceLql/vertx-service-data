package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 通话记录表
 *
 * 序列化时忽略密码和盐值
 * value = ["password", "password_salt"]
 * allowGetters = false (不可读)
 * allowSetters = true (可写入)
 * */
@JsonIgnoreProperties(ignoreUnknown = true, value = [], allowGetters = false, allowSetters = true)
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class SmsInfo(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var bill_month: String? = null,             // 账单月
    var time: String? = null,                   // 收/发短信时间
    var peer_number: String? = null,            // 对方号码
    var location: String? = null,               // 通信地(自己的)
    var package_name: String? = null,          // package_name
    var send_type: String? = null,     // SEND-发送; RECEIVE-收取
    var msg_type: String? = null,              // SMS-短信; MSS-彩信
    var service_name: String? = null,                          // 业务名称. e.g. 点对点(网内)
    var star_score: String? = null,                          // 星级得分（仅移动有）
    var user_email: String? = null,               // 电子邮箱
    var zip_code: String? = null,               // 邮政编码
    var user_address: String? = null,               //联系地址
    var idcard: String? = null,               // 身份证号码
    var carrier: String? = null,               // 运营商类型：移动、联通、电信
    var province: String? = null,               // province 省份
    var city: String? = null,               // city 城市
    var state: String? = null,               // state 账户状态
    var reliability: String? = null,               // reliability 实名认证
    var carrier_001: String? = null,            // 预留1
    var carrier_002: String? = null          // 预留2


//    var status: Int = StatusEnum.ALLOW.ordinal  // 状态
) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_baseinfo"

    }
}
