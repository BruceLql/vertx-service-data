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
data class CallLog(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var bill_month: String? = null,             // 账单月份
    var time: String? = null,                   // 通话时间
    var peer_number: String? = null,            // 对方号码
    var location: String? = null,               // 通话地(自己的)
    var location_type: String? = null,          // 通话地类型. e.g.省内漫游
    var duration_in_second: String? = null,     // 通话地类型. e.g.省内漫游
    var dial_type: String? = null,              // DIAL-主叫; DIALED-被叫
    var fee: Int? = 0,                          // 通话费(单位分)
    var homearea: String? = null,               // 对方归属地
    var carrier_001: String? = null,            // 预留1
    var carrier_002: String? = null             // 预留2

//    var status: Int = StatusEnum.ALLOW.ordinal  // 状态
) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_voicecall"

    }
}
