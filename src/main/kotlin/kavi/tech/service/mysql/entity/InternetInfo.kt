package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 上网详情记录表
 *
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class InternetInfo(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var bill_month: String? = null,             // 账单月
    var start_time: String? = null,             // 起始时间
    var comm_plac: String? = null,              // 通信地点
    var net_play_type: String? = null,          // 上网方式
    var net_type: String? = null,               // 网络类型
    var comm_time: String? = null,              // 总时长
    var sum_flow: String? = null,               // 总流量
    var meal: String? = null,                   // 套餐优惠
    var comm_fee: Int? = 0,                     // 总费用
    var carrier_001: String? = null,            // 预留1
    var carrier_002: String? = null             // 预留2

) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_net_detial"

    }
}
