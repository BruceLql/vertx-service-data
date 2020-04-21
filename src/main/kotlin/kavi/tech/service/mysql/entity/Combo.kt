package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 套餐表（套餐使用情况）
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class Combo(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var bill_month: String? = null,             // 账单月
    var bill_start_date: String? = null,         // 套餐起始日, 格式为yyyy-MM-dd
    var bill_end_date: String? = null,           // 套餐结束日, 格式为yyyy-MM-dd
    var name: String? = null,                    // 套餐项目名称  流量、语音、短信
    var unit: String? = null,                    // 单位 如：KB
    var used: String? = null,                    // 使用量
    var total: String? = null,                   // 总量
    var carrier_001: String? = null,             // 预留1
    var carrier_002: String? = null              // 预留2

) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_combo"

    }
}
