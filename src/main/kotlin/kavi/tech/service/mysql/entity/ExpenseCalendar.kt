package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 上网详情记录表
 *
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class ExpenseCalendar(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var bill_month: String? = null,             // 账单月份
    var bill_start_date: String? = null,        // 账单开始时间
    var bill_end_date: String? = null,          // 账单结束时间
    var bill_fee: Int? = 0,                     // 账单金额
    var carrier_001: String? = null,            // 预留1
    var carrier_002: String? = null             // 预留2

) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_month_bill"

    }
}
