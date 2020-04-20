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
    var bill_fee: Int? = 0,                     // 总账单金额
    var bass_fee: Int? = 0,                     // 套餐及固定费 单位分
    var extra_service_fee: Int? = 0,            // 增值业务费 单位分
    var voice_fee: Int? = 0,                    // 语音费 单位分
    var sms_fee: Int? = 0,                      // 短彩信费 单位分
    var web_fee: Int? = 0,                      // 网络流量费 单位分
    var extra_fee: Int? = 0,                    // 其他费用 单位分
    var discount: Int? = 0,                     // 优惠费 单位分
    var extra_discount_fee: Int? = 0,           // 其他优惠费 单位分
    var actual_fee: Int? = 0,                   // 个人实际费用 单位分
    var paid_fee: Int? = 0,                     // 本期已付费用 单位分
    var unpaid_fee: Int? = 0,                   // 本期未付费用 单位分
    var point: Int? = 0,                        // 本期可用积分
    var last_point: Int? = 0,                   // 上期可用积分
    var related_mobiles: String? = null,        // 本手机关联号码, 多个手机号以逗号分隔(手机号的附属号码)
    var notes: String? = null,                  // 备注
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
