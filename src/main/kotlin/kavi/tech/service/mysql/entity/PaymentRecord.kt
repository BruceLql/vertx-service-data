package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 上网详情记录表
 *
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class PaymentRecord(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var recharge_time: String? = null,          // 充值时间
    var amount_money: Int? = 0,           // 充值金额(单位分)
    var type: String? = null,                   // 充值方式  交费方式名称. e.g. 现金
    var pay_chanel: String? = null,                   // 交费渠道
    var pay_addr: String? = null,                   // 充值地址
    var pay_flag: String? = null,                   // 支付状态
    var carrier_001: String? = null,            // 预留1
    var carrier_002: String? = null             // 预留2

) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_recharge"

    }
}
