package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * @packageName kavi.tech.service.mysql.entity
 * @author litiezhu
 * @date 2020/4/13 11:50
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true, value = [], allowGetters = false, allowSetters = true)
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class CarrierResultData(
    var id: Long? = null,                       //id
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var item: String? = null,                 // 数据项：基本信息："user_basic";\r\n朋友圈分析：friend_circle ;\r\n短信联系详情统计分析：sms_contact_detail;\r\n电话风险分析：call_risk_analysis;\r\n电话行为分析：cell_behavior;\r\n电话联系详情统计分析：call_contact_detail;\r\n基本检查项分析：basic_check_items;\r\n联系区域分析：contact_region;\r\n分析报告：report;\r\n行为核查分析：behavior_check;\r\n手机号账户信息分析：cell_phone;
    var result: String? = null,                 // 单项数据结果
    var carrier_001: String? = null,                 // 预留1
    var carrier_002: String? = null                 // 预留2
) : AbstractEntity() {
    /**
     * 表名
     * */
    override fun tableName() = CallLog.tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_result_data"

    }
}