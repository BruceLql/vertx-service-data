package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 分析结果数据表
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class ResultData(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    /*
    item 数据项 :
        基本信息："user_basic";
        朋友圈分析：friend_circle ;
        短信联系详情统计分析：sms_contact_detail;
        电话风险分析：call_risk_analysis;
        电话行为分析：cell_behavior;
        电话联系详情统计分析：call_contact_detail;
        基本检查项分析：basic_check_items;
        联系区域分析：contact_region;
        分析报告：report;
        行为核查分析：behavior_check;
        手机号账户信息分析：cell_phone;
    */
    var item: String? = null,             // 数据项
    var result: String? = null,           // 单项数据结果
    var carrier_001: String? = null,      // 预留1
    var carrier_002: String? = null       // 预留2
) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_result_data"

    }
}
