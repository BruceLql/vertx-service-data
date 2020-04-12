package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 查询记录表
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class QueryRecords(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var type: Int? = 0,             // 获取数据的方式 1:推送 2：主动查询
    var status: Int? = 0,           // 状态 0：success 1:error
    var message: String? = null,      // 说明："success";"异常信息"
    var result: String? = null,      // 查询结果 完整数据
    var carrier_001: String? = null,      // 预留1
    var carrier_002: String? = null       // 预留2
) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_query_records"

    }
}
