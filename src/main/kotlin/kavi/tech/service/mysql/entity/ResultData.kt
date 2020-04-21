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
    var name: String? = null,             // 前端传 姓名
    var id_card: String? = null,             // 前端传 身份证 新增
    var nonce: String? = null,             // 前端传 随机字符串 新增
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
