package kavi.tech.service.mongo.schema

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kavi.tech.service.mongo.component.AbstractSchema


/**
 * CarrierStatistics 爬虫任务执行情况统计
 */
//忽略该目标对象不存在的属性
@JsonIgnoreProperties(ignoreUnknown = true)
data class CarrierStatistics constructor(
        var mobile: String? = null, // 手机号
        var task_id: String? = null, // 任务ID
        var operator: String? = null, // 运营商类型
        var statistics: Int? = 0, // 执行时间（毫秒数）
        var city: String? = null, // 城市
        var province: String? = null, // 省份
        var ip: String? = null, // ip
        var app_name: String? = null, // 产品名称
        var success: Boolean ? = false  // 最终任务状态

) : AbstractSchema() {
    override fun tableName() = TABLE_NAME
    // 静态方法 属性
    companion object {
        const val TABLE_NAME = "carrier_statistics"
    }
}
