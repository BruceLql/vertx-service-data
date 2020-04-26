package kavi.tech.service.mongo.schema

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.json.JsonObject
import kavi.tech.service.mongo.component.AbstractSchema


/**
 * CarrierReportInfo 运营商报告数据表   包括raw: 原始数据 report: 分析后的报告数据
 */
//忽略该目标对象不存在的属性
@JsonIgnoreProperties(ignoreUnknown = true)
data class CarrierReportInfo constructor(
        var mobile: String? = null,
        var task_id: String? = null,
        var name: String? = null,
        var idCard: String? = null,
        var nonce: String? = null, //
        var item: String? = null, //数据类型 raw: 原始数据 report: 分析后的报告数据
        var result: JsonObject? = null // 数据结果
) : AbstractSchema() {
    override fun tableName() = TABLE_NAME
    // 静态方法 属性
    companion object {
        const val TABLE_NAME = "carrier_report_info"
    }
}
