package kavi.tech.service.mongo.schema

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kavi.tech.service.mongo.component.AbstractSchema

/**
 * DataAppreciationInfo 增值业务表
 */
//忽略该目标对象不存在的属性
@JsonIgnoreProperties(ignoreUnknown = true)
class DataAppreciationInfo constructor(
        var mobile: String? = null,
        var user_id: String? = null,
        var mid: String? = null,
        var channel_id: String? = null,
        var url: String? = null,
        var title: String? = null,
        var model: String? = null,
        var month: String? = null,
        var params: String? = null,
        var data: String? = null
) : AbstractSchema() {

    override fun tableName() = TABLE_NAME

    companion object {
        const val TABLE_NAME = "data_appreciation_info"
    }
}
