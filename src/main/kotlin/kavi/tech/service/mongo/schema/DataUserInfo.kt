package tech.kavi.cms.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kavi.tech.service.mongo.component.AbstractSchema
import kavi.tech.service.mongo.schema.Record


/**
 * DataUserInfo 个人信息表
 */
//忽略该目标对象不存在的属性
@JsonIgnoreProperties(ignoreUnknown = true)
data class DataUserInfo constructor(
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
    override fun tableName() = DataUserInfo.TABLE_NAME
    // 静态方法 属性
    companion object {
        const val TABLE_NAME = "data_user_info"
    }
}
