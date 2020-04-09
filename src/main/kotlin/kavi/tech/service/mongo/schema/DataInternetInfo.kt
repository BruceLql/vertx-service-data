package tech.kavi.cms.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kavi.tech.service.mongo.component.AbstractSchema

/**
 * DataInternetInfo 上网记录表
 */
//忽略该目标对象不存在的属性
@JsonIgnoreProperties(ignoreUnknown = true)
class DataInternetInfo constructor(
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

    override fun tableName() = DataInternetInfo.TABLE_NAME

    companion object {
        const val TABLE_NAME = "data_internet_info"
    }
}