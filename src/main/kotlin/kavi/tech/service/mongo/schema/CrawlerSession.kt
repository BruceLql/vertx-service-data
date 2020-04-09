package tech.kavi.cms.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kavi.tech.service.mongo.component.AbstractSchema

/**
 * 爬虫触发会话记录
 */
//忽略该目标对象不存在的属性
@JsonIgnoreProperties(ignoreUnknown = true)
data class CrawlerSession constructor(
        var mobile: String? = null,
        var active: String? = null,
        var ip: String? = null,
        var user_id: String? = null,
        var channel_id: String? = null,
        var platform: String? = null,
        var verify: String? = null,
        var status: String? = null,
        var task: String? = null,
        var progress: String? = null,
        var params: String? = null
) : AbstractSchema() {

    override fun tableName() = CrawlerSession.TABLE_NAME

    companion object {
        const val TABLE_NAME = "crawler_session"
    }
}



