package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 通知记录表
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class NoticeRecords(
    var id: Long? = null,
    var task_id: String? = null,                // 创建任务时的monoId
    var mobile: String? = null,                 // 手机号码
    var back_url: String? = null,             // 回调地址 数据推送地址
    var result: String? = null,           // 单项数据结果
    var carrier_001: String? = null,      // 预留1
    var carrier_002: String? = null       // 预留2
) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "carrier_notice_records"

    }
}
