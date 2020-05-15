package kavi.tech.service.mysql.entity

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import kavi.tech.service.mysql.component.AbstractEntity

/**
 * 通知参数表
 * */
@JsonNaming(PropertyNamingStrategy.LowerCaseStrategy::class)
data class H5RequestParam(
    var id: Long? = null,
    var uuid: String? = null,                // token标识
    var mid: String? = null,                 // mongoId   taskId
    var user_id: String? = null,             // 用户ID
    var name: String? = null,           // 姓名
    var cid: String? = null,      // 身份证号
    var mobile: String? = null,       // 手机号
    var call_back: String? = null,       // 回调地址
    var notify_url: String? = null,       // 跳转url
    var nonce: String? = null       // 32位加密随机数
) : AbstractEntity() {

    override fun tableName() = tableName

    companion object {
        /**
         * 表名
         * */
        const val tableName = "h5_request_param"
        // 盐值
        const val  KEY ="50BF48B9D36F6B908A1427BFD83929DCCF48C0EAADE4C0B3B0660240640B19D3"

    }
}
