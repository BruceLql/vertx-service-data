package kavi.tech.service.common.extension

import java.util.regex.Pattern


/**
 * 手机号校验正则
 */
fun regexPhone(phone: String): Boolean {
    var mainRegex = "^((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(18[0,1,2,3,5-9])|(177))\\d{8}$"
    var p = Pattern.compile(mainRegex)
    val m = p.matcher(phone)
    return m.matches()
}

/**
 * 手机号校验正则
 */
fun regexInt(num: String): Boolean {
    var mainRegex = "^\\d+"
    var p = Pattern.compile(mainRegex)
    val m = p.matcher(num)
    return m.matches()
}
