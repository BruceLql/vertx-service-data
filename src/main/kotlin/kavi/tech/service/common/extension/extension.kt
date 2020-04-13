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

/**
 *  字符串时分秒单位 取值转换成秒
 */
fun splitHms(strHms: String): Int {

    if (strHms.isNullOrEmpty()) {
        return 0
    }

    var totalInt: Int = 0
    return strHms.let {

        val str = it.replace("小时", "-").replace("分钟", "-").replace("秒", "")

        val strList = str.split("-")
        if (strList.size == 3) {
            totalInt += strList[0].toInt() * 3600
            totalInt += strList[1].toInt() * 60
            totalInt += strList[2].toInt()
        }
        if (strList.size == 2) {
            totalInt += strList[0].toInt() * 60
            totalInt += strList[1].toInt()
        }
        if (strList.size == 1) {
            totalInt += strList[0].toInt()
        }

        totalInt
    }
}


/**
 *  字符串年月日单位 取值转换成月
 */
fun splitYmd(strYmd: String): Int {

    if (strYmd.isNullOrEmpty()) {
        return 0
    }

    var totalInt: Int = 0
    return strYmd.let {

        val str = it.replace("年", "-").replace("个月", "")

        val strList = str.split("-")
        if (strList.size == 2) {
            totalInt += strList[0].toInt() * 12
            totalInt += strList[1].toInt()
        }
        if (strList.size == 1) {
            totalInt += strList[0].toInt()
        }

        totalInt
    }
}
