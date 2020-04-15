package kavi.tech.service.common.utils

import java.security.MessageDigest

/**
 * sha256加密工具类
 */
object Sha256Utils {

    /**
     * 将byte转16进制
     */
    private fun byte2Hex(byteArray: ByteArray): String {
        //转成16进制后是32字节
        return with(StringBuilder()) {
            byteArray.forEach {
                val hex = it.toInt() and (0xFF)
                val hexStr = Integer.toHexString(hex)
                if (hexStr.length == 1) {
                    append("0").append(hexStr)
                } else {
                    append(hexStr)
                }
            }
            toString()
        }
    }

    fun sha256(str: String): String {
        val digest = MessageDigest.getInstance("SHA-256")
        val result = digest.digest(str.toByteArray())
        return byte2Hex(result)
    }
}
