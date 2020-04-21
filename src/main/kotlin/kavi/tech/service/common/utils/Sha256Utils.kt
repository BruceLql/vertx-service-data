package kavi.tech.service.common.utils

import java.io.UnsupportedEncodingException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import kotlin.experimental.and


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

    /**
     * 利用java原生的类实现SHA256加密
     *
     * @param str
     * @return
     *//*
    private fun getSHA256(str: String): String {
        val messageDigest: MessageDigest
        var encodestr: String = ""
        try {
            messageDigest = MessageDigest.getInstance("SHA-256")
            messageDigest.update(str.toByteArray(charset("UTF-8")))
            encodestr = byte2Hex(messageDigest.digest())
        } catch (e: NoSuchAlgorithmException) {
            e.printStackTrace()
        } catch (e: UnsupportedEncodingException) {
            e.printStackTrace()
        }
        return encodestr
    }*/

}
