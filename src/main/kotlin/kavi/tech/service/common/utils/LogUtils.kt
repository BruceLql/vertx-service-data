package kavi.tech.service.common.utils

import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tech.kavi.vs.core.VertxBeansBase
import tech.kavi.vs.core.VertxBeansBase.Companion.value
import java.net.InetAddress


/**
 * 日志工具类
 */
class LogUtils constructor(private val appName: String) {
    private val log: Logger = LoggerFactory.getLogger(appName)

    /**
     * 获取配置port
     */
    private val ip: Pair<String, String> by lazy {
        try {
            VertxBeansBase.config("config.json").let {
                Pair("${it.value<String>("HTTP.HOST")}", "${it.value<Int>("HTTP.PORT")}")
            }
        } catch (e: Exception) {
            Pair(InetAddress.getLocalHost().hostAddress, "0000")
        }
    }


    /**
     * json 输出
     */
    private fun buildJsonStr(mobile: String?, content: String, sessionId: String?, levelEnum: LevelEnum) {
        val jsonObject = JsonObject()
            .put("level", levelEnum.value)
            .put("app_name", appName)
            .put("mobile", mobile)
            .put("content", content)
            .put("ip", ip.first)
            .put("port", ip.second)
            .put("record_time", System.currentTimeMillis())
            .put("session_id", sessionId)
            .put("version", 0)
            .put("business_log",true)
        // business_log 标示是否业务日志，详见MyMongoDBAppend.kt
        val str = Json.encode(jsonObject)
        when (levelEnum.value) {
            "info" -> log.info(str)
            "error" -> log.error(str)
            "warn" -> log.warn(str)
            "debug" -> log.debug(str)
        }
    }


    /**
     * info
     *
     * @param mobile
     * @param content
     * @param sessionId
     */
    fun info(mobile: String, content: String, sessionId: String) {
        buildJsonStr(mobile, content, sessionId, LevelEnum.INFO)
    }

    /**
     * info
     *
     * @param mobile
     * @param content
     */
    fun info(mobile: String, content: String) {
        buildJsonStr(mobile, content, null, LevelEnum.INFO)
    }

    /**
     * error
     *
     * @param mobile
     * @param sessionId
     */
    fun error(mobile: String, e: Exception, sessionId: String) {
        buildJsonStr(mobile, getErrorStr(e), sessionId, LevelEnum.ERROR)
    }

    /**
     * 获取控制台异常日志 [n] 代表换行 ，[t]代表空格
     * @param e
     * @return
     */
    private fun getErrorStr(e: Exception): String {
        return e.javaClass.simpleName + " " + e?.message
    }

    /**
     * error catch块记录exception信息
     *
     * @param e
     */
    fun error(e: Exception) {
        buildJsonStr(null, getErrorStr(e), null, LevelEnum.ERROR)
    }

    /**
     * warn
     *
     * @param mobile
     * @param content
     * @param sessionId
     */
    fun warn(mobile: String, content: String, sessionId: String) {
        buildJsonStr(mobile, content, sessionId, LevelEnum.WARN)
    }

    /**
     * warn
     *
     * @param mobile
     * @param content
     */
    fun warn(mobile: String, content: String) {
        buildJsonStr(mobile, content, null, LevelEnum.WARN)
    }

    /**
     * debug
     *
     * @param mobile
     * @param content
     * @param sessionId
     */
    fun debug(mobile: String, content: String, sessionId: String) {
        buildJsonStr(mobile, content, sessionId, LevelEnum.DEBUG)
    }


    /**
     * debug
     *
     * @param mobile
     * @param content
     */
    fun debug(mobile: String, content: String) {
        buildJsonStr(mobile, content, null, LevelEnum.DEBUG)
    }


    companion object {
        /**
         * 日志级别
         */
        enum class LevelEnum(val value: String) {
            ERROR("error"), WARN("warn"), DEBUG("debug"), INFO("info");
        }
    }
}
