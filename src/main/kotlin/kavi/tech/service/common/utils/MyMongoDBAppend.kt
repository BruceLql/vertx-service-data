package kavi.tech.service.common.utils

import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.AppenderBase
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.ext.mongo.MongoClient
import tech.kavi.vs.core.VertxBeansBase
import tech.kavi.vs.core.VertxBeansBase.Companion.value

/**
 * 自定义mongo append 入库
 */
class MyMongoDBAppend : AppenderBase<LoggingEvent>() {


    private lateinit var mongoClient: MongoClient

    /**
     * 日志表
     */
    private val LOG_COLLECTION = "logging_event"

    /**
     * mongo db 入库
     */
    override fun append(eventObject: LoggingEvent) {
        try {
            if (!eventObject.formattedMessage.isNullOrBlank()) {
                if (eventObject.formattedMessage.contains("business_log")) {
                    val jsonObject = JsonObject(eventObject.formattedMessage)
                    jsonObject.remove("business_log")
                    mongoClient.rxInsert(LOG_COLLECTION, jsonObject).subscribe({}, {
                        it.printStackTrace()
                    })
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    /**
     * 启动时初始化mongo 连接
     */
    override fun start() {
        try {
            mongoClient = MongoClient.createShared(
                io.vertx.rxjava.core.Vertx.vertx(),
                VertxBeansBase.config("config.json")
                    .value("MONGO", JsonObject())
            )
        } catch (e: Exception) {
            e.printStackTrace()
        }
        super.start()
    }
}
