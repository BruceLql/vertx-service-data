package kavi.tech.service.web.admin.record

import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.value
import kavi.tech.service.common.utils.Sha256Utils
import kavi.tech.service.mongo.model.RecordModel
import kavi.tech.service.mysql.entity.NoticeRecords
import org.springframework.beans.factory.annotation.Autowired
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

@HandlerRequest(path = "/list", method = HttpMethod.POST)
class ListHandler @Autowired constructor(
    private val vertx: Vertx,
    private val recordModel: RecordModel
) : ControllerHandler() {
    private val log = logger(this::class)
    /**
     * 数据处理
     * */
    override fun handle(event: RoutingContext) {
        log.info("=========/record/list==============")
        val buffer = event.body

        vertx.executeBlocking<String>({
            it.complete( GZIPUtils().uncompressToString(buffer.bytes))
        },{
            println("==============结果===="+it.result())
            val resultSend =  JsonObject(it.result())
            val sign = Sha256Utils.sha256(
                "${resultSend.getJsonObject("data").toString()}"
                        + "${resultSend.value<String>("task_id")}"
                        + "${resultSend.value<String>("mobile")}"
                        + "${resultSend.value<String>("return_code")}"
                        + "${resultSend.value<String>("message")}"
                        + "${resultSend.value<Long>("operation_time")}"
                        + "${resultSend.value<String>("nonce")}"
                        + "${NoticeRecords.KEY}"
            )
            println("sign:$sign")
            val _sign = resultSend.value<String>("sign")
            println("_sign:$_sign")
            println("=============比较=== :"+(_sign == sign))

        })
//        val result = GZIPUtils().uncompressToString(buffer.bytes)
//        println("结果：${result}")
        val returnJson = JsonObject().put("code", "FALL")
        event.response().setStatusCode(200).end(returnJson.toString())


    }
}
