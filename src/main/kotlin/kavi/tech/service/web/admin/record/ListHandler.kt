package kavi.tech.service.web.admin.record

import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mongo.model.RecordModel
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

        val result = GZIPUtils().uncompressToString(buffer.bytes)
        vertx.executeBlocking<String>({
            it.complete( GZIPUtils().uncompressToString(buffer.bytes))
        },{
            println(it.result())
        })
        println("结果：${result}")
        event.response().end("success")


    }
}
