package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.client.WebClient
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.common.extension.value
import kavi.tech.service.mongo.model.*
import kavi.tech.service.mysql.dao.*
import kavi.tech.service.mysql.entity.NoticeRecords
import kavi.tech.service.service.*
import org.springframework.beans.factory.annotation.Autowired
import rx.Observable
import rx.Single
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

@HandlerRequest(path = "/notice", method = HttpMethod.POST)
class NoticeHandler @Autowired constructor(
    val reportService: ReportService

) : ControllerHandler() {
    private val log = logger(this::class)

    @Autowired
    private lateinit var rxClient: WebClient

    /**
     * 爬虫程序执行完毕后通知
     * @author max
     * @param mobile  手机号
     * @param task_id  任务ID
     * @param back_url 回调地址
     * */
    override fun handle(event: RoutingContext) {
        log.info("=========/data/notice==============")
        // result 返回值
        val result = JsonObject()

        try {
            val params: JsonObject = event.bodyAsJson
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            /* 查询条件 */
            val query = JsonObject()
            val mobile = params.value<String>("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val task_id = params.value<String>("task_id") ?: throw IllegalArgumentException("缺少任务ID！")
            val back_url = params.value<String>("back_url") ?: throw IllegalArgumentException("缺少回调地址！")
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            if (task_id.isEmpty()) {
                throw IllegalArgumentException("(任务ID)参数不合法！")
            }
            if (back_url.isEmpty()) {
                throw IllegalArgumentException("(回调地址)参数不合法！")
            }


            val resultJsonObject = JsonObject()  // 存取最终返回结果

            query.put("mobile", mobile).put("mid", task_id)

            //   数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
            reportService.beginDataByMongo(query,FindOptions()).flatMap {

                //   调用数据清洗服务 结果封装到 result
                reportService.dataClear(mobile, task_id)
            }.subscribe({
                // 推送数据结果
                val resultSend = JsonObject()

                resultSend.put("data", it)
                    .put("mobile", mobile)
                    .put("task_id", task_id)
                    .put("return_code", "00000")
                    .put("message", "成功")
                    .put("operation_time", System.currentTimeMillis())
                // TODO  数据推送服务  resultSend
                println("推送前结果： $resultSend")
                println("推送前结果size： ${resultSend.toString().length}")

                println("推送地址 : $back_url")
                reportService.pushData(back_url,resultSend)

                event.response().end(result.put("message", "notice success").toString())

            }, { it.printStackTrace() })


//            event.response().end(result.put("message", "notice success").toString())
        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }

    }
}



