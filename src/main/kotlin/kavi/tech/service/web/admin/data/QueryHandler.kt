package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.*
import kavi.tech.service.mongo.model.CarrierReportInfoModel
import org.springframework.beans.factory.annotation.Autowired
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

/**
 * @param mobile 必传参数 手机号
 *
 */
@HandlerRequest(path = "/query", method = HttpMethod.POST)
class QueryHandler @Autowired constructor(
    private val carrierReportInfoModel: CarrierReportInfoModel
) : ControllerHandler() {
    private val log = logger(this::class)
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
        result.put("status", "0")
        result.put("message", "success")
        val params: JsonObject = try {
            event.bodyAsJson
        } catch (e: Exception) {
            event.response().error(e) // 返回数据
            return
        }
        val mobile =params.value<String>("mobile")
        val outTaskId = params.value<String>("task_id")
        val item = params.value<String>("item")
        try {
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            mobile ?: throw IllegalArgumentException("缺少手机号码！")

            item ?: throw IllegalArgumentException("缺少 item 类型！")  // ”raw“; "report"
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
        } catch (e: Exception) {
            e.printStackTrace()
            println("======== catch =========")
            event.response().error(e, message= "异常，请联系管理员排查") // 返回数据
            return
        }
        val query = JsonObject()
        query.put("mobile", mobile)
        if (!outTaskId.isNullOrEmpty()) {
            query.put("task_id", outTaskId)
        }
        val findOptions = FindOptions()
        findOptions.setFields(JsonObject().put("task_id", 1).put("_id", 0))
            .setSort(JsonObject().put("created_at", -1)).limit = 1
        //如果没传task_id 将根据手机号查询最近一次的任务ID ，如果传来task_id 将检查该任务ID是否存在
        carrierReportInfoModel.queryLatestTaskId(query, findOptions).map { list ->

            if (list.isEmpty()) {
                result.put("status", "1")
                result.put("message", "未查询到数据!")
                event.response().end(result.toString())
                throw IllegalArgumentException("未查询到数据！")
            }

            // 查询出来的task_id
            val taskId = list[0].value<String>("task_id")
            when (taskId) {
                null -> {
                    result.put("status", "1")
                    result.put("message", "未查询到数据!")
                    event.response().end(result.toString())
                    throw IllegalArgumentException("未查询到数据！")
                }
            }

            // 返回手机号和 任务ID
            result.put("mobile", mobile)
            result.put("task_id", taskId)

            JsonObject().also { que ->
                que.put("mobile", mobile).put("task_id", taskId).put("item", item)
            }
        }.flatMap { query ->
            val findOption = FindOptions()
            findOption.setSort(JsonObject().put("created_at", -1)).limit = 1
            carrierReportInfoModel.queryListResultData(query, findOption)

        }.subscribe({
            when {
                it.isEmpty() -> {
                    result.put("status", "1").put("message", "未查询到数据!")
                    event.response().success(null, result)
                }
                else -> {
                    val resultStr = it[0].value<JsonObject>("result").toString()
                    // 取出result 字段 再gzip压缩转成ByteArray
                    val gzipData = GZIPUtils().compress(resultStr)
                    result.put("data", gzipData)
                    //  数据返回
                    event.response().end(result.toString())
                }
            }

        }, { it.printStackTrace() })


    }

}

