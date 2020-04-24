package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.common.extension.value
import kavi.tech.service.mysql.dao.QueryRecordsDao
import kavi.tech.service.mysql.dao.ResultDataDao
import kavi.tech.service.mysql.entity.QueryRecords
import org.springframework.beans.factory.annotation.Autowired
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

/**
 * @param mobile 必传参数 手机号
 *
 */
@HandlerRequest(path = "/query", method = HttpMethod.POST)
class QueryHandler @Autowired constructor(
    private val queryRecordsDao: QueryRecordsDao,
    private val resultDataDao: ResultDataDao
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
        val data = JsonObject()
        result.put("status", "0")
        result.put("message", "success")

        try {
            val params: JsonObject = event.bodyAsJson
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            val mobile = params.value<String>("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val task_id = params.value<String>("task_id") ?: null
            val item = params.value<String>("item") ?: throw IllegalArgumentException("缺少 item 类型！")  // ”raw“; "report"
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            val queryRecord = QueryRecords()
            queryRecord.mobile = mobile
            if (!task_id.isNullOrEmpty()) {
                queryRecord.task_id = task_id
            }
            // 获取数据的方式 1:推送 2：主动查询
            queryRecord.type = 2
            // 状态 0：success 1:error
            queryRecord.status = 0
            // 保存请求记录
            resultDataDao.queryLastestTaskId(queryRecord).flatMap {

                println("==============it:" + it.toJson())
                if (it.numRows == 0) {
                    result.put("message", "没有查询到数据!")
                    event.response().setStatusCode(500).end(result.toString())
//                  throw IllegalArgumentException("没有查询到数据！")
                }

                // 查询出来的task_id
                val taskId = it.rows[0].getString("task_id") ?: null
                when (taskId) {
                    null -> {
                        result.put("message", "没有查询到数据!")
                        event.response().setStatusCode(500).end(result.toString())
                    }
                }

                // 返回手机号和 任务ID
                result.put("mobile", mobile)
                result.put("task_id", taskId)
                val listOf = if (item.isNotEmpty()) {
                    listOf(Pair("task_id", taskId), Pair("mobile", mobile), Pair("item", item))
                } else {
                    listOf(Pair("task_id", taskId), Pair("mobile", mobile))
                }

                resultDataDao.selectData(listOf())

            }.subscribe({
                println(it.toString())
                val resultStr = it.getString("result")
                // 取出result 字段 再gzip压缩转成ByteArray
                val gzipData = GZIPUtils().compress(JsonObject(resultStr).toString())
                result.put("data", gzipData)
                //  数据返回
                event.response().end(result.toString())

            }, { it.printStackTrace() })


        } catch (e: Exception) {
            e.printStackTrace()
            println("======== catch =========")
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }
    }

}

