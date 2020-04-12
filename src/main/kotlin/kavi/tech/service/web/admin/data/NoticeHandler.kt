package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexInt
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.mysql.dao.CallLogDao
import org.springframework.beans.factory.annotation.Autowired
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

@HandlerRequest(path = "/notice", method = HttpMethod.POST)
class NoticeHandler @Autowired constructor(
    private val callLogDao: CallLogDao
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
        try {
            val params: JsonObject = event.bodyAsJson
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            val mobile = params.getString("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val task_id = params.getString("task_id") ?: throw IllegalArgumentException("缺少任务ID！")
            val back_url = params.getString("back_url") ?: throw IllegalArgumentException("缺少回调地址！")
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            if (task_id.isEmpty()) {
                throw IllegalArgumentException("(任务ID)参数不合法！")
            }
            if (back_url.isEmpty()) {
                throw IllegalArgumentException("(回调地址)参数不合法！")
            }
            // TODO  数据提取

            // TODO  数据清洗
            // TODO  数据推送

            callLogDao.select("select * from  carrier_voicecall limit 10").subscribe({

                println(it.toString())
            }, {
                it.printStackTrace()
            })





            event.response().end(result.put("message", "notice success").toString())
        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }

    }
}
