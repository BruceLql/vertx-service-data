package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.error
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.common.extension.value
import kavi.tech.service.common.utils.OperatorCertificationComponent
import kavi.tech.service.mysql.dao.H5RequestParamDao
import kavi.tech.service.service.CarrierService
import kavi.tech.service.service.ReportService
import org.springframework.beans.factory.annotation.Autowired
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

@HandlerRequest(path = "/notice", method = HttpMethod.POST)
class NoticeHandler @Autowired constructor(
    val reportService: ReportService,
    val carrierService: CarrierService,
    val h5RequestParamDao: H5RequestParamDao,
    val operatorCertificationComponent: OperatorCertificationComponent

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
        val params: JsonObject = try {
            event.bodyAsJson
        } catch (e: Exception) {
            event.response().error(e) // 返回数据
            return
        }
        /* 查询条件 */
        val query = JsonObject()
        val taskId = params.value<String>("task_id")
        try {
            log.info("/data/notice 请求参数：${params.toString()}")
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            taskId ?: throw IllegalArgumentException("缺少任务ID！")
        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().error(e, result) // 返回数据
            return
        }
        h5RequestParamDao.queryParams(listOf(Pair("mid", taskId))).map {h5 ->
            log.info("$h5.toJson()")
            val mobile = h5.mobile
            val backUrl = h5.call_back
            val nonce = h5.nonce
            val name = h5.name
            val idCard = h5.cid
            mobile ?: throw IllegalArgumentException("缺少手机号码！")
            backUrl ?: throw IllegalArgumentException("缺少回调地址！")
            nonce ?: throw IllegalArgumentException("缺少nonce！")
            name ?: throw IllegalArgumentException("缺少name！")
            idCard ?: throw IllegalArgumentException("缺少idCard！")
            try {
                if (!regexPhone(mobile)) {
                    throw IllegalArgumentException("(手机号)参数不合法！")
                }
                if (taskId.isEmpty()) {
                    throw IllegalArgumentException("(任务ID)参数不合法！")
                }
                if (backUrl.isEmpty()) {
                    throw IllegalArgumentException("(回调地址)参数不合法！")
                }
            } catch (e: Exception) {
                e.printStackTrace()
                result.put("message", e.message ?: "异常，请联系管理员排查")
                event.response().error(e, result) // 返回数据
                return@map
            }

            query.put("mobile", mobile).put("mid", taskId)

            //   数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
            reportService.beginDataByMongo(query, FindOptions(), name, idCard).flatMap {

                //   调用数据清洗服务 结果封装到 result
                reportService.dataClear(mobile, taskId).flatMap {
                    // todo 清洗服务失败的情况下  调整return_code message
                    val return_code = "00000"
                    val message = "成功"
                    val operation_time = System.currentTimeMillis()
                    // 封装运营商原始数据报文格式
                    val resultSend =
                        reportService.resultPacket(it, return_code, message, operation_time, taskId, mobile, nonce)
                    // 存储运营商报告
                    reportService.saveRecord("report", taskId, mobile, idCard, name, nonce, resultSend)
                    // 查询原始数据封装到result
                    carrierService.dataRaw(mobile, taskId)
                }
            }.subscribe({
                // todo 原始数据获取失败的情况下  调整return_code message
                val return_code = "00000"
                val message = "成功"
                val operation_time = System.currentTimeMillis()
                // 封装运营商原始数据报文格式
                val resultSend =
                    reportService.resultPacket(it, return_code, message, operation_time, taskId, mobile, nonce)
                // 加签名后  运营商原始数据存储入库
                reportService.saveRecord("raw", taskId, mobile, idCard, name, nonce, resultSend)

                log.info("推送前结果： $resultSend")
                log.info("推送前结果size： ${resultSend.toString().length}")

                log.info("推送地址 : $backUrl")
                operatorCertificationComponent.callBackPlatform(resultSend, backUrl, backUrl)
                event.response().end(result.put("message", "notice success").toString())

            }, { it.printStackTrace() })

        }.subscribe({
            it
        },{
            it.printStackTrace()
            event.response().end(result.put("message", "notice failed").toString())
        })

    }
}



